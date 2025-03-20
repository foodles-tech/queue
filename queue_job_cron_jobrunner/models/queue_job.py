# Copyright 2022 Camptocamp SA (https://www.camptocamp.com).
# @author Iván Todorovich <ivan.todorovich@camptocamp.com>
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).

import logging
import traceback
from datetime import datetime, timedelta
from io import StringIO

import psutil
from psycopg2 import OperationalError

from odoo import _, api, fields, models, tools
from odoo.service.model import PG_CONCURRENCY_ERRORS_TO_RETRY

from odoo.addons.base.models.ir_cron import _intervalTypes
from odoo.addons.queue_job.controllers.main import PG_RETRY
from odoo.addons.queue_job.exception import (
    FailedJobError,
    NothingToDoJob,
    RetryableJobError,
)
from odoo.addons.queue_job.job import Job
from odoo.addons.queue_job.jobrunner import QueueJobRunner

_logger = logging.getLogger(__name__)


class QueueJob(models.Model):
    _inherit = "queue.job"

    @api.model
    def _acquire_one_job(self, commit=False):
        """Acquire the next job to be run.

        :returns: queue.job record (locked for update)
        """
        runner = QueueJobRunner.from_environ_or_config()
        self.env.cr.execute(
            """
            SELECT id
            FROM queue_job
            WHERE state = 'pending'
            AND (eta IS NULL OR eta <= (now() AT TIME ZONE 'UTC'))
            ORDER BY priority, date_created
            FOR NO KEY UPDATE
            """
        )
        rows = self.env.cr.fetchall()

        channels = {}
        for queue_job in self.search([("state", "=", "started")]):
            if not queue_job.channel:
                continue
            channels[queue_job.channel] = channels.get(queue_job.channel, 0) + 1
        channels_without_capacity = set()
        for channel_str, running in channels.items():
            channel = runner.channel_manager.get_channel_by_name(
                channel_str, autocreate=True
            )
            if channel.capacity and channel.capacity <= running:
                channels_without_capacity.add(channel_str)
        channels_without_capacity.discard(
            "root"
        )  # root must be disabled to avoid normal jobrunner
        _logger.info(
            "_acquire_one_job channels_without_capacity %s",
            channels_without_capacity,
        )

        result = self.browse()
        for row in rows:
            queue_job = self.browse(row[0])
            if queue_job.channel and queue_job.channel in channels_without_capacity:
                continue
            job = Job._load_from_db_record(queue_job)
            job.set_started()
            job.store()
            _logger.info(
                "_acquire_one_job queue.job %s[channel=%s,uuid=%s] started",
                row[0],
                job.channel,
                job.uuid,
            )
            result = queue_job
            break
        self.flush()
        if commit:  # pragma: no cover
            self.env.cr.commit()  # pylint: disable=invalid-commit
        return result

    def _process(self, commit=False):
        """Process the job"""
        self.ensure_one()
        job = Job._load_from_db_record(self)
        # Actual processing
        try:
            try:
                with self.env.cr.savepoint():
                    _logger.info(
                        "perform %s[channel=%s,uuid=%s]",
                        self.id,
                        self.channel,
                        self.uuid,
                    )
                    job.perform()
                    _logger.info(
                        "performed %s[channel=%s,uuid=%s]",
                        self.id,
                        self.channel,
                        self.uuid,
                    )
                    job.set_done()
                    job.store()
            except OperationalError as err:
                # Automatically retry the typical transaction serialization errors
                if err.pgcode not in PG_CONCURRENCY_ERRORS_TO_RETRY:
                    raise
                message = tools.ustr(err.pgerror, errors="replace")
                job.postpone(result=message, seconds=PG_RETRY)
                job.set_pending(reset_retry=False)
                job.store()
                _logger.debug("%s OperationalError, postponed", job)

        except NothingToDoJob as err:
            if str(err):
                msg = str(err)
            else:
                msg = _("Job interrupted and set to Done: nothing to do.")
            job.set_done(msg)
            job.store()
            _logger.info(
                "interrupted %s[channel=%s,uuid=%s]", self.id, self.channel, self.uuid
            )

        except RetryableJobError as err:
            # delay the job later, requeue
            job.postpone(result=str(err), seconds=5)
            job.set_pending(reset_retry=False)
            job.store()
            _logger.info(
                "postponed %s[channel=%s,uuid=%s]", self.id, self.channel, self.uuid
            )

        except (FailedJobError, Exception):
            with StringIO() as buff:
                traceback.print_exc(file=buff)
                _logger.error(buff.getvalue())
                job.set_failed(exc_info=buff.getvalue())
                job.store()
            _logger.info(
                "failed %s[channel=%s,uuid=%s]", self.id, self.channel, self.uuid
            )

        if commit:  # pragma: no cover
            self.env["base"].flush()
            self.env.cr.commit()  # pylint: disable=invalid-commit

        _logger.debug("%s enqueue depends started", job)
        job.enqueue_waiting()
        _logger.debug("%s enqueue depends done", job)

    @api.model
    def _job_runner(self, commit=True):
        """Short-lived job runner, triggered by async crons"""
        self._release_started_jobs(commit=commit)
        job = self._acquire_one_job(commit=commit)

        while job:
            job._process(commit=commit)

            if self._stop_processing():
                _logger.info(
                    "Stop processing queue jobs in this "
                    "ir.cron call, waiting next ir.cron call.",
                )
                return

            job = self._acquire_one_job(commit=commit)

    @api.model
    def _stop_processing(self):
        """compute what ever the next ir.cron call is going to be
        trigger, if yes we stop processing queue job here

        One of the goal is to mitigate that, when you have a long list of queue
        job to process, the cron thread can be killed
        by odoo.sh or odoo with the limit_time_real_cron limit.

        We suggest to set ir cron interval lower to the limit_time_real_cron.
        """
        # In the current cursor (nor a new cursor) we can't see fresh nextcall which:
        # is committed by Odoo at the end of the cron so we assume all crons are running
        # so nextcall is the current started date
        next_calls = [
            cron.nextcall + _intervalTypes[cron.interval_type](cron.interval_number)
            for cron in self.env["ir.cron"]
            .sudo()
            .search([("queue_job_runner", "=", True)])
        ]
        if not next_calls:
            _logger.info("Stopping queue job processing, no nextcall found.")
            return True

        next_cron_job_runner_trigger_date = min(next_calls)

        stop_processing_threshold_seconds = int(
            self.env["ir.config_parameter"]
            .sudo()
            .get_param(
                "queue_job_cron_jobrunner.stop_processing_threshold_seconds",
                "0",
            )
        )
        end_process_queue_job_date = next_cron_job_runner_trigger_date - timedelta(
            seconds=stop_processing_threshold_seconds
        )
        now = fields.Datetime.now()
        _logger.debug(
            "now: %s - estimated cron nextcall: %s - "
            "Threshold: %ss"
            "stop processing new job after %s",
            now,
            next_cron_job_runner_trigger_date,
            stop_processing_threshold_seconds,
            end_process_queue_job_date,
        )
        if now >= end_process_queue_job_date:
            return True
        return False

    @api.model
    def _cron_trigger(self, at=None):
        """Trigger the cron job runners

        Odoo will prevent concurrent cron jobs from running.
        So, to support parallel execution, we'd need to have (at least) the
        same number of ir.crons records as cron workers.

        All crons should be triggered at the same time.
        """
        if at is not None:
            if isinstance(at, list) and not all([isinstance(x, datetime) for x in at]):
                raise TypeError(f"Invalid parameter 'at': {str(at)}")
            elif not isinstance(at, list) and not isinstance(at, datetime):
                raise TypeError(f"Invalid parameter 'at': {str(at)}")
        crons = self.env["ir.cron"].sudo().search([("queue_job_runner", "=", True)])
        nextcall = fields.Datetime.now()
        if at is not None:
            if isinstance(at, list) and len(at):
                nextcall = sorted(at).pop()
            elif isinstance(at, datetime):
                nextcall = at
        for cron in crons:
            if nextcall < cron.nextcall:
                cron.try_write({"nextcall": nextcall})

    def _ensure_cron_trigger(self):
        """Create cron triggers for these jobs"""
        records = self.filtered(lambda r: r.state == "pending")
        if not records:
            return
        # Trigger immediate runs
        immediate = any(not rec.eta for rec in records)
        if immediate:
            self._cron_trigger()
        # Trigger delayed eta runs
        delayed_etas = {rec.eta for rec in records if rec.eta}
        if delayed_etas:
            self._cron_trigger(at=list(delayed_etas))

    @api.model
    def _release_started_jobs(self, commit=False):
        pids = [x.pid for x in psutil.process_iter()]
        for record in self.search(
            [("state", "=", "started"), ("worker_pid", "not in", pids)]
        ):
            job = Job._load_from_db_record(record)
            job.set_pending()
            job.store()
            _logger.info(
                "release started job %s[channel=%s,uuid=%s]",
                record.id,
                record.channel,
                record.uuid,
            )
        if commit:  # pragma: no cover
            self.env.cr.commit()  # pylint: disable=invalid-commit

    @api.model_create_multi
    def create(self, vals_list):
        # When jobs are created, also create the cron trigger
        records = super().create(vals_list)
        records._ensure_cron_trigger()
        return records

    def write(self, vals):
        # When a job state or eta changes, make sure a cron trigger is created
        res = super().write(vals)
        if "state" in vals or "eta" in vals:
            self._ensure_cron_trigger()
        return res
