# Copyright 2022 Camptocamp SA (https://www.camptocamp.com).
# @author Iván Todorovich <ivan.todorovich@camptocamp.com>
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).

from datetime import datetime, timedelta
from unittest import mock

from freezegun import freeze_time

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger

from odoo.addons.queue_job.jobrunner import QueueJobRunner


class TestQueueJob(TransactionCase):
    def setUp(self):
        super().setUp()
        self.env = self.env(context=dict(self.env.context, tracking_disable=True))
        self.cron = self.env.ref("queue_job_cron_jobrunner.queue_job_cron")
        self.env2 = api.Environment(self.env.registry.cursor(), SUPERUSER_ID, {})

        def cleanUp():
            self.env2.cr.close()

        self.addCleanup(cleanUp)

    @mute_logger("odoo.addons.queue_job_cron_jobrunner.models.queue_job")
    def test_queue_job_process(self):
        """Test that jobs are processed by the queue job cron"""
        # Create some jobs
        job1 = self.env["res.partner"].with_delay().create({"name": "test"})
        job1_record = job1.db_record()
        job2 = self.env["res.partner"].with_delay().create(False)
        job2_record = job2.db_record()
        job3 = self.env["res.partner"].with_delay(eta=3600).create({"name": "Test"})
        job3_record = job3.db_record()
        # Run the job processing cron
        self.cron.nextcall = datetime.now() + timedelta(seconds=600)
        self.env["queue.job"]._job_runner(commit=False)
        # Check that the jobs were processed
        self.assertEqual(job1_record.state, "done", "Processed OK")
        self.assertEqual(job2_record.state, "failed", "Has errors")
        self.assertEqual(job3_record.state, "pending", "Still pending, because of eta")

    def test_stop_processing_job(self):
        # nextcall is the theorical start time of the current cron
        self.cron.nextcall = datetime(2022, 2, 22, 22, 20, 22)
        self.cron.interval_number = 2
        self.cron.interval_type = "minutes"
        self.env["ir.config_parameter"].set_param(
            "queue_job_cron_jobrunner.stop_processing_threshold_seconds", "60"
        )
        job1 = self.env["res.partner"].with_delay().create({"name": "test"})
        job1_record = job1.db_record()
        job2 = self.env["res.partner"].with_delay().create({"name": "Test"})
        job2_record = job2.db_record()
        with freeze_time("2022-02-22 22:21:23"):
            # theorical nextcall is 2022-02-22 22:20:22 + 2' => 22:22:22
            # no queue job should be started after 22:22:22 - 60" => 22:21:22
            self.env["queue.job"]._job_runner(commit=False)
        self.assertEqual(job1_record.state, "done", "Processed OK")
        self.assertEqual(job2_record.state, "pending", "no time left to start it")

    def test_stop_processing_multiple_jobs(self):
        cron2 = self.cron.copy()
        # why not negative thersholds meaning we stop after time
        # is already over for the nextcall
        self.env["ir.config_parameter"].set_param(
            "queue_job_cron_jobrunner.stop_processing_threshold_seconds", "-10"
        )

        self.cron.nextcall = datetime(2022, 2, 22, 22, 22, 22)
        self.cron.interval_number = 2
        self.cron.interval_type = "minutes"

        cron2.nextcall = datetime(2022, 2, 22, 22, 21, 22)
        cron2.interval_number = 2
        cron2.interval_type = "minutes"

        # cron1 not after 22:24:32
        # cron2 not after 22:23:32

        with freeze_time("2022-02-22 22:23:32"):
            self.assertTrue(self.env["queue.job"]._stop_processing())

    def test_stop_processing_inactive_cron_stop_processing(self):
        self.cron.active = False
        self.assertTrue(self.env["queue.job"]._stop_processing())

    @freeze_time("2022-02-22 22:22:22")
    def test_queue_job_cron_trigger_enqueue_dependencies(self):
        """Test that ir.cron execution enqueue waiting dependencies"""
        delayable = self.env["res.partner"].delayable().create({"name": "test"})
        delayable2 = self.env["res.partner"].delayable().create({"name": "test2"})
        delayable.on_done(delayable2)
        delayable.delay()
        job_record = delayable._generated_job.db_record()
        job_record_depends = delayable2._generated_job.db_record()

        self.cron.nextcall = datetime(2022, 2, 22, 23, 23, 23)
        self.env["queue.job"]._job_runner(commit=False)

        self.assertEqual(job_record.state, "done", "Processed OK")
        # if the state is "waiting_dependencies", it means the "enqueue_waiting()"
        # step has not been done when the parent job has been done
        self.assertEqual(job_record_depends.state, "done", "Processed OK")

    @freeze_time("2022-02-22 22:22:22")
    def test_concurrent_cron_access(self):
        """to avoid to launch ir cron twice odoo add a lock
        while running task, if task take times to compute
        other users should be able to create new queue job
        at the same time
        """
        self.env2.cr.execute(
            """SELECT * FROM ir_cron WHERE id=%s FOR UPDATE NOWAIT""",
            (self.cron.id,),
            log_exceptions=False,
        )
        self.env["res.partner"].delayable().create({"name": "test"})
        self.assertNotEqual(self.cron.nextcall, datetime(2022, 2, 22, 22, 22, 22))

    def test_acquire_one_job_use_priority(self):
        with freeze_time("2024-01-01 10:01:01"):
            self.env["res.partner"].with_delay(priority=3).create({"name": "test"})

        with freeze_time("2024-01-01 10:02:01"):
            job = (
                self.env["res.partner"].with_delay(priority=1).create({"name": "test"})
            )

        with freeze_time("2024-01-01 10:03:01"):
            self.env["res.partner"].with_delay(priority=2).create({"name": "test"})

        self.assertEqual(
            self.env["queue.job"]._acquire_one_job(commit=False), job.db_record()
        )

    def test_acquire_one_job_consume_the_oldest_first(self):
        with freeze_time("2024-01-01 10:01:01"):
            job = (
                self.env["res.partner"].with_delay(priority=30).create({"name": "test"})
            )

        with freeze_time("2024-01-01 10:02:01"):
            self.env["res.partner"].with_delay(priority=30).create({"name": "test"})

        with freeze_time("2024-01-01 10:03:01"):
            self.env["res.partner"].with_delay(priority=30).create({"name": "test"})

        self.assertEqual(
            self.env["queue.job"]._acquire_one_job(commit=False), job.db_record()
        )

    def test_acquire_one_job_starts_job(self):
        job = self.env["res.partner"].with_delay(priority=1).create({"name": "test"})

        result = self.env["queue.job"]._acquire_one_job(commit=False)

        self.assertEqual(result, job.db_record())
        self.assertEqual(job.db_record().state, "started")

    def test_acquire_one_job_do_not_overload_channel(self):
        runner = QueueJobRunner.from_environ_or_config()
        runner.channel_manager.get_channel_by_name(
            "root.foobar", autocreate=True
        ).capacity = 2
        job1 = (
            self.env["res.partner"]
            .with_delay(channel="root.foobar")
            .create({"name": "test1"})
        )
        job2 = (
            self.env["res.partner"]
            .with_delay(channel="root.foobar")
            .create({"name": "test2"})
        )
        self.env["res.partner"].with_delay(channel="root.foobar").create(
            {"name": "test3"}
        )

        with mock.patch.object(
            QueueJobRunner, "from_environ_or_config", return_value=runner
        ):
            first_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)
            second_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)
            third_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)

        self.assertEqual(first_acquired_job, job1.db_record())
        self.assertEqual(second_acquired_job, job2.db_record())
        self.assertEqual(third_acquired_job, self.env["queue.job"].browse())

    def test_acquire_one_job_root_capacity_ignored(self):
        runner = QueueJobRunner.from_environ_or_config()
        runner.channel_manager.get_channel_by_name("root", autocreate=True).capacity = 0
        job1 = (
            self.env["res.partner"].with_delay(channel="root").create({"name": "test1"})
        )
        job2 = (
            self.env["res.partner"].with_delay(channel="root").create({"name": "test2"})
        )
        job3 = (
            self.env["res.partner"].with_delay(channel="root").create({"name": "test3"})
        )

        with mock.patch.object(
            QueueJobRunner, "from_environ_or_config", return_value=runner
        ):
            first_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)
            second_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)
            third_acquired_job = self.env["queue.job"]._acquire_one_job(commit=False)

        self.assertEqual(first_acquired_job, job1.db_record())
        self.assertEqual(second_acquired_job, job2.db_record())
        self.assertEqual(third_acquired_job, job3.db_record())

    @freeze_time("2022-02-22 22:22:22")
    def test_queue_job_creation_create_change_next_call(self):
        self.cron.nextcall = datetime(2021, 1, 21, 21, 21, 21)
        self.env["res.partner"].with_delay().create({"name": "test"})
        self.assertNotEqual(self.cron.nextcall, datetime(2022, 2, 22, 22, 22, 22))

    def test_release_started_jobs(self):
        job_known_pid = self.env["res.partner"].with_delay().create({"name": "test"})
        job_known_pid.set_started()
        job_known_pid.store()
        known_pid = job_known_pid.db_record().worker_pid
        job_unknown_pid = self.env["res.partner"].with_delay().create({"name": "test"})
        job_unknown_pid.set_started()
        job_unknown_pid.store()
        job_unknown_pid.db_record().worker_pid = -1

        self.env["queue.job"]._release_started_jobs(commit=False)

        self.assertEqual(job_unknown_pid.db_record().state, "pending")
        self.assertEqual(job_unknown_pid.db_record().worker_pid, 0)
        self.assertEqual(job_known_pid.db_record().state, "started")
        self.assertEqual(job_known_pid.db_record().worker_pid, known_pid)
