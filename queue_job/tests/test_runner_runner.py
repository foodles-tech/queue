# Copyright 2015-2016 Camptocamp SA
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)

# pylint: disable=odoo-addons-relative-import
# we are testing, we want to test as we were an external consumer of the API
import os
from unittest import TestCase

from odoo.addons.queue_job.jobrunner import runner

from .common import load_doctests

load_tests = load_doctests(runner)


class TestRunner(TestCase):
    def test_runner_file_descriptor(self):
        def is_fd_open(fd):
            try:
                os.fstat(fd)  # Vérifie si le fd est valide
                return True  # Pas d'erreur => le fd est ouvert
            except OSError:
                return False  # Erreur => le fd est fermé

        a_runner = runner.QueueJobRunner.from_environ_or_config()

        read_fd, write_fd = a_runner._stop_pipe
        self.assertTrue(is_fd_open(read_fd))
        self.assertTrue(is_fd_open(write_fd))

        del a_runner

        self.assertFalse(is_fd_open(read_fd))
        self.assertFalse(is_fd_open(write_fd))
