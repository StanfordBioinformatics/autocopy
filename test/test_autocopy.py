#!/usr/bin/env python

DEBUG=True

import grp
import os
import pwd
import re
import shutil
import sys
import tempfile
import unittest

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from bin.autocopy import Autocopy
from bin.autocopy import ValidationError
from bin.rundir import RunDir

class CopyProcHelper:
    # This can be assigned to Rundir.copyproc
    # to simulate a rundir with a copyproc at various stages
    # retcode = 0, complete
    # retcode = None, still processing
    # retcode = nonzero int, error

    _retcode = None

    def __init__(self, retcode):
        self._retcode = retcode

    def poll(self):
        return self.retcode

class TestAutocopy(unittest.TestCase):

    def setUp(self):
        self.tmp_file = tempfile.NamedTemporaryFile()
        self.tmp_dir = tempfile.mkdtemp()
        self.config = {
            "COPY_DEST_HOST": "localhost",
        }

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    # --------------- INITIALIZATION --------------

    def testOptionsLogfile(self):
        a = Autocopy(log_file=self.tmp_file.name, run_root_dirs=[self.tmp_dir], no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(a.LOG_FILE.name, self.tmp_file.name)

    def testOptionsCopyOnly(self):
        a = Autocopy(no_copy=True, log_file=self.tmp_file.name, run_root_dirs=[self.tmp_dir], no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(a.MAX_COPY_PROCESSES, 0)

    def testOptionsRunRootList(self):
        one = os.path.join(self.tmp_dir, 'ek')
        two = os.path.join(self.tmp_dir, 'do')
        three = os.path.join(self.tmp_dir, 'tin')
        run_root_dirs=[one, two, three]
        a = Autocopy(run_root_dirs=run_root_dirs, log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(len(run_root_dirs), len(a.RUN_ROOT_DIRS))
        for run_root in run_root_dirs:
            self.assertIn(run_root, a.RUN_ROOT_DIRS)
                           
    def testOptionsRunRootListCWD(self):
        # With no run_root specified, defaults to CWD
        cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(os.path.realpath(self.tmp_dir), os.path.realpath(a.RUN_ROOT_DIRS[0]))
        os.chdir(cwd)
    
#    # Disabled because this test leaves a file in /var/log
#    def testOptionsLogfileDefaultDir(self):
#        # Verify that LOG_FILE defaults to the default log dir
#        LOG_DIR_DEFAULT = "/usr/local/log"
#        a = Autocopy(run_root_dirs=None)
#        self.assertEqual(os.path.dirname(a.LOG_FILE.name), LOG_DIR_DEFAULT)

    def testCreateRundir(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertTrue(os.path.exists(run_root))

    def testCreateSubdirs(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertTrue(os.path.exists(os.path.join(run_root, 'CopyCompleted')))
        self.assertTrue(os.path.exists(os.path.join(run_root, 'RunAborted')))

    def testConfig(self):
        max_copy_processes = 99
        email_from = 'test@example.com' 
        config = {
            'MAX_COPY_PROCESSES': max_copy_processes,
            'EMAIL_FROM': email_from,
            'COPY_DEST_HOST': 'localhost',
        }
        a = Autocopy(run_root_dirs=[self.tmp_dir], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=config)
        self.assertEqual(a.MAX_COPY_PROCESSES, max_copy_processes)
        self.assertEqual(a.EMAIL_FROM, email_from)

    def testConfigInvalid(self):
        copy_dest_host = 'example.com;deletesomestuff;oops'
        email_from = 'test@example.com' 
        config = {
            'COPY_DEST_HOST': copy_dest_host,
            'EMAIL_FROM': email_from
        }
        with self.assertRaises(ValidationError):
            a = Autocopy(run_root_dirs=[self.tmp_dir], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=config)

    # --------------- SERVICES -------------------

    def testLog(self):
        a = Autocopy(run_root_dirs=[self.tmp_dir], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.log('test', 'log', 'to', 'file')
        with open(a.LOG_FILE.name, 'r') as f:
            text = f.read()
            self.assertTrue(re.search('test log to file', text))

    def testLIMSConnection(self):
        a = Autocopy(run_root_dirs=[self.tmp_dir], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.LIMS.testconnection()

    def testSMTPConnection(self):
        a = Autocopy(run_root_dirs=[self.tmp_dir], log_file=self.tmp_file.name, no_email=False, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)

    # ------------------- EMAILS -------------------------

    def testEmails(self):
        runroot = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        rundirname = '141117_MONK_0387_AC4JCDACXX'
        rundir = RunDir(runroot, rundirname)
        rundir.set_copy_proc_and_start_time(1)
        rundir.unset_copy_proc_and_set_stop_time()
        a = Autocopy(run_root_dirs=[runroot], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        dummy_retcode = 1
        dummy_disk_usage = 1024
        is_rundir_valid = False
        are_files_missing = True
        dummy_problems_found = ['problem98', 'problem99']

        a.send_email_autocopy_exception(Exception('test exception'))
        a.send_email_autocopy_started()
        a.send_email_rundir_aborted(rundir, '/aborted/runs/directory')
        a.send_email_rundir_copy_failed(rundir, dummy_retcode)
        a.send_email_rundir_copy_complete(rundir, are_files_missing, dummy_problems_found, dummy_disk_usage)
        a.send_email_missing_rundir(rundir)
        a.send_email_low_freespace(runroot, dummy_disk_usage)
        a.send_email_rundirs_monitored_summary()

    # --------------- GENERAL UNIT TESTS -----------------

    def testIsRundirAborted(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        lims_runinfo = a.get_runinfo_from_lims(rundir)
        self.assertFalse(a.is_rundir_aborted(lims_runinfo))

    def testIsRundirReadyForCopy(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        self.assertTrue(a.is_rundir_ready_for_copy(rundir))

    def testProcessAbortedRundir(self):
        run_root = self.tmp_dir
        dirname='141117_MONK_0387_AC4JCDACXX'
        source_path = os.path.join(run_root, dirname)
        os.mkdir(source_path)
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=dirname)
        a.process_aborted_rundir(rundir)

        dest_path = os.path.join(run_root, a.SUBDIR_ABORTED, dirname)
        self.assertFalse(os.path.exists(source_path))
        self.assertTrue(os.path.exists(dest_path))
    
    def testProcessCompletedRundir(self):
        run_root = self.tmp_dir
        dirname='141117_MONK_0387_AC4JCDACXX'
        source_path = os.path.join(run_root, dirname)
        os.mkdir(source_path)
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=dirname)
        lims_runinfo = a.get_runinfo_from_lims(rundir)
        rundir.set_copy_proc_and_start_time(1)
        rundir.unset_copy_proc_and_set_stop_time()
        a.process_completed_rundir(rundir, lims_runinfo)

        dest_path = os.path.join(run_root, a.SUBDIR_COMPLETED, dirname)
        self.assertFalse(os.path.exists(source_path))
        self.assertTrue(os.path.exists(dest_path))

    def testUpdateRundirsMonitored(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        n_valid_runs = 2 # valid rundirs in root directory
        self.assertEqual(len(a.rundirs_monitored), n_valid_runs)

    def testAreFilesMissing(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        areFilesMissing = True
        self.assertEqual(a.are_files_missing(rundir), areFilesMissing)

    def testCheckRundirAgainstLims(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = Autocopy(run_root_dirs=[run_root], log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        runinfo = a.get_runinfo_from_lims(rundir)
        problems_found = a.check_rundir_against_lims(rundir, runinfo)
        self.assertEqual(len(problems_found), 0)

        (field, rundirval, limsval) = ('Test', '1', 'not 1')
        problems_found = a.check_rundir_against_lims(rundir, runinfo, testproblem=(field, rundirval, limsval))
        self.assertEqual(len(problems_found), 1)
        self.assertEqual(problems_found[0], 'Mismatched value "%s". Value in run directory: %s. Value in LIMS: %s' % (field, rundirval, limsval))

    def testStartCopy(self):
        run_name = '000000_RUNDIR_1234_ABCDEFG'
        source_run_root = os.path.join(self.tmp_dir, 'source')
        source_rundir = os.path.join(source_run_root, run_name)
        os.makedirs(source_rundir)
        testfile = 'test.txt'
        with open(os.path.join(source_rundir, testfile), 'w') as f:
            f.write("Hello")

        dest_run_root = os.path.join(self.tmp_dir, 'dest')
        dest_host = 'localhost'
        dest_group = grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
        dest_user = pwd.getpwuid(os.getuid()).pw_name
        os.makedirs(dest_run_root)

        config = {
            'COPY_DEST_HOST': dest_host,
            'COPY_DEST_USER': dest_user,
            'COPY_DEST_GROUP': dest_group,
            'COPY_DEST_RUN_ROOT': dest_run_root,
        }

        # Initialize autocopy and create the source root
        a = Autocopy(log_file=self.tmp_file.name, run_root_dirs=[source_run_root], no_email=True, test_mode_lims=True, config=config)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=run_name)
        a.start_copy(rundir)
        retcode = rundir.copy_proc.wait()
        self.assertEqual(retcode, 0)

        with open(os.path.join(dest_run_root, run_name, testfile), 'r') as f:
            text = f.read()
        self.assertTrue(re.search("Hello", text))

if __name__=='__main__':
    unittest.main()
