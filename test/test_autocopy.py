#!/usr/bin/env python

DEBUG=True

import grp
import os
import pwd
import re
import shutil
import sys
import tempfile

if sys.version_info[0:2] == (2, 6):
    import unittest2 as unittest
else:
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
        self.run_root = tempfile.mkdtemp()
        self.config = {
            "COPY_DEST_HOST": "localhost",
            "COPY_SOURCE_RUN_ROOTS": [self.run_root],
        }
        self.test_run_name = '141117_MONK_0387_AC4JCDACXX'
        self.test_run_path = os.path.join(self.run_root, self.test_run_name)
        source = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0', self.test_run_name))
        dest = os.path.join(self.run_root, self.test_run_name)
        shutil.copytree(source, dest)

    def tearDown(self):
        shutil.rmtree(self.run_root)

    # --------------- INITIALIZATION --------------

    def testOptionsLogfile(self):
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(a.LOG_FILE.name, self.tmp_file.name)
        a.cleanup()

    def testOptionsCopyOnly(self):
        a = Autocopy(no_copy=True, log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(a.MAX_COPY_PROCESSES, 0)
        a.cleanup()

    def testOptionsRunRootList(self):
        one = os.path.join(self.run_root, 'ek')
        two = os.path.join(self.run_root, 'do')
        three = os.path.join(self.run_root, 'tin')
        run_root_dirs=[one, two, three]
        self.config.update({'COPY_SOURCE_RUN_ROOTS': run_root_dirs})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(len(run_root_dirs), len(a.COPY_SOURCE_RUN_ROOTS))
        for run_root in run_root_dirs:
            self.assertIn(run_root, a.COPY_SOURCE_RUN_ROOTS)
        a.cleanup()
                           
    def testOptionsRunRootListCWD(self):
        # With no run_root specified, defaults to CWD
        cwd = os.getcwd()
        os.chdir(self.run_root)
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertEqual(os.path.realpath(self.run_root), os.path.realpath(a.COPY_SOURCE_RUN_ROOTS[0]))
        os.chdir(cwd)
        a.cleanup()
    
    def testCreateSubdirs(self):
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        self.assertTrue(os.path.exists(os.path.join(self.run_root, 'Runs_Completed')))
        self.assertTrue(os.path.exists(os.path.join(self.run_root, 'Runs_Aborted')))
        a.cleanup()

    def testConfig(self):
        max_copy_processes = 99
        email_from = 'test@example.com' 
        self.config.update(
            {
                'MAX_COPY_PROCESSES': max_copy_processes,
                'EMAIL_FROM': email_from,
                'COPY_DEST_HOST': 'localhost',
            }
        )
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config)
        self.assertEqual(a.MAX_COPY_PROCESSES, max_copy_processes)
        self.assertEqual(a.EMAIL_FROM, email_from)
        a.cleanup()

    def testConfigInvalid(self):
        copy_dest_host = 'example.com;deletesomestuff;oops'
        email_from = 'test@example.com' 
        self.config.update({
            'COPY_DEST_HOST': copy_dest_host,
            'EMAIL_FROM': email_from
        })
        with self.assertRaises(ValidationError):
            a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config)

    # --------------- SERVICES -------------------

    def testLog(self):
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.log('test', 'log', 'to', 'file')
        with open(a.LOG_FILE.name, 'r') as f:
            text = f.read()
            self.assertTrue(re.search('test log to file', text))
        a.cleanup()

    def testLIMSConnection(self):
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.LIMS.testconnection()
        a.cleanup()

    def testSMTPConnection(self):
        a = Autocopy(log_file=self.tmp_file.name, no_email=False, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.cleanup()

    # ------------------- EMAILS -------------------------

    def testEmails(self):
        runroot = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [runroot]})
        rundirname = '141117_MONK_0387_AC4JCDACXX'
        rundir = RunDir(runroot, rundirname)
        rundir.set_copy_proc_and_start_time(1)
        rundir.unset_copy_proc_and_set_stop_time()
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        dummy_retcode = 1
        dummy_disk_usage = 1024
        is_rundir_valid = False
        are_files_missing = True
        dummy_problems_found = ['problem98', 'problem99']

        a.send_email_autocopy_exception(Exception('test exception'))
        a.send_email_autocopy_started()
        a.send_email_autocopy_stopped()
        a.send_email_rundir_aborted(rundir, '/aborted/runs/directory')
        a.send_email_rundir_copy_failed(rundir, dummy_retcode)
        a.send_email_rundir_copy_complete(rundir, are_files_missing, dummy_problems_found, dummy_disk_usage)
        a.send_email_missing_rundir(rundir)
        a.send_email_low_freespace(runroot, dummy_disk_usage)
        a.send_email_rundirs_monitored_summary()
        a.cleanup()

    # --------------- GENERAL UNIT TESTS -----------------

    def testIsRundirAborted(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [run_root]})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        lims_runinfo = a.get_runinfo_from_lims(rundir)
        self.assertFalse(a.is_rundir_aborted(lims_runinfo))
        a.cleanup()

    def testIsRundirReadyForCopy(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [run_root]})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        self.assertTrue(a.is_rundir_ready_for_copy(rundir))
        a.cleanup()

    def testProcessAbortedRundir(self):
        dirname = self.test_run_name
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=dirname)
        runinfo = a.get_runinfo_from_lims(rundir)
        a.process_aborted_rundir(rundir, runinfo)

        
        dest_path = os.path.join(self.run_root, a.SUBDIR_ABORTED, dirname)
        self.assertFalse(os.path.exists(self.test_run_path))
        self.assertTrue(os.path.exists(dest_path))

        # These are broken because cached runinfo doesn't get updated
        # when solexarun and solexaflowcell get updated.
        # self.assertEqual(runinfo.getflowcellstatus(), 'done')
        # self.assertEqual(runinfo.isanalysisdone(), True)
    
        a.cleanup()

    def testProcessCompletedRundir(self):
        run_root = self.run_root
        dirname=self.test_run_name
        source_path = self.test_run_path
        
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=dirname)
        lims_runinfo = a.get_runinfo_from_lims(rundir)
        rundir.set_copy_proc_and_start_time(1)
        rundir.unset_copy_proc_and_set_stop_time()
        a.process_completed_rundir(rundir, lims_runinfo)

        dest_path = os.path.join(run_root, a.SUBDIR_COMPLETED, dirname)
        self.assertFalse(os.path.exists(source_path))
        self.assertTrue(os.path.exists(dest_path))
        a.cleanup()

    def testUpdateRundirsMonitored(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [run_root]})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        n_valid_runs = 2 # valid rundirs in root directory
        self.assertEqual(len(a.rundirs_monitored), n_valid_runs)
        a.cleanup()

    def testAreFilesMissing(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [run_root]})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        areFilesMissing = True
        self.assertEqual(a.are_files_missing(rundir), areFilesMissing)
        a.cleanup()

    def testCheckRundirAgainstLims(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        self.config.update({'COPY_SOURCE_RUN_ROOTS': [run_root]})
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=self.config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        runinfo = a.get_runinfo_from_lims(rundir)
        problems_found = a.check_rundir_against_lims(rundir, runinfo)
        self.assertEqual(len(problems_found), 0)

        (field, rundirval, limsval) = ('Test', '1', 'not 1')
        problems_found = a.check_rundir_against_lims(rundir, runinfo, test_only_dummy_problem=(field, rundirval, limsval))
        self.assertEqual(len(problems_found), 1)
        self.assertEqual(problems_found[0], 'Mismatched value "%s". Value in run directory: "%s". Value in LIMS: "%s"' % (field, rundirval, limsval))
        a.cleanup()

    def testStartCopy(self):
        run_name = '000000_RUNDIR_1234_ABCDEFG'
        source_run_root = os.path.join(self.run_root, 'source')
        source_rundir = os.path.join(source_run_root, run_name)
        os.makedirs(source_rundir)
        testfile = 'test.txt'
        with open(os.path.join(source_rundir, testfile), 'w') as f:
            f.write("Hello")

        dest_run_root = os.path.join(self.run_root, 'dest')
        dest_host = 'localhost'
        dest_group = grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
        dest_user = pwd.getpwuid(os.getuid()).pw_name
        os.makedirs(dest_run_root)

        config = {
            'COPY_DEST_HOST': dest_host,
            'COPY_DEST_USER': dest_user,
            'COPY_DEST_GROUP': dest_group,
            'COPY_DEST_RUN_ROOT': dest_run_root,
            'COPY_SOURCE_RUN_ROOTS': [source_run_root],
        }

        # Initialize autocopy and create the source root
        a = Autocopy(log_file=self.tmp_file.name, no_email=True, test_mode_lims=True, config=config, errors_to_terminal=DEBUG)
        a.update_rundirs_monitored()
        rundir = a.get_rundir(dirname=run_name)
        a.start_copy(rundir)
        retcode = rundir.copy_proc.wait()
        self.assertEqual(retcode, 0)

        with open(os.path.join(dest_run_root, run_name, testfile), 'r') as f:
            text = f.read()
        self.assertTrue(re.search("Hello", text))
        a.cleanup()

if __name__=='__main__':
    unittest.main()
