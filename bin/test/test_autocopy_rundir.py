#!/usr/bin/env python

import grp
import os
import pwd
import re
import shutil
import tempfile
import unittest

from autocopy.bin.autocopy_rundir import AutocopyRundir
from autocopy.bin.rundir import RunDir

class CopyProcHelper:
    # This can be assigned to Rundir.copyproc
    # to simulate a rundir with a copyproc at various stages
    # retcode = 0, complete
    # retcode = 5, Run directory already exists at destination
    # retcode = other (but not None), unknown error

    _retcode = None

    def __init__(self, retcode):
        self._retcode = retcode

    def poll(self):
        return self.retcode

class TestAutocopyRundir(unittest.TestCase):

    def setUp(self):
        self.tmp_file = tempfile.NamedTemporaryFile()
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def testOptionsLogfile(self):
        a = AutocopyRundir(log_file=self.tmp_file.name, run_root_list=[self.tmp_dir], redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertEqual(a.LOG_FILE.name, self.tmp_file.name)
        
     # Disabled because this test leaves an empty file behind.
#    def testOptionsLogfileDefaultDir(self):
#        # Verify that LOG_FILE defaults to the default log dir
#        LOG_DIR_DEFAULT = "/usr/local/log"
#        a = AutocopyRundir(run_root_list=None)
#        self.assertEqual(os.path.dirname(a.LOG_FILE.name), LOG_DIR_DEFAULT)

    def testOptionsCopyOnly(self):
        a = AutocopyRundir(no_copy=True, log_file=self.tmp_file.name, run_root_list=[self.tmp_dir], redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertEqual(a.MAX_COPY_PROCESSES, 0)

    def testOptionsRunRootList(self):
        one = os.path.join(self.tmp_dir, 'ek')
        two = os.path.join(self.tmp_dir, 'do')
        three = os.path.join(self.tmp_dir, 'tin')
        run_root_list=[one, two, three]
        a = AutocopyRundir(run_root_list=run_root_list, log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertEqual(len(run_root_list), len(a.RUN_ROOT_LIST))
        for run_root in run_root_list:
            self.assertIn(run_root, a.RUN_ROOT_LIST)
                           
    def testOptionsRunRootListCWD(self):
        # With no run_root specified, defaults to CWD
        cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        a = AutocopyRundir(log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertEqual(os.path.realpath(self.tmp_dir), os.path.realpath(a.RUN_ROOT_LIST[0]))
        os.chdir(cwd)
    
    def testCreateRundir(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = AutocopyRundir(run_root_list=[run_root], redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertTrue(os.path.exists(run_root))

    def testCreateSubdirs(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = AutocopyRundir(run_root_list=[run_root], redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        self.assertTrue(os.path.exists(os.path.join(run_root, 'Archived')))
        self.assertTrue(os.path.exists(os.path.join(run_root, 'Aborted')))

    def testLog(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.log('test', 'log', 'to', 'file')
        with open(a.LOG_FILE.name, 'r') as f:
            text = f.read()
            self.assertTrue(re.search('test log to file', text))

    def testLIMSConnection(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.LIMS.testconnection()

    def testSMTPConnection(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=False, test_mode_lims=True)

#    # Commented out because I pulled the AutocopyRundir.isValid method. Confusing alongside rundir_utils.validate, and didn't seem necessary.
#    def testIsRundirValid(self):
#        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
#        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
#        a.scan_run_roots_to_discover_rundirs()
#        rundirs = a.rundirs_monitored
#        isValid = a.is_rundir_valid(rundirs[0])
#        self.assertFalse(isValid)

    def testScanRunRootsAddUnkonwn(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs()
        n_valid_runs = 2 # valid rundirs in root directory
        self.assertEqual(len(a.rundirs_monitored), n_valid_runs)

    def testScanRundirForMissingFiles(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        isValid = False
        self.assertEqual(a.scan_rundir_for_missing_files(rundir), isValid)

    def testCheckRundirAgainstLims(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs()
        rundir = a.get_rundir(dirname='141117_MONK_0387_AC4JCDACXX')
        problems_found = a.check_rundir_against_lims(rundir)
        self.assertEqual(len(problems_found), 0)

        (field, rundirval, limsval) = ('Test', '1', 'not 1')
        problems_found = a.check_rundir_against_lims(rundir, testproblem=(field, rundirval, limsval))
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

        # Initialize autocopy and create the source root
        a = AutocopyRundir(log_file=self.tmp_file.name, run_root_list=[source_run_root], redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)

        a.cleanup_ssh_socket()
        a.COPY_DEST_HOST  = dest_host
        a.COPY_DEST_USER  = dest_user
        a.COPY_DEST_GROUP = dest_group
        a.COPY_DEST_RUN_ROOT = dest_run_root
        a.initialize_ssh_socket()

        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        rundir = a.get_rundir(dirname=run_name)
        a.start_copy(rundir)
        retcode = rundir.copy_proc.wait()
        self.assertEqual(retcode, 0)

        with open(os.path.join(dest_run_root, run_name, testfile), 'r') as f:
            text = f.read()
        self.assertTrue(re.search("Hello", text))


    def testSetStatus(self):
        pass

    def testReadyForCopyDirs(self):
        pass

    def testGetCopyingRundirs(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        n_valid_runs = 2 #valid rundirs in root directory
        self.assertEqual(len(a.rundirs_monitored), n_valid_runs)

        # Switch one run to a copying status
        a.rundirs_monitored[0].status = RunDir.STATUS_COPY_STARTED
        n_copying_runs = 1
        self.assertEqual(len(a.get_copying_rundirs()), n_copying_runs)

    def testGetXXRundirs(self):
        # No validation, just exercising the code
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        n_valid_runs = 2 #valid rundirs in root directory
        self.assertEqual(len(a.rundirs_monitored), n_valid_runs)

        a.get_rundirs()
        a.get_ready_for_copy_rundirs()
        a.get_copying_rundirs()
        a.get_running_rundirs()
        a.get_completed_rundirs()
        a.get_aborted_rundirs()

    """
    def testGenerateRundirsTable(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        run_status_table = a.generate_run_status_table()
        # Verify snippets of expected text
        self.assertTrue(re.search('RUNNING DIRECTORIES', run_status_table))
        run1 = '141117_MONK_0387_AC4JCDACXX'
        run2 = '141126_PINKERTON_0343_BC4J1PACXX'
        self.assertTrue(re.search(run1, run_status_table))
        
    def testExamineCopyingDirsRetCodeFailed(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.rundirs_monitored[0].cached_rundir_status = RunDir.STATUS_COPY_STARTED
        a.rundirs_monitored[0].cached_rundir_status = RunDir.copyproc = failedCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsRetCodeAlreadyExists(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.rundirs_monitored[0].cached_rundir_status = RunDir.STATUS_COPY_STARTED
        a.rundirs_monitored[0].cached_rundir_status = RunDir.copyproc = destAlreadyExistsCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsRetCodeSuccess(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.rundirs_monitored[0].cached_rundir_status = RunDir.STATUS_COPY_STARTED
        a.rundirs_monitored[0].cached_rundir_status = RunDir.copyproc = successCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsRetCodeRunning(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True, test_mode_lims=True)
        a.scan_run_roots_to_discover_rundirs() # To initialize rundirs_monitored list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.rundirs_monitored[0].cached_rundir_status = RunDir.STATUS_COPY_STARTED
        a.rundirs_monitored[0].cached_rundir_status = RunDir.copyproc = runningCopyProc
        a.examine_copying_dirs()
        #TODO
"""
        

if __name__=='__main__':
    unittest.main()
