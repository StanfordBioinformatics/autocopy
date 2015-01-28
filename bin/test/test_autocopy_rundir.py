#!/usr/bin/env python

import os
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
        a = AutocopyRundir(log_file=self.tmp_file.name, run_root_list=[self.tmp_dir], redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertEqual(a.LOG_FILE.name, self.tmp_file.name)
        
#    def testOptionsLogfileDefaultDir(self):
#        # Disabled because this test leaves an empty file behind.
#        # Verify that LOG_FILE defaults to the default log dir
#        LOG_DIR_DEFAULT = "/usr/local/log"
#        a = AutocopyRundir(run_root_list=None)
#        self.assertEqual(os.path.dirname(a.LOG_FILE.name), LOG_DIR_DEFAULT)

    def testOptionsCopyOnly(self):
        a = AutocopyRundir(no_copy=True, log_file=self.tmp_file.name, run_root_list=[self.tmp_dir], redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertEqual(a.COPY_PROCESSES, 0)

    def testOptionsRunRootList(self):
        one = os.path.join(self.tmp_dir, 'ek')
        two = os.path.join(self.tmp_dir, 'do')
        three = os.path.join(self.tmp_dir, 'tin')
        run_root_list=[one, two, three]
        a = AutocopyRundir(run_root_list=run_root_list, log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertEqual(len(run_root_list), len(a.RUN_ROOT_LIST))
        for run_root in run_root_list:
            self.assertIn(run_root, a.RUN_ROOT_LIST)
                           
    def testOptionsRunRootListCWD(self):
        # With no run_root specified, defaults to CWD
        cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        a = AutocopyRundir(log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertEqual(os.path.realpath(self.tmp_dir), os.path.realpath(a.RUN_ROOT_LIST[0]))
        os.chdir(cwd)
    
    def testCreateRundir(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = AutocopyRundir(run_root_list=[run_root], redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertTrue(os.path.exists(run_root))

    def testCreateSubdirs(self):
        run_root = os.path.join(self.tmp_dir, 'run1')
        a = AutocopyRundir(run_root_list=[run_root], redirect_stdout_stderr_to_log=False, no_mail=True)
        self.assertTrue(os.path.exists(os.path.join(run_root, 'Archived')))
        self.assertTrue(os.path.exists(os.path.join(run_root, 'Aborted')))

    def testLog(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.log('test', 'log', 'to', 'file')
        with open(a.LOG_FILE.name, 'r') as f:
            text = f.read()
            self.assertTrue(re.search('test log to file', text))

    def testLIMSConnection(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.CONN.testconnection()

    def testSMTPConnection(self):
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=False)

    def testAddToActiveRundirsInvalid(self):
        entry = '150102_INVALID_0002_OPQRST'
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        n_rundirs_before = len(a.active_rundirs_w_statuses)
        a.add_to_active_rundirs(entry, run_root)
        self.assertEqual(len(a.active_rundirs_w_statuses), n_rundirs_before)

    def testAddToActiveRundirsValid(self):
        entry1 = '141225_TEST_0000_ABCDEFG'
        entry2 = '150101_TEST_0001_HIJKLMN'
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[self.tmp_dir], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        n_rundirs_before = len(a.active_rundirs_w_statuses)
        a.add_to_active_rundirs(entry1, run_root)
        a.add_to_active_rundirs(entry2, run_root)
        self.assertEqual(len(a.active_rundirs_w_statuses), n_rundirs_before + 2)

    def testScanRunRootsAddUnkonwn(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots()
        n_valid_runs = 2 # valid rundirs in root directory
        self.assertEqual(len(a.active_rundirs_w_statuses), n_valid_runs)

    def testGetCopyingRundirs(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots() # To initialize active_rundirs list
        n_valid_runs = 2 #valid rundirs in root directory
        self.assertEqual(len(a.active_rundirs_w_statuses), n_valid_runs)

        # Switch one run to a copying status
        a.active_rundirs_w_statuses[0][1] = RunDir.STATUS_COPY_STARTED
        n_copying_runs = 1
        self.assertEqual(len(a.get_copying_rundirs()), n_copying_runs)
        
    def testExamineCopyingDirsFailed(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots() # To initialize active_rundirs list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.active_rundirs_w_statuses[0][1] = RunDir.STATUS_COPY_STARTED
        a.active_rundirs_w_statuses[0][1] = RunDir.copyproc = failedCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsAlreadyExists(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots() # To initialize active_rundirs list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.active_rundirs_w_statuses[0][1] = RunDir.STATUS_COPY_STARTED
        a.active_rundirs_w_statuses[0][1] = RunDir.copyproc = destAlreadyExistsCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsSuccess(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots() # To initialize active_rundirs list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.active_rundirs_w_statuses[0][1] = RunDir.STATUS_COPY_STARTED
        a.active_rundirs_w_statuses[0][1] = RunDir.copyproc = successCopyProc
        a.examine_copying_dirs()
        #TODO

    def testExamineCopyingDirsRunning(self):
        run_root = os.path.realpath(os.path.join(os.path.dirname(__file__), 'testdata', 'RunRoot0'))
        a = AutocopyRundir(run_root_list=[run_root], log_file=self.tmp_file.name, redirect_stdout_stderr_to_log=False, no_mail=True)
        a.scan_run_roots() # To initialize active_rundirs list
        # Assign a fake copyproc object to imitate a copy that is completed, in progress, failed because dest exists, or failed for unkown
        failedCopyProc = CopyProcHelper(7)
        destAlreadyExistsCopyProc = CopyProcHelper(5)
        successCopyProc = CopyProcHelper(0)
        runningCopyProc = CopyProcHelper(None)
        a.active_rundirs_w_statuses[0][1] = RunDir.STATUS_COPY_STARTED
        a.active_rundirs_w_statuses[0][1] = RunDir.copyproc = runningCopyProc
        a.examine_copying_dirs()
        #TODO

        

if __name__=='__main__':
    unittest.main()
