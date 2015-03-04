#!/usr/bin/env python

import os
import sys

if sys.version_info[0:2] == (2, 6):
    import unittest2 asunittest
else:
    import unittest

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from bin.rundir import RunDir

class TestRundir(unittest.TestCase):

    def setUp(self):
        self.runname = '141117_MONK_0387_AC4JCDACXX'
        self.runroot = os.path.join('.', 'testdata', 'RunRoot0')
        self.rundir = RunDir(self.runroot, self.runname)

    def tearDown(self):
        pass

    def testGetDir(self):
        self.assertEqual(self.rundir.get_dir(), self.runname)

    def testGetStartDate(self):
        self.rundir.get_start_date()

    def testGetDataVolume(self):
        # Should return None if not on a standard path like IlluminaRuns3
        self.assertEqual(self.rundir.get_data_volume(), None)

    def testStr(self):
        self.rundir.str()

if __name__=='__main__':
    unittest.main()
    
