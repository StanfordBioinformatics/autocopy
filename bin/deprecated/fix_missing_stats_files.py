#!/usr/bin/env python

###############################################################################
#
# fix_missing_stats_files.py - Fix missing .stats files in a run directory
#
# ARGS:
#   1st: Run directory.
#
# SWITCHES:
#
# OUTPUT:
#   <STDOUT>:
#
# ASSUMPTIONS:
#
# AUTHOR:
#   Keith Bettinger
#
###############################################################################

#####
#
# IMPORTS
#
#####
from optparse import OptionParser
import os
import os.path
import sys

from rundir import RunDir
import rundir_utils

#####
#
# CONSTANTS
#
#####

#####
#
# FUNCTIONS
#
#####

#####
#
# SCRIPT BODY
#
#####

usage = "%prog [options] run_dir"
parser = OptionParser(usage=usage)

parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default=False,
                  help='Verbose mode [default = false]')

(opts, args) = parser.parse_args()

if (len(args) == 0):
    print >> sys.stderr, os.path.basename(__file__), ": No run directories given"
    sys.exit(1)

(root, dir) = os.path.split(os.path.abspath(args[0]))
            
run_dir = RunDir(root,dir)

rundir_utils.fix_missing_stats_files(run_dir, opts.verbose)
