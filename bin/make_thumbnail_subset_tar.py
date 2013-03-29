#!/usr/bin/env python

###############################################################################
#
# make_thumbnail_subset_tar.py - Make the thumbnail image subset tar in a run directory
#
# ARGS:
#   All: Run directories.
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

for arg in args:
    (root, dir) = os.path.split(os.path.abspath(arg))

    rundir = RunDir(root,dir)

    if not rundir_utils.make_thumbnail_subset_tar(rundir, verbose=opts.verbose):
        print >> sys.stderr, "make_thumbnail_subset_tar: %s failed" % rundir.get_dir()
