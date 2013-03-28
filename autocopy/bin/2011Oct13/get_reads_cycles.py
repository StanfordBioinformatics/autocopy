#!/usr/bin/env python

###############################################################################
#
# get_reads_cycles.py - Get reads and cycles from a run directory
#
# ARGS:
#   1st: Run directory.
#
# SWITCHES:
#
# OUTPUT:
#   <STDOUT>: Lines of form:
#              READS <reads>
#              CYCLES <cycles1> <cycles2>... <cycles<reads>>
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

(opts, args) = parser.parse_args()

if (len(args) == 0):
    print >> sys.stderr, os.path.basename(__file__), ": No run directories given"
    sys.exit(1)

(root, dir) = os.path.split(args[0])
run_dir = RunDir(root,dir)

reads = run_dir.get_reads()
print "READS",
if (reads is not None):
    print "%d" % reads
else:
    print "N/A"

cycle_list = run_dir.get_cycle_list()
print "CYCLES",
if (cycle_list != []):
    for c in cycle_list:
        print "%d" % c,
else:
    print "N/A",
print
