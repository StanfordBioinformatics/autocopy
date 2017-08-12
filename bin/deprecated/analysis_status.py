#!/usr/bin/env python

###############################################################################
#
# analysis_status.py - Query/control the analysis status files in a run dir.
#
# ARGS:
#   1st: Run directory.
#   2nd: Lane number (optional)
#   3rd: Set status (optional)
#
# SWITCHES:
#   --query:   Check for the status of the lane given.
#   --set:     Change the status of the run dir to the given one.
#   --unset:   Remove the given status from the run dir.
#
# OUTPUT:
#   <STDOUT>:
#     If --query:  A string denoting the analysis status.
#     If --set:    A string confirming the setting of the analysis status.
#     If --unset:  A string confirming the unsetting of the analysis status.
#
# ASSUMPTIONS:
#   Input status strings for arg 3 and output status strings for --query results
#   are one of the following:
#      NONE
#      STARTED
#      PRERUN_STARTED
#      PRERUN_COMPLETE
#      BCL_STARTED
#      BCL_COMPLETE
#      MAPPING_STARTED
#      MAPPING_COMPLETE
#      PUBLISH_STARTED
#      PUBLISH_COMPLETE
#      POSTRUN_STARTED
#      POSTRUN_COMPLETE
#      COMPLETE
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
import datetime
import os
import sys
import time

from rundir import RunDir


#####
#
# CONSTANTS
#
#####
STATUS_STRINGS = map(lambda x: x[RunDir.ANALYSIS_STATUS_IDX_COMP_STRING],
                     RunDir.ANALYSIS_STATUS_ARRAY)

WAIT_SLEEP_INTERVAL = 60*60 # secs = 1 hr

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

usage = "%prog [options] run_dir [status]"
parser = OptionParser(usage=usage)

parser.add_option("--query", action="store_true",
                  default=False,
                  help='Query the existing status [default = false]')
parser.add_option("--set", action="store_true",
                  default=False,
                  help='Set the status [default = false]')
parser.add_option("--unset", action="store_true",
                  default=False,
                  help='Unset the status [default = false]')
parser.add_option("--lane", type="int",
                  default=None,
                  help='Lane number [default = None (all lanes)]')
parser.add_option("--wait", action="store_true",
                  default=False,
                  help='Wait the status to become given status [default = false]')
parser.add_option("-q", "--quiet", action="store_true",
                  default=False,
                  help='No logging output [default = false]')
parser.add_option("-v", "--verbose", action="store_true",
                  default=False,
                  help='Verbose mode [default = false]')

(opts, args) = parser.parse_args()

if not (opts.query or opts.set or opts.unset or opts.wait):
    print >> sys.stderr, os.path.basename(__file__), ": No commands given (--query, --set, --unset, --wait)"
    parser.print_help()
    sys.exit(1)

args_len = len(args)
if args_len == 0:
    print >> sys.stderr, os.path.basename(__file__), ": No run directories given"
    sys.exit(2)

run_directory_string = args[0]

if args_len > 1:
    status_string = args[1]
    if status_string not in STATUS_STRINGS:
        print >> sys.stderr, os.path.basename(__file__), ": Unknown status string %s" % status_string
        sys.exit(3)

    # Convert the status string to a status value.
    status_value = STATUS_STRINGS.index(status_string)

(root, dir) = os.path.split(os.path.abspath(run_directory_string))
rundir = RunDir(root,dir)

if opts.query:
    curr_status_string = rundir.get_analysis_status_string(opts.lane, human_readable=False)
    print curr_status_string

elif opts.set:
    rundir.set_analysis_status(opts.lane, status_value)

elif opts.unset:
    rundir.unset_analysis_status(opts.lane, status_value)

elif opts.wait:

    if not opts.quiet:
        print >> sys.stderr, "Tracking status of rundir %s:" % rundir.get_path()

    while True:
        curr_status_value = rundir.get_analysis_status(opts.lane)

        if curr_status_value == status_value:
            break

        if not opts.quiet:
            if opts.lane is not None:
                log_line = "Status is %s" % (rundir.get_analysis_status_string(opts.lane, human_readable=True))
            else:
                log_line = "Status of lane %s is %s" % (opts.lane, rundir.get_analysis_status_string(opts.lane, human_readable=True))

            print >> sys.stderr, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), log_line)

        # Sleep for a while.
        time.sleep(WAIT_SLEEP_INTERVAL)

    if not opts.quiet:
        print >> sys.stderr, "[%s] Desired status of %s reached." % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"),
                                                                     rundir.get_analysis_status_string(opts.lane))

