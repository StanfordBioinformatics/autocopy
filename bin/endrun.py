#!/usr/bin/env python

##############################################################################
#
# endrun.py - Sets the appropriate flags in the LIMS to complete an analysis.
#
# ARGS:
#   1st: run name
#
# SWITCHES:
#   none
#
# OUTPUT:
#   none
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
import glob
import os
import os.path
import re
import sys

import lims

#####
#
# CONSTANTS
#
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

usage = "%prog [options] run_name+"
description = "Updates status flags on the LIMS. Sets the analysis run status to 'Finished' (unsetting it if -r), and sets the run status to 'Analysis Done' (unsetting it if -r)."
parser = OptionParser(usage=usage)

parser.add_option("-p", "--pipeline_id", dest="pipeline_id", 
                  default=None,
                  help='input the pipeline ID for the run directly. [default = find it in run dir]')
parser.add_option("-r", "--reverse", dest="reverse", action="store_true",
                  default=False,
                  help='reverse the end run steps. [default = False]')
parser.add_option("--run-root",default="/srv/gsfs0/projects/seq_center/Illumina/RunsInProgress",help="Folder path containing run directories. Default is %default")

(opts, args) = parser.parse_args()

# Validate remaining args.
if len(args) == 0:
    print >> sys.stderr, "need at least one run name"
    sys.exit(-1)

run_root = opts.run_root
exit_status = 0

lims_obj = lims.LIMS()
# Open ssh socket for LIMS.

for run_name in args:
    print >> sys.stderr, "=========="
    print >> sys.stderr, "ENDING RUN %s IN LIMS" % (run_name)
    print >> sys.stderr, "=========="
    if opts.reverse:
        print >> sys.stderr, "[Reversing]"

    #
    # Change Pipeline Run status to Finished.
    #

    # What is the Pipeline Run ID?  If the user didn't give us one, we have to look.
    if not opts.pipeline_id:

        run_dir = os.path.join(run_root, run_name)
        ana_dirs = glob.glob(os.path.join(run_dir,"analysis_*"))
        pipeline_ids = map(lambda d: re.search("(\d+)$",d).group(1), ana_dirs)

        if len(pipeline_ids) == 1:
            pipeline_id = pipeline_ids[0]
        else:
            # End the largest ID.
            print >> sys.stderr, "Pipeline IDs: ",
            max_id = -1
            for ids in pipeline_ids:
                if ids > max_id: max_id = ids
            print >> sys.stderr, "",
            print >> sys.stderr, "",
            print >> sys.stderr, "Using largest ID: %s" % max_id
            pipeline_id = max_id
    else:
        pipeline_id = opts.pipeline_id

    if not opts.reverse:
        if lims_obj.lims_pipeline_modify(pipeline_id, {'finished': True}):
            print >> sys.stderr, "set Pipeline Run (ID = %s) to Finished" % (pipeline_id)
        else:
            print >> sys.stderr, "couldn't set Pipeline Run (ID = %s) to Finished" % (pipeline_id)
            exit_status = -1
    else:
        if lims_obj.lims_pipeline_modify(pipeline_id, {'finished': False}):
            print >> sys.stderr, "REVERSE: set Pipeline Run (ID = %s) to NOT Finished" % (pipeline_id)
        else:
            print >> sys.stderr, "REVERSE: couldn't set Pipeline Run (ID = %s) to NOT Finished" % (pipeline_id)
            exit_status = -1

    #
    # Check the Solexa Run flag "Analysis Done".
    #
    if not opts.reverse:
        if lims_obj.lims_run_modify_params(run_name, { "analysis_done": True }):
            print >> sys.stderr, "set Analysis Done flag."
        else:
            print >> sys.stderr, "couldn't set Analysis Done flag."
            exit_status = -1
    else:
        if lims_obj.lims_run_modify_params(run_name, { "analysis_done": False }):
            print >> sys.stderr, "REVERSE: CLEAR Analysis Done flag."
        else:
            print >> sys.stderr, "REVERSE: couldn't CLEAR Analysis Done flag."
            exit_status = -1

    #
    # Change Flowcell Status to "Reviewing Results".
    #

    # Get flowcell from run fields.
    run_fields = lims_obj.lims_run_get_fields(run_name)
    if run_fields:

        flowcell = run_fields["flowcell"]

        if not opts.reverse:
            if lims_obj.lims_flowcell_modify_status(name=flowcell, status="reviewing"):
                print >> sys.stderr, "changed flowcell %s to status Reviewing Results" % (flowcell)
            else:
                print >> sys.stderr, "couldn't modify flowcell %s to status Reviewing Results" % (flowcell)
                exit_status = -1
        else:
            if lims_obj.lims_flowcell_modify_status(name=flowcell, status="analyzing"):
                print >> sys.stderr, "REVERSE: changed flowcell %s to status ANALYZING" % (flowcell)
            else:
                print >> sys.stderr, "REVERSE: couldn't modify flowcell %s to status ANALYZING" % (flowcell)
                exit_status = -1

    else:
        print >> sys.stderr, "couldn't get fields for %s" % (run_name)
        exit_status = -1

    print >> sys.stderr, "RUN %s/PIPELINE ID %s ended." % (run_name, pipeline_id)

sys.exit(exit_status)
