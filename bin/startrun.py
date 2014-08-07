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
#		Keith Bettinger
#		Nathaniel Watson
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
RUN_ROOT = "/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/RunsInProgress"

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
description = "Updates status flags on the LIMS. Sets the analysis run status in the LIMS to 'Started' (unsetting it if -r)."
parser = OptionParser(usage=usage)

parser.add_option("-p", "--pipeline_id", dest="pipeline_id", 
                  default=None,
                  help='input the pipeline ID for the run directly. [default = find it in run dir]')
parser.add_option("-r", "--reverse", dest="reverse", action="store_true",
                  default=False,
                  help="Uncheck the 'Started' check box. [default = False]")

(opts, args) = parser.parse_args()

# Validate remaining args.
if len(args) == 0:
    print >> sys.stderr, "need at least one run name"
    sys.exit(-1)

exit_status = 0

lims_obj = lims.LIMS()
# Open ssh socket for LIMS.

for run_name in args:
    print >> sys.stderr, "=========="
    if opts.reverse:
      print >> sys.stderr, "Setting run %s to not 'Started' IN LIMS" % (run_name)
    else:
      print >> sys.stderr, "Setting Run %s to 'Started' IN LIMS" % (run_name)
    print >> sys.stderr, "=========="
    #
    # Change Pipeline Run status to Finished.
    #

    # What is the Pipeline Run ID?  If the user didn't give us one, we have to look.
    if not opts.pipeline_id:

        run_dir = os.path.join(RUN_ROOT, run_name)
        ana_dirs = glob.glob(os.path.join(run_dir,"analysis_*"))
        pipeline_ids = map(lambda d: re.search("(\d+)$",d).group(1), ana_dirs)

        if len(pipeline_ids) == 1:
            pipeline_id = pipeline_ids[0]
        else:
            # End the largest ID.
            print >> sys.stderr, "Pipeline IDs: ",
            max_id = -1
            for ids in pipeline_ids:
                print id,
                if ids > max_id: max_id = ids
            print >> sys.stderr, "",
            print >> sys.stderr, "",
            print >> sys.stderr, "Using largest ID: %s" % max_id
            pipeline_id = max_id
    else:
        pipeline_id = opts.pipeline_id

    if not opts.reverse:
        if lims_obj.lims_pipeline_modify(pipeline_id, {'started': True}):
            print >> sys.stderr, "set Pipeline Run (ID = %s) to Started" % (pipeline_id)
        else:
            print >> sys.stderr, "couldn't set Pipeline Run (ID = %s) to Started" % (pipeline_id)
            exit_status = -1
    else:
        if lims_obj.lims_pipeline_modify(pipeline_id, {'started': False}):
            print >> sys.stderr, "REVERSE: set Pipeline Run (ID = %s) to NOT Started" % (pipeline_id)
        else:
            print >> sys.stderr, "REVERSE: couldn't set Pipeline Run (ID = %s) to NOT Started" % (pipeline_id)
            exit_status = -1
sys.exit(exit_status)
