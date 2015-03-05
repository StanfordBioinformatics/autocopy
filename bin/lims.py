#!/usr/bin/env python

# This script is here to keep APF running.
# It receives this command:
# lims.py modifyRun #{@config[:run_name]} archiving_done True
# and it needs to set the archiving_done flag in the LIMS to True.

# Once APF goes away, this can go away too.    

import os
import pwd
import random
import re
import socket
import subprocess
import sys

from scgpm_lims import Connection, RunInfo

def lims_run_modify_params(run, update):
    conn=Connection()
    run_id = RunInfo(conn, run).get_solexa_run_id()
    conn.updatesolexarun(run_id, update)

if __name__ == "__main__":
    from optparse import OptionParser
    import os.path
    import sys

    usage = "%prog [options] command run_dir [command-specific args]"
    parser = OptionParser(usage=usage)

    (opts, args) = parser.parse_args()

    if not len(args):
        print >> sys.stderr, os.path.basename(__file__), ": No run directories given"
        sys.exit(1)

    # Argument 1 is command:
    #  "modifyRun RUNDIR FIELD VALUE [FIELD VALUE]"

    command = args[0]
    args = args[1:]

    if command == "modifyRun":
        run = args[0]

        # Make a dictionary of the remaining arguments.
        arg_dict = {}
        for idx in range(2,len(args),2):
            key = args[idx-1]
            value = args[idx]
            if not key == 'archiving_done':
                raise Exception("Disabled this script to do anything but 'archiving_done'. Refusing to do '%s'." % args[idx-1])
            if value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            else:
                raise Exception("Looking for True or False, but I got this: %s" % value)
            
            lims_run_modify_params(run, {key: value})

    else:
        raise Exception("unknown command")

