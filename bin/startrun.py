#!/usr/bin/env python

from optparse import OptionParser
import sys

from scgpm_lims import Connection, RunInfo

class StartRun:

    def __init__(self, run_name, pipeline_run_id, reverse):
        self.conn=Connection()
        self.run_name=run_name
        self.reverse = reverse
        if reverse:
            status = 'inprogress'
        else:
            status = 'new'
        if pipeline_run_id is None:
            # Get pipeline_run_id from the LIMS
            (pipeline_run_id, pipeline_run) = RunInfo(self.conn, self.run_name).get_pipeline_run(status=status)
        self.pipeline_run_id = pipeline_run_id

    def update(self):
        if self.reverse:
            update = {'started': False}
        else:
            update = {'started': True}
        self.conn.updatepipelinerun(self.pipeline_run_id, paramdict=update)

if __name__=='__main__':
    usage = "%prog [options] run_name"
    description = "Updates status flags on the LIMS. Sets the analysis run status in the LIMS to 'Started' (unsetting it if -r)."
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--pipeline_id", dest="pipeline_id", 
                      default=None,
                      help='input the pipeline ID for the run directly. [default = find it from the LIMS]')
    parser.add_option("-r", "--reverse", dest="reverse", action="store_true",
                      default=False,
                      help="Uncheck the 'Started' check box. [default = False]")
    (opts, args) = parser.parse_args()

    if len(args) != 1:
        print >> sys.stderr, "need exactly one run name"
        sys.exit(-1)

    run_name = args[0]

    reverse = opts.reverse
    pipeline_run_id = opts.pipeline_id

    StartRun(run_name, pipeline_run_id=pipeline_run_id, reverse=reverse).update()
