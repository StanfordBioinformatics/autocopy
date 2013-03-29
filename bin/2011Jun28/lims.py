import random
import re
import socket
import subprocess
import sys

import rundir

LIMS_HOST_PRODUCTION      = "scg-data.stanford.edu"
LIMS_RAKEFILE_PRODUCTION  = "/opt/spg/uhts-archive/current/Rakefile"
LIMS_HOST_DEVELOPMENT     = "genescene.stanford.edu"
LIMS_RAKEFILE_DEVELOPMENT = "/Users/bettingr/Projects/RubyLIMS/trunk/Rakefile"

LIMS_HOST = LIMS_HOST_DEVELOPMENT
LIMS_RAKEFILE = LIMS_RAKEFILE_DEVELOPMENT

# This host.
HOSTNAME = socket.gethostname()
HOSTNAME = HOSTNAME[0:HOSTNAME.find('.')] # Remove domain part.

TECHNICIAN_LIST = ["Addleman,Nick", "Ramirez,Lucia", "Eastman,Catharine"]

def init_rake_cmd_list(task, trace=False):
    cmd_list = ["ssh", LIMS_HOST, "rake", "-s", "-f", LIMS_RAKEFILE]
    if trace:
        cmd_list.append("--trace")
    cmd_list.append(task)
    return cmd_list
def add_to_rake_cmd_list(rake_cmd_list, var, value):
    return rake_cmd_list.append("%s=%s" % (var, value))

def add_params_from_rundir(rundir, rake_cmd_list):

    add_to_rake_cmd_list(rake_cmd_list, 'name', rundir.get_dir())
    add_to_rake_cmd_list(rake_cmd_list, 'start_date', rundir.get_start_date())
    add_to_rake_cmd_list(rake_cmd_list, 'seq_instrument', rundir.get_machine())
    add_to_rake_cmd_list(rake_cmd_list, 'technician', TECHNICIAN_LIST[random.randrange(len(TECHNICIAN_LIST))])

    # Run directory location: parse path for rundir to see if it matches an IlluminaRuns* standard place.
    standard_root_dir = False
    standard_root_prefix = re.match("/Volumes/IlluminaRuns(\d)",rundir.get_root())
    if standard_root_prefix:
        if HOSTNAME.startswith("poirot"):
            standard_root_dir = (standard_root_prefix.group(1) == "1" or
                                 standard_root_prefix.group(1) == "2" or
                                 standard_root_prefix.group(1) == "3" or
                                 standard_root_prefix.group(1) == "4" )
        elif HOSTNAME.startswith("ashton"):
            standard_root_dir = (standard_root_prefix.group(1) == "5" or
                                 standard_root_prefix.group(1) == "6" )
        elif HOSTNAME.startswith("maigret"):
            standard_root_dir = (standard_root_prefix.group(1) == "7" or
                                 standard_root_prefix.group(1) == "8" )

    if standard_root_dir:
        add_to_rake_cmd_list(rake_cmd_list, 'data_volume', "illumina_runs%s" % (standard_root_prefix.group(1)))
    else:
        add_to_rake_cmd_list(rake_cmd_list, 'local_run_dir', "%s:%s" % (HOSTNAME,rundir.get_path()))

    sw_version = rundir.get_control_software_version()
    if rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA:
        add_to_rake_cmd_list(rake_cmd_list, 'seq_kit_version', 'version5')
        add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'scs_%s' % sw_version[:3].replace('.','_'))
    elif rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ:
        add_to_rake_cmd_list(rake_cmd_list, 'seq_kit_version', 'hiseq_v3')
        if sw_version.startswith("1.3"):
            add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'hcs_%s' % sw_version.replace('.','_'))
        else:
            add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'hcs_%s' % sw_version[:3].replace('.','_'))
    else:
        print >> sys.stderr, "WARNING: platform unknown (%s)" % rundir.get_platform()

    paired_end = (rundir.get_reads() > 1)
    add_to_rake_cmd_list(rake_cmd_list, 'paired_end', paired_end)
    index_read = (rundir.get_reads() > 2)
    add_to_rake_cmd_list(rake_cmd_list, 'index_read', index_read)

    cycle_list = rundir.get_cycle_list()
    add_to_rake_cmd_list(rake_cmd_list, 'read1_cycles', cycle_list[0])
    if len(cycle_list) == 2:
        add_to_rake_cmd_list(rake_cmd_list, 'read2_cycles', cycle_list[1])
    elif len(cycle_list) == 3:
        add_to_rake_cmd_list(rake_cmd_list, 'read2_cycles', cycle_list[2])


def lims_run_create(rundir, verbose=False):

    #
    # Set up the rake command
    #
    rake_cmd_list = init_rake_cmd_list("analyze:illumina:create_run")

    add_params_from_rundir(rundir, rake_cmd_list)

    # Choose a random technician.
    add_to_rake_cmd_list(rake_cmd_list, 'technician', TECHNICIAN_LIST[random.randrange(len(TECHNICIAN_LIST))])

    #
    # Execute the rake command.
    #
    if verbose:
        print rake_cmd_list

    rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (rake_stdout, rake_stderr) = rake_popen.communicate()

    # Parse the output for error messages.
    if len(rake_stderr):
        # ERROR: "No name given"
        # ERROR: "Run with name <rundir> already exists"
        # ERROR: "No sequencing instrument named <machine>"
        # ERROR: "No technician named <first_name> <last_name>"
        # ERROR: "No technician given"
        # ERROR: "Must be paired end if index read"
        # ERROR: "Need read2_cycles for paired end run"
        # ERROR: "No local run directory given"

        # ERROR: "Run creation failed"

        print >> sys.stderr, rake_stderr
        return False
    else:
        return True

#
# This version of lims_run_modify() finds the run record matching the name of the RunDir
#  object, then modifies it to match data from the RunDir object, with optional arguments
#  for data not found in the RunDir object.
#
def lims_run_modify(rundir, technician=None, analysis_dir=None, backup_dir=None,
                    seqdone=None, anadone=None, dnadone=None, archdone=None, notifdone=None,
                    comments=None, verbose=False):

    # Set up the rake command
    rake_cmd_list = init_rake_cmd_list("analyze:illumina:modify_run")

    # Add the necessary run directory parameters to the rake command.
    add_params_from_rundir(rundir, rake_cmd_list)

    if technician:
        add_to_rake_cmd_list(rake_cmd_list, 'technician', technician)

    if analysis_dir:
        add_to_rake_cmd_list(rake_cmd_list, 'analysis_dir', analysis_dir)
    if backup_dir:
        add_to_rake_cmd_list(rake_cmd_list, 'backup_dir', backup_dir)

    if seqdone:
        add_to_rake_cmd_list(rake_cmd_list, 'sequencer_done', seqdone)
    if anadone:
        add_to_rake_cmd_list(rake_cmd_list, 'analysis_done', anadone)
    if dnadone:
        add_to_rake_cmd_list(rake_cmd_list, 'dna_nexus_done', dnadone)
    if archdone:
        add_to_rake_cmd_list(rake_cmd_list, 'archiving_done', archdone)
    if notifdone:
        add_to_rake_cmd_list(rake_cmd_list, 'notification_done', notifdone)

    if comments:
        add_to_rake_cmd_list(rake_cmd_list, 'comments', comments)

    # Execute the rake command.
    if verbose:
        print rake_cmd_list
        
    rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (rake_stdout, rake_stderr) = rake_popen.communicate()

    # Parse the output for error messages.
    if len(rake_stderr):
        # ERROR: "No name given"
        # ERROR: "Run with name <rundir> already exists"
        # ERROR: "No sequencing instrument named <machine>"
        # ERROR: "No technician named <first_name> <last_name>"
        # ERROR: "Must be paired end if index read"
        # ERROR: "Need read2_cycles for paired end run"
        # ERROR: "No local run directory given"

        # ERROR: "Run modification failed"

        print >> sys.stderr, rake_stderr
        return False
    else:
        return True

#
# This method takes in a run dir name and returns a dictionary with keys of field names
#   and values from the run record which matches the name.
#
# Possible keys for the field names in the output dictionary:
#   name
#   flowcell
#   start_date
#   seq_instrument
#   technician
#   seq_kit_version
#   paired_end
#   index_read
#   read1_cycles
#   read2_cycles
#   local_run_dir
#   analysis_dir
#   backup_dir
#   sequencer_done
#   analysis_done
#   archiving_done
#   notification_done
#
# Example: I want the flowcell, technician, and paired end of run 111213_ILLUMINA_FCHAMMER.
#
# field_dict = lims_run_get_fields("111213_ILLUMINA_FCHAMMER", field_dict)
# if field_dict:
#   print field_dict["flowcell"], field_dict["technician"], field_dict["paired_end"]
#
def lims_run_get_fields(rundir, verbose=False):

    # Dictionary of field names/values to return.
    field_dict = {}

    # Set up the rake command
    rake_cmd_list = init_rake_cmd_list("analyze:illumina:query_run")
    add_to_rake_cmd_list(rake_cmd_list, 'name', rundir.get_dir())

    # Execute the rake command.
    if verbose:
        print >> sys.stderr, rake_cmd_list
        
    rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (rake_stdout, rake_stderr) = rake_popen.communicate()

    # Parse the output for error messages.
    if len(rake_stderr):
        # ERROR: "missing run name"
        # ERROR: "no run record for <rundir>"

        print >> sys.stderr, rake_stderr
        return None
    # Parse stdout for fields to return.
    else:
        #print rake_stdout
        for line in rake_stdout.split('\n'):
            keyvalue = line.split("\t")

            #print keyvalue

            if not keyvalue[0].startswith("**"):
                if len(keyvalue) > 1:
                    field_dict[keyvalue[0]] = keyvalue[1]
                else:
                    field_dict[keyvalue[0]] = None

        return field_dict

#
# This method takes a bunch of fields from a run record and returns a list of
# run names which match the fields given.
#
def lims_run_query(field_dict, verbose=False):

    # Set up the rake command
    rake_cmd_list = init_rake_cmd_list("analyze:illumina:query_run", trace=True)
    for key in field_dict:
        add_to_rake_cmd_list(rake_cmd_list, key, field_dict[key])

    # Execute the rake command.
    if verbose:
        print >> sys.stderr, rake_cmd_list
        
    rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (rake_stdout, rake_stderr) = rake_popen.communicate()

    # Parse the output for error messages.
    if len(rake_stderr):
        # ERROR: 

        print >> sys.stderr, rake_stderr
        return None
    # Parse stdout for list of run names.
    else:
        if verbose:
            print rake_stdout

        run_name_list = []
        for line in rake_stdout.split('\n'):
            if not line.startswith("**"):
                run_name_list.append(line)

        return run_name_list[0:-1]


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

    #
    # Argument 1 is command:
    #  "createRun RUNDIR [RUNDIR*]"
    #  "modifyRun RUNDIR FIELD VALUE [FIELD VALUE]"
    #  "getFields RUNDIR FIELD FIELD FIELD"
    #  "queryRun FIELD VALUE [FIELD VALUE]"
    #
    command = args[0]
    args = args[1:]

    if command == "createRun":
        for arg in args:
            (root, dir) = os.path.split(os.path.abspath(arg))
            rundir = rundir.RunDir(root,dir)

            if lims_run_create(rundir, verbose=True):
                print >> sys.stderr, os.path.basename(__file__), ": Run creation of %s succeeded." % rundir.get_dir()
            else:
                print >> sys.stderr, os.path.basename(__file__), ": Run creation of %s FAILED." % rundir.get_dir()

    elif command == "modifyRun":
        (root, dir) = os.path.split(os.path.abspath(args[0]))
        rundir = rundir.RunDir(root,dir)

        # Make a dictionary of the remaining arguments.
        arg_dict = {"verbose" : True}
        for idx in range(2,len(args),2):
            arg_dict[args[idx-1]] = args[idx]

        if lims_run_modify(rundir, **arg_dict):
            print >> sys.stderr, os.path.basename(__file__), ": Run modification of %s succeeded." % rundir.get_dir()
        else:
            print >> sys.stderr, os.path.basename(__file__), ": Run modification of %s FAILED." % rundir.get_dir()


    elif command == "getRunFields":
        (root, dir) = os.path.split(os.path.abspath(args[0]))
        rundir = rundir.RunDir(root,dir)

        fields = lims_run_get_fields(rundir, verbose=True)

        if fields:
            #print fields
            for arg in args[1:]:
                if arg in fields:
                    print "%s: %s=%s" % (rundir.get_dir(), arg, fields[arg])
                else:
                    print "%s: No value for %s" % (rundir.get_dir(), arg)
        else:
            print >> sys.stderr, os.path.basename(__file__), ": No fields found."


    elif command == "queryRun":
        # Make a dictionary of the arguments.
        arg_dict = {}
        for idx in range(1,len(args),2):
            arg_dict[args[idx-1]] = args[idx]

        run_name_list = lims_run_query(arg_dict, verbose=True)

        print run_name_list
    else:
        print >> sys.stderr, os.path.basename(__file__), ": Command %s not understood" % command
