#!/usr/bin/env python

###############################################################################
#
# copy_rundir.py - Copy run directories to the cluster.
#
# ARGS:
#   1st: Run directory to copy.
#
# SWITCHES:
#   --host:        Destination host for the directory.
#   --user:        User to log into destination host as.
#   --group:       Group to own destination files.
#   --dest_root:   Directory to put run directory in on destination host.
#   --status_file: File to add to destination run directory to mark completion.
#   --dry_run:     Stop at the copy step.
#   --rsync:       Use rsync instead of tar over ssh.
#   --no_cif:      Don't copy intensity files.
#   --verbose:     Increase running status chatter.
#
# OUTPUT:
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
import os.path
import pwd
import subprocess
import sys

#####
#
# CONSTANTS
#
#####

DEST_HOST  = "crick.stanford.edu"
DEST_USER  = pwd.getpwuid(os.getuid()).pw_name
DEST_GROUP = "scg-admin"

DEST_RUN_ROOT = "/srv/gs1/projects/scg/Runs"

COPY_COMPLETED_FILE = "Autocopy_complete.txt"

# Error/exit codes for various problems.
ERROR_SSH_FAILED     = 3
ERROR_NO_RUN_ROOT    = 4
ERROR_ALREADY_EXISTS = 5
ERROR_COPY_FAILED    = 6
ERROR_STATUS_FAILED  = 7
ERROR_PERM_FAILED    = 8
ERROR_MULTIPLE       = 100

#####
#
# CLASSES
#
#####

#####
#
# FUNCTIONS
#
#####
def exit_script(exit_code):

    if opts.ssh_socket != SSH_CM_SOCKET:
        #
        # Close the ssh Control Master login to the destination host.
        #
        retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-O", "exit", opts.host],
                                  stdout=DEV_NULL, stderr=subprocess.STDOUT)
        if (retcode != 0):
            print >> sys.stderr, os.path.basename(__file__), ": cannot close ssh control master into", opts.host, "( retcode =", retcode, ")"
            exit_code = ERROR_SSH_FAILED 

    # Return the exit value.
    sys.exit(exit_code)

#####
#
# SCRIPT BODY
#
#####

usage = "%prog [options] run_dir1"
parser = OptionParser(usage=usage)

parser.add_option("-m", "--host", dest="host", type="string",
                  default=DEST_HOST,
                  help='Destination host for the directory [default = ' + DEST_HOST + ']')
parser.add_option("-u", "--user", dest="user", type="string",
                  default=DEST_USER,
                  help='User to log into destination host as [default = ' + DEST_USER + ']')
parser.add_option("-g", "--group", dest="group", type="string",
                  default=DEST_GROUP,
                  help='Group to own destination files [default = ' + DEST_GROUP + ']')
parser.add_option("-d", "--dest_root", dest="dest_root", type="string",
                  default=DEST_RUN_ROOT,
                  help='Directory to put run directory in on destination host [default = ' + DEST_RUN_ROOT + ']')
parser.add_option("-f", "--status_file", dest="status_file", type="string",
                  default=COPY_COMPLETED_FILE,
                  help='File to add to destination run directory to mark completion [default = ' + DEST_RUN_ROOT + '/' + COPY_COMPLETED_FILE + ']')
parser.add_option("-s", "--ssh_socket", dest="ssh_socket", type="string",
                  default=None,
                  help="SSH Control Master socket to run all ssh commands through")
parser.add_option("-n", "--dry_run", dest="dry_run", action="store_true",
                  default=False,
                  help='Do everything BUT the copy [default = false]')
parser.add_option("-r", "--rsync", dest="rsync", action="store_true",
                  default=False,
                  help='Use rsync instead of tar over ssh [default = false]')
parser.add_option("-c", "--no_cif", dest="no_cif", action="store_true",
                  default=False,
                  help='Skip copying the intensity files (.cif) [default = false]')
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default=False,
                  help='Get real chatty [default = false]')


(opts, args) = parser.parse_args()

if not len(args):
    run_path = os.getcwd()
else:
    run_path = args[0]

# The Dark Place.
DEV_NULL = open("/dev/null", "w")

# The name used for the ssh Control Master socket used for all the other ssh calls.
if opts.ssh_socket and os.path.exists(opts.ssh_socket):
    # Use ssh Control Master socket given as argument.
    SSH_CM_SOCKET = opts.ssh_socket

    #
    # Check that the ssh Control Master socket is OK.
    #
    retcode = subprocess.call(["ssh", "-O", "check", "-S", SSH_CM_SOCKET, opts.host],
                              stdout=DEV_NULL, stderr=subprocess.STDOUT)

    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": cannot use", opts.ssh_socket, "as ssh Control Master into", opts.host, "( retcode =", retcode, ")"
        exit_code = ERROR_SSH_FAILED
        exit_script(exit_code)
        
else:
    # Create a new one for just this run.
    SSH_CM_SOCKET = "/tmp/copy_rundir-%d.ssh" % os.getpid()

    if not opts.ssh_socket or not os.path.exists(opts.ssh_socket):
        print >> sys.stderr, os.path.basename(__file__), ": ssh Control Master", opts.ssh_socket, "does not exist...creating %s" % SSH_CM_SOCKET

    #
    # Create a ssh Control Master socket to the destination host.
    #
    retcode = subprocess.call(["ssh", "-o", "ConnectTimeout=10", "-l", opts.user,
                               "-S", SSH_CM_SOCKET, "-M", "-f", "-N",
                               opts.host],
                              stdout=DEV_NULL, stderr=subprocess.STDOUT)
    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": cannot create ssh Control Master into", opts.host, "( retcode =", retcode, ")"
        exit_code = ERROR_SSH_FAILED 
        exit_script(exit_code)

# If all goes well, this will be our exit value.
exit_code = 0
    
#
# Confirm that the run root directory exists on the destination.
#
retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-l", opts.user, opts.host,
                           'ls %s' % (opts.dest_root)],
                          stdout=DEV_NULL, stderr=subprocess.STDOUT)
if retcode:
    print >> sys.stderr, os.path.basename(__file__), ": Run root directory", opts.dest_root, "does not exist on", opts.host, "( retcode =", retcode, ")"
    exit_code = ERROR_NO_RUN_ROOT
    exit_script(exit_code)

# Split the run directory path into root and base, and append the base to the destination dir.
(run_path_root, run_path_base) = os.path.split(os.path.abspath(run_path))
    
dest_run_path = os.path.join(opts.dest_root, run_path_base)

#
# Initiate copy.
#
os.chdir(run_path_root)

if not opts.rsync:
    #
    # Confirm that the run directory doesn't already exist on the destination.
    #
    retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-l", opts.user, opts.host,
                               'ls %s' % (dest_run_path)],
                              stdout=DEV_NULL, stderr=subprocess.STDOUT)
    if not retcode:
        print >> sys.stderr, os.path.basename(__file__), ": Directory", run_path_base, "already exists on", opts.host, "in", opts.dest_root
        exit_code = ERROR_ALREADY_EXISTS 
        exit_script(exit_code)

    #
    # If this is a dry run, stop here.
    #
    if opts.dry_run:
        exit_script(exit_code)

    #
    # "tar -C run_path --exclude Images --exclude Thumbnail_Images -c run_path_base |
    #   ssh scg1 sg DEST_GROUP tar -C dest -x"
    #
    tar_cmd_list = ["tar", # "-C", run_path_root,
                    "--exclude", "Images", "--exclude", "Thumbnail_Images"]

    if opts.no_cif:
        tar_cmd_list.extend(["--exclude", "Data/Intensities/L00*/C*"])

    if opts.verbose:
        tar_cmd_list.append("-v")

    # Finish the tar command with the run dir to be tarred.
    tar_cmd_list.extend(["-c", run_path_base])

    tar_pipe_out = subprocess.Popen(tar_cmd_list,  stdout=subprocess.PIPE)
    retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-l", opts.user, opts.host,
                               "tar -C %s -x" % (opts.dest_root)],
                              stdin=tar_pipe_out.stdout)

    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": Error in copying", run_path, "to", opts.dest_root, "on", opts.host, "( retcode =", retcode, ")"
        exit_code = ERROR_COPY_FAILED
        exit_script(exit_code)
else:
    #
    # If this is a dry run, stop here.
    #
    if opts.dry_run:
        exit_script(exit_code)

    #
    # "rsync -rlptR --exclude=Thumbnail_Images/ run_path_base opts.host:dest_root"
    #
    rsync_cmd_list = ["rsync", "-rlptRc", "-e", "ssh -S %s -l %s" % (SSH_CM_SOCKET, opts.user),
                      "--exclude=Thumbnail_Images/", "--chmod=Dug=rwX,Do=rX,Fug=rw,Fo=r"]

    if opts.no_cif:
        rsync_cmd_list.append("--exclude=Data/Intensities/L00*/C*/")

    if opts.verbose:
        rsync_cmd_list.append("--progress")

    retcode = subprocess.call(rsync_cmd_list + [run_path_base, "%s:%s" % (opts.host, opts.dest_root)])

    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": Error in rsyncing", run_path, "to", opts.dest_root, "on", opts.host, "( retcode =", retcode, ")"
        exit_code = ERROR_COPY_FAILED
        exit_script(exit_code)

#
# Drop a status file saying we have completed our copy.
#
retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-l", opts.user, opts.host,
                           "touch %s" % (os.path.join(dest_run_path, opts.status_file))])
if retcode:
    print >> sys.stderr, os.path.basename(__file__), ": Error in creating status file in", run_path, "on", opts.host, "( retcode =", retcode, ")"
    exit_code = ERROR_STATUS_FAILED
    exit_script(exit_code)

if not opts.rsync:
    #
    # Change permissions on new run dir on remote to group writable.
    #
    retcode = subprocess.call(["ssh", "-S", SSH_CM_SOCKET, "-l", opts.user, opts.host,
                               'chmod -R g+w %s' % (dest_run_path)])
    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": Error in setting group permissions on", run_path, "( retcode =", retcode, ")."
        exit_code = ERROR_PERM_FAILED
        exit_script(exit_code)

# Exiting.  This call closes the ssh Control Master, if necessary.
exit_script(exit_code)


