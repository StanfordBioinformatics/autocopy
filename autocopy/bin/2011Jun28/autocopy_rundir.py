#!/usr/bin/env python

###############################################################################
#
# autocopy_rundir.py - Copy run directories to the cluster as they are created.
#
# ARGS:
#   all: Directories below which to monitor for run directories.
#
# SWITCHES:
#   --log_file   File to store log messages from this daemon [default = stdout].
#   --query_port Port to connect to when querying this daemon for run dir status [default = 48048].
#
# OUTPUT:
#   <LOG_FILE>: Lines of chatter about run directory statuses et al.
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
import datetime
from datetime import date
import email.mime.text
from optparse import OptionParser
import os
import os.path
import pwd
import re
import signal
import smtplib
import socket
import SocketServer
import subprocess
import sys
import time
import threading
import traceback

from rundir import RunDir
import rundir_utils

#####
#
# CONSTANTS
#
#####

# How long between short status updates to log file.
ALARM_TIME = 3600 #seconds = 1 hour

# How long between looks at the run root directory.
LOOP_DELAY_TIME = 60 #seconds

# How many copy processes should be active simultaneously.
MAX_COPY_PROCESSES = 2

# Where to copy the run directories to.
COPY_DEST_HOST  = "carmack"
COPY_DEST_USER  = pwd.getpwuid(os.getuid()).pw_name
COPY_DEST_GROUP = "scg_seq"
COPY_DEST_RUN_ROOT = "/srv/gs1/projects/scg/Runs"

COPY_COMPLETED_FILE = RunDir.STATUS_FILES[RunDir.STATUS_COPY_COMPLETE] # "Autocopy_complete.txt"

# The name of the executable which copies the run directories to their
# necessary destinations.
COPY_PROCESS_EXEC = "copy_rundir.py"
# Add the directory of this script to the path to the executable.
COPY_PROCESS_EXEC = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC)

# Whom to email to as directories are processed.
EMAIL_TO = ['bettingr@stanford.edu','lacroute@stanford.edu']
EMAIL_SUBJ_PREFIX = 'AUTOCOPY (%m): '
EMAIL_SMTP_SERVER = 'smtp.stanford.edu'

# This host.
HOSTNAME = socket.gethostname()
HOSTNAME = HOSTNAME[0:HOSTNAME.find('.')] # Remove domain part.

#####
#
# CLASSES
#
#####
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer): pass

class QueryServer:
    
    def __init__(self):
        HOST, PORT = "localhost", opts.query_port

        self.server = ThreadedTCPServer((HOST, PORT), QueryServerHandler)

        # Start a thread with the server -- that thread will then start one
        # more thread for each request.
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        # Exit the server thread when the main thread terminates.
        self.server_thread.setDaemon(True)
    
    def start(self):
        self.server_thread.start()
    
    def shutdown(self):
        self.server.shutdown()
        
        
class QueryServerHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        
        # Start by telling the client that we are "READY".
        self.wfile.write("READY")
        # Read in the query.
        query = self.rfile.readline().strip()
        log("Query server received command %s" % query)

        # Parse and answer the query.
        (cmd, arg) = query.split()

        #
        # Command "QUERY": return status of run directory sent as argument.
        #
        if cmd == "QUERY":
            # Get status of run dir named <arg>.
            for (rundir, status) in active_rundirs_w_statuses:
                if rundir.get_dir() == arg:
                    cur_status     = rundir.get_status()
                    cur_status_str = rundir.get_status_string()
                    log("Query server returned for QUERY %s status %d %s" % (arg, cur_status, cur_status_str))
                    self.wfile.write("RETURN\t%d\t%s" % (cur_status, cur_status_str))
                    return
            else:
                log("Query server returned for QUERY %s status %d %s" % (arg, -1, "No Such Dir"))
                self.wfile.write("RETURN\t%d\t%s" % (-1, "No Such Dir"))
                return
        else:
            # Unknown command -- return error
            self.wfile.write("RETURN\t-9\tERROR")

#####
#
# FUNCTIONS
#
#####

def log(*args):
    print >> LOG_FILE, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), ' '.join(args))
    LOG_FILE.flush()
    os.fsync(LOG_FILE.fileno())


def sigUSR1(signum, frame):

    copying_rundirs = get_copying_rundirs()
    log("")
    if COPY_PROCESSES == MAX_COPY_PROCESSES:
        log("COPYING DIRECTORIES:")
    else:
        log("COPYING DIRECTORIES: (New copying turned off)")
    log("-------------------")
    if len(copying_rundirs):
        for rundir in copying_rundirs:
            log(rundir.get_dir().ljust(32), rundir.get_root())
    else:
        log("None")

    ready_rundirs = get_ready_rundirs()
    log("")
    log("READY DIRECTORIES:")
    log("-----------------")
    if len(ready_rundirs):
        for rundir in ready_rundirs:
            rundir.update_status()
            log(rundir.get_dir().ljust(32), rundir.get_root(), "(%s)" % rundir.get_status_string())
    else:
        log("None")

    running_rundirs = get_running_rundirs()
    log("")
    log("RUNNING DIRECTORIES:")
    log("-------------------")
    if len(running_rundirs):
        for rundir in running_rundirs:
            rundir.update_status()
            #log(rundir.get_dir().ljust(32), rundir.get_root(), "(%s)" % rundir.get_status_string())
            log(rundir.get_dir().ljust(32), rundir.get_root(), "(Cycle %s of %s)" % (rundir.get_scored_cycle(),
                                                                                     rundir.get_total_cycles()))
    else:
        log("None")

    completed_rundirs = get_completed_rundirs()
    log("")
    log("COMPLETED DIRECTORIES:")
    log("---------------------")
    if len(completed_rundirs):
        for rundir in completed_rundirs:
            log(rundir.get_dir().ljust(32), rundir.get_root())
    else:
        log("None")

    aborted_rundirs = get_aborted_rundirs()
    log("")
    log("ABORTED DIRECTORIES:")
    log("-------------------")
    if len(aborted_rundirs):
        for rundir in aborted_rundirs:
            log(rundir.get_dir().ljust(32), rundir.get_root())
    else:
        log("None")

    failed_rundirs = get_failed_rundirs()
    log("")
    log("FAILED DIRECTORIES:")
    log("------------------")
    if len(failed_rundirs):
        for rundir in failed_rundirs:
            log(rundir.get_dir().ljust(32), rundir.get_root())
    else:
        log("None")
    log("")


def sigUSR2(signum, frame):
    global COPY_PROCESSES
    
    # Toggle whether the copying out takes place.
    if COPY_PROCESSES != MAX_COPY_PROCESSES:
        COPY_PROCESSES = MAX_COPY_PROCESSES
        log("USR2 signal received: COPYING TURNED ON")
    else:
        COPY_PROCESSES = 0
        log("USR2 signal received: NEW COPYING TURNED OFF")


def sigALRM(signum, frame):
    
    copying_rundirs = get_copying_rundirs()
    ready_rundirs = get_ready_rundirs()
    running_rundirs = get_running_rundirs()
    completed_rundirs = get_completed_rundirs()
    aborted_rundirs = get_aborted_rundirs()
    failed_rundirs = get_failed_rundirs()
 
    if COPY_PROCESSES == MAX_COPY_PROCESSES:
        log("%d Copying," % len(copying_rundirs),
            "%d Ready," % len(ready_rundirs),
            "%d Running," % len(running_rundirs),
            "%d Completed," % len(completed_rundirs),
            "%d Aborted," % len(aborted_rundirs),
            "%d Failed" % len(failed_rundirs))
    else:
        log("%d Copying," % len(copying_rundirs),
            "%d READY," % len(ready_rundirs),
            "%d Running," % len(running_rundirs),
            "%d Completed," % len(completed_rundirs),
            "%d Aborted," % len(aborted_rundirs),
            "%d Failed" % len(failed_rundirs),
            "(NEW COPYING TURNED OFF)")

    signal.alarm(ALARM_TIME)


def sig_die(signum, frame):
    
    log("Killed by signal %d" % signum)
    sys.exit(1)


def email_message(to, subj, body):

    # Add a prefix to the subject line, and substitute the host here for "%m".
    subj = EMAIL_SUBJ_PREFIX.replace("%m", HOSTNAME) + subj

    msg = email.mime.text.MIMEText(body)
    msg['Subject'] = subj
    msg['From'] = 'bettingr@stanford.edu'
    if isinstance(to,basestring):
        msg['To'] = to
    else:
        msg['To'] = ','.join(to)

    server = smtplib.SMTP(EMAIL_SMTP_SERVER)
    server.starttls()
    server.sendmail(msg['From'], to, msg.as_string())
    server.quit()

    
def get_copying_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_STARTED, active_rundirs_w_statuses))
def get_running_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter((lambda rundirstatus:
                       rundirstatus[1] <= RunDir.STATUS_BASECALLING_COMPLETE_READ3 and
                       not rundirstatus[0].is_finished() ),
                      active_rundirs_w_statuses))
def get_completed_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_COMPLETE, active_rundirs_w_statuses))
def get_failed_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_FAILED, active_rundirs_w_statuses))
def get_aborted_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_RUN_ABORTED, active_rundirs_w_statuses))
def get_ready_rundirs():
    return map(lambda rundirstatus: rundirstatus[0],
               filter(lambda rundirstatus: rundirstatus[0].is_finished(), active_rundirs_w_statuses))

def start_copy(rundir, rsync=False):

    # Construct copy command.
    copy_cmd_list = [COPY_PROCESS_EXEC,
                     "--host", COPY_DEST_HOST,
                     "--user", COPY_DEST_USER,
                     "--group", COPY_DEST_GROUP,
                     "--dest_root", COPY_DEST_RUN_ROOT,
                     "--status_file", RunDir.STATUS_FILES[RunDir.STATUS_COPY_COMPLETE],
                     "--ssh_socket", SSH_CM_SOCKET,
                     "--no_cif"
    ]

    if rsync:
        copy_cmd_list.append("--rsync")

    # End command with run directory to copy.
    copy_cmd_list.append(rundir.get_path())

    # Copy the directory.
    rundir.copy_proc = subprocess.Popen(copy_cmd_list,
                                        stdout=LOG_FILE, stderr=subprocess.STDOUT)
    rundir.copy_start_time = datetime.datetime.now()
    rundir.copy_end_time = None

    # Change the status to COPY_STARTED.
    rundir.status = RunDir.STATUS_COPY_STARTED
    rundir.drop_status_file()         # Drop "Copy started" file in directory.


def cleanup():
 
    # Shutdown the query server.
    log("Shutting down the query server.")
    query_server.shutdown()
        
    # Close the ssh Control Master socket.
    log("Closing the ssh socket.")
    retcode = subprocess.call(["ssh", "-O", "exit", "-S", SSH_CM_SOCKET, COPY_DEST_HOST],
                              stdout=LOG_FILE, stderr=subprocess.STDOUT)
    if retcode:
        print >> sys.stderr, os.path.basename(__file__), ": cannot close ssh Control Master socket into", COPY_DEST_HOST, "( retcode =", retcode, ")"


def strftdelta(timedelta):
    hours, remainder = divmod(timedelta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return '%d:%02d:%02d' % (hours, minutes, seconds)



#####
#
# SCRIPT BODY
#
#####

print "STARTING AUTOCOPY DAEMON: (pid: %d)" % os.getpid()

# Install signal handler for SIGUSR1, which dumps the state of the directories into the log file.
signal.signal(signal.SIGUSR1, sigUSR1)
# Install signal handler for SIGUSR2, which toggles whether the daemon copies or not.
signal.signal(signal.SIGUSR2, sigUSR2)
# Install signal handler for SIGALRM, which dumps the count of the directories into the log file.
signal.signal(signal.SIGALRM, sigALRM)
signal.alarm(60)  # 60 secs to start
# Install signal handler for termination signals.
signal.signal(signal.SIGTERM, sig_die)

usage = "%prog [options] run_root"
parser = OptionParser(usage=usage)

parser.add_option("-l", "--log_file", dest="log_file", type="string",
                  default=None,
                  help='the log file for the daemon [default = /tmp/autocopy_PID.log]')
parser.add_option("-p", "--query_port", dest="query_port", type="int",
                  default=48048,
                  help='the port number for querying for status [default = 48048]')
parser.add_option("-c", "--no_copy", dest="no_copy", action="store_true",
                  default=False,
                  help="don't copy run directories [default = allow copies]")

(opts, args) = parser.parse_args()

if not len(args):
    run_root_list = [os.getcwd()]
else:
    run_root_list = args

# If the run root directories don't exist, create them.
for run_root in run_root_list:
    if not os.path.exists(run_root):
        os.makedirs(run_root, 0664)

# The Dark Place.
DEV_NULL = open("/dev/null", "w")

# Open the requested log file.
if opts.log_file == "-":
    LOG_FILE = sys.stdout
elif opts.log_file:
    LOG_FILE = open(opts.log_file, "w")
else:
    LOG_FILE = open(os.path.join("/tmp","autocopy_%d.log" % os.getpid()),'w')

# STDERR and STDERR go to the LOG_FILE too.
sys.stdout = LOG_FILE
sys.stderr = LOG_FILE

#
# Initialize the log file.
#
log("-------------------------")
log("STARTING AUTOCOPY DAEMON: (pid: %d)" % os.getpid())
log("-------------------------")
log("RUN ROOT DIRS:")
for run_root in run_root_list:
    log("  %s" % run_root)
log("")

# Start the query server.
log("Spawning the query server.")
query_server = QueryServer()
query_server.start()

# Open an SSH Control Master socket for use in copying.
log("Opening the ssh socket.")
SSH_CM_SOCKET = "/tmp/autocopy_%d.ssh" % os.getpid()
retcode = subprocess.call(["ssh", "-o", "ConnectTimeout=10", "-l", COPY_DEST_USER,
                           "-S", SSH_CM_SOCKET, "-M", "-f", "-N",
                           COPY_DEST_HOST],
                          stderr=subprocess.STDOUT)
if retcode:
    print >> sys.stderr, os.path.basename(__file__), ": cannot create ssh Control Master into", COPY_DEST_HOST, "( retcode =", retcode, ")"
    sys.exit(1)

# Set local variable for number of copy processes.
if opts.no_copy:
    COPY_PROCESSES = 0
else:
    COPY_PROCESSES = MAX_COPY_PROCESSES

###
#
#  MAIN LOOP
#
###
try:
    # List of active directories being monitored.
    #  (list of [RunDir, status] lists)
    active_rundirs_w_statuses = []
    
    while True:
    
        #
        # Examine list of directories currently being copied.
        #
        copying_rundirs = get_copying_rundirs()
        for rundir in copying_rundirs:
            
            # If we have a copy process running (and we should: each RunDir here should have one),
            #  check to see if it ended happily, and change status to COPY_COMPLETE if it did.
            if rundir.copy_proc:
    
                retcode = rundir.copy_proc.poll()
                if retcode == 0:
                    # Copy succeeded: advance to Copy Complete.
                    rundir.status = RunDir.STATUS_COPY_COMPLETE
                    rundir.drop_status_file()
                    rundir.copy_proc = None
                    rundir.copy_end_time = datetime.datetime.now()
    
                    log("Copy of", rundir.get_dir(), "completed successfully [ time taken",
                        strftdelta(rundir.copy_end_time - rundir.copy_start_time), "].")

                    # Validate that the run directory has all the right files.
                    log(rundir.get_dir(), ": Validating")
                    valid_rundir = rundir_utils.validate(rundir)

                    if valid_rundir:
                        log(rundir.get_dir(), "is a valid run directory.")
                    else:
                        log(rundir.get_dir(), "is missing some files.")

                    # Calculate how large the run directory is.
                    disk_usage = rundir.get_disk_usage()

                    if disk_usage > 1024:
                        disk_usage /= 1024
                        disk_usage_units = "Tb"
                    else:
                        disk_usage_units = "Gb"

                    # Send an email announcing the completed run directory copy.
                    email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
                    email_body += "NEW LOCATION:\t\t%s:%s/%s" % (COPY_DEST_HOST, COPY_DEST_RUN_ROOT, rundir.get_dir())
                    email_body += "\n"
                    email_body += "Original Location:\t%s:%s\n" % (HOSTNAME, rundir.get_path())
                    email_body += "Read count:\t\t%d\n" % rundir.get_reads()
                    email_body += "Cycles:\t\t\t%s\n" % " ".join(map(lambda d: str(d), rundir.get_cycle_list()))
                    email_body += "\n"
                    email_body += "Copy time:\t\t%s\n" % strftdelta(rundir.copy_end_time - rundir.copy_start_time)
                    email_body += "Disk usage:\t\t%.1f %s\n" % (disk_usage, disk_usage_units)

                    email_subj_prefix = "Finished Run Dir "
                    if not valid_rundir:
                        email_body += "\n"
                        email_body += "*** RUN HAS MISSING FILES ***"

                        email_subj_prefix += "w/Missing Files "

                    email_message(EMAIL_TO, email_subj_prefix + rundir.get_dir(), email_body)
    
                elif retcode == 5:
                    # Run directory already exists, assume success.
                    rundir.status = RunDir.STATUS_COPY_COMPLETE
                    rundir.drop_status_file()
                    rundir.copy_proc = None
                    rundir.copy_end_time = datetime.datetime.now()
    
                    log("Copy of", rundir.get_dir(), "already done.")
    
                elif retcode: # is not None
                    # Copy failed, change to COPY_FAILED state.
                    rundir.copy_proc = None
                    rundir.copy_start_time = None
                    rundir.copy_end_time = None
    
                    rundir.status = RunDir.STATUS_COPY_FAILED
                    rundir.drop_status_file()
    
                    log("Copy of", rundir.get_dir(), "failed with retcode", str(retcode), ", emailing...")

                    # Send an email announcing the failed run directory copy.
                    email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
                    email_body += "Original Location:\t%s:%s\n" % (HOSTNAME, rundir.get_path())
                    email_body += "\n"
                    email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (COPY_DEST_HOST, COPY_DEST_RUN_ROOT, rundir.get_dir())
                    email_body += "Return code:\t%d\n" % retcode
                    email_message(EMAIL_TO, "ERROR COPYING Run Dir " + rundir.get_dir(), email_body)
                else:    # retcode == None
                    
                    pass # Copy process is still running...
            else:
                # If we have a RunDir in this list with no process associated, must mean that a
                # new run dir was already in a "Copy Started" state.

                # Remove COPY_STARTED file.
                rundir.undrop_status_file()  # Remove "Copy_started.txt"

                rundir.copy_proc = None
                rundir.copy_start_time = None
                rundir.copy_end_time = None
   
                log("Copy of", rundir.get_dir(), "failed with no copy process attached -- previously started?")

        #
        # Examine list of active RunDirs.
        #
        for rundir_status in active_rundirs_w_statuses:

            (rundir, old_status) = rundir_status

            # Get up-to-date status for the run dir.
            cur_status = rundir.update_status()

            # If status is "Ready to Copy":
            #if (cur_status == RunDir.STATUS_BASECALLING_COMPLETE_SINGLEREAD or
            #    cur_status == RunDir.STATUS_BASECALLING_COMPLETE_READ2):
            if rundir.is_finished():

                log("Run dir", rundir.get_dir(), "has finished processing.")

                # If there aren't too many copies already going on, ready this dir and copy it.
                if len(get_copying_rundirs()) < COPY_PROCESSES:

                    # Make thumbnails subset tar.
                    log(rundir.get_dir(), ": Making thumbnail subset tar")
                    if rundir_utils.make_thumbnail_subset_tar(rundir,overwrite=True):
                        log(rundir.get_dir(), ": Thumbnail subset tar created")
                    else:
                        log(rundir.get_dir(), ": Failed to make thumbnail subset tar")

                    # Copy the directory.
                    log("Starting copy of %s" % (rundir.get_dir()))
                    start_copy(rundir)

                    # Get up-to-date status for the run dir following the copy.
                    cur_status = rundir.update_status()

            # If the current status is the same as the previous status,
            # move along, else store the new status.
            if cur_status != old_status:

                # Update our cache of the status.
                rundir_status[1] = cur_status

                # Log new status change.
                log("Run %s changed from %s to %s" % (rundir.get_dir(),
                                                      RunDir.STATUS_STRS[old_status],
                                                      RunDir.STATUS_STRS[cur_status]))

        #
        # Scan run root list for new run dirs.
        #
        for run_root in run_root_list:
        
            # Get list of current directories.
            curr_dirs = os.listdir(run_root)
    
            # Get names of currently known run directories in this run_root directory.
            current_rundir_names = map(lambda rundirstatus: rundirstatus[0].get_dir(),
                                       filter(lambda rundirstatus: rundirstatus[0].get_root() == run_root,
                                              active_rundirs_w_statuses)) 
            
            # Look for newly created run directories to monitor.
            for entry in curr_dirs:
    
                entry_path = os.path.join(run_root, entry)
    
                if os.path.isdir(entry_path):
                    # Filter list for directories previously unseen.
                    #  Match the directory to a regexp for run names: does it start with 6 digits (start date)?
                    if ((entry not in current_rundir_names) and
                        re.match("\d{6}_",entry)):
    
                        # Make new RunDir object.
                        new_rundir = RunDir(run_root, entry)

                        # Wait until get_reads() comes back with a value.
                        if new_rundir.get_reads():
    
                            # Save RunDir object and its first status.
                            active_rundirs_w_statuses.append([new_rundir, new_rundir.get_status()])

                            # Log the new directory.
                            log("Discovered new run %s (%s) " % (entry, new_rundir.get_status_string()))

                            if new_rundir.get_status() < RunDir.STATUS_COPY_COMPLETE:

                                # Email out the discovery.
                                email_body  = "NEW RUN:\t%s\n" % entry
                                email_body += "Location:\t%s:%s/%s\n" % (HOSTNAME, run_root, entry)
                                email_body += "Read count:\t%d\n" % new_rundir.get_reads()
                                email_body += "Cycles:\t\t%s\n" % " ".join(map(lambda d: str(d), new_rundir.get_cycle_list()))

                                email_message(EMAIL_TO, "New Run Dir " + entry, email_body) 
    
            # Dirs that are in active dirs list but not the current listing
            #  are removed from the active dirs list.
            for rundir_status in reversed(active_rundirs_w_statuses):
                if ((rundir_status[0].get_root() == run_root) and (rundir_status[0].get_dir() not in curr_dirs)):
                    active_rundirs_w_statuses.remove(rundir_status)
                    log("Removing missing directory %s from active run directories." % rundir_status[0].get_dir())
            
        # Let's not poll as hard as we can -- relax...
        time.sleep(LOOP_DELAY_TIME)
    
    # END while(True)

except SystemExit, se:
    log("Exiting gracefully with code %d" % (se.code))
    email_message(EMAIL_TO, "Daemon Exited", "The autocopy daemon exited gracefully with code %d." % se.code )
    raise se

except Exception, e:
    tb = traceback.format_exc(e)
    log("Daemon crashed with Exception " + tb)
    email_message(EMAIL_TO, "Daemon Crashed", "The autocopy daemon crashed with Exception\n" + tb)
    raise e

finally:
    
    # Close the ssh socket and shutdown the query server.
    cleanup()
    
