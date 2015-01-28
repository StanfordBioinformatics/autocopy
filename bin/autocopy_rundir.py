#!/Usr/bin/env python

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
#   --no_copy    Start daemon in a no-copy mode.
#
# OUTPUT:
#   <LOG_FILE>: Lines of chatter about run directory statuses et al.
#
# ASSUMPTIONS:
#
# AUTHORS:
#   Keith Bettinger, Nathan Hammond
#
###############################################################################


"""

#####
#
# IMPORTS
#
#####
import datetime
from datetime import date
import email.mime.text
from optparse import OptionParser
import glob
import os
import os.path
import platform
import pwd
import re

import socket
import SocketServer
import subprocess
import sys
import time
import threading
import traceback

import rundir_utils

#####
#
# CONSTANTS
#
#####

# How long between infrequent tasks, like putting short status updates to log file.
TIME_ALARM = 3600 #seconds = 1 hour

# How long between looks at the run root directory.
TIME_LOOP_DELAY = 600 #seconds = 10 minutes

# The name of the executable which copies the run directories to their
# necessary destinations.
COPY_PROCESS_EXEC = "copy_rundir.py"
# Add the directory of this script to the path to the executable.
COPY_PROCESS_EXEC = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC)

# Where to archive the run directories to.
ARCH_DEST_RUN_ROOT = "/srv/gsfs0/projects/seq_center/Illumina/RawDataArchive"
# The name of the executable which archives the run directories.
ARCH_PROCESS_EXEC = "make_archive_tar.py"
# Add the directory of this script to the path to the executable.
ARCH_PROCESS_EXEC = os.path.join(os.path.dirname(__file__), ARCH_PROCESS_EXEC)

# Log directory default
tday = datetime.datetime.now()
TODAY = str(tday.year) + str(tday.month) + str(tday.day)

#####
#
# FUNCTIONS
#
#####

    
def get_running_rundirs(run_root=None):
    rundirs =  map(lambda rundirstatus: rundirstatus[0],
                   filter((lambda rundirstatus: rundirstatus[1] <= RunDir.STATUS_BASECALLING_COMPLETE_READ4 and
                                                not rundirstatus[0].is_finished() ),
                          active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)
def get_completed_rundirs(run_root=None):
    rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_COMPLETE, active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)
def get_failed_rundirs(run_root=None):
    rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_FAILED, active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)
def get_aborted_rundirs(run_root=None):
    rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_RUN_ABORTED, active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)
def get_ready_rundirs(run_root=None):
    rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[0].is_finished(), active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)
def get_archiving_rundirs(run_root=None):
    rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_ARCHIVE_STARTED, active_rundirs_w_statuses))
    if run_root is None:
        return rundirs
    else:
        return filter(lambda rundir: rundir.get_root() == run_root, rundirs)


def start_copy(rundir, rsync=True):

    # Construct copy command.
    copy_cmd_list = [COPY_PROCESS_EXEC,
                     "--host", COPY_DEST_HOST,
                     "--user", COPY_DEST_USER,
                     "--group", COPY_DEST_GROUP,
                     "--dest_root", COPY_DEST_RUN_ROOT,
                     "--status_file", RunDir.STATUS_FILES[RunDir.STATUS_COPY_COMPLETE],
                     "--ssh_socket", SSH_SOCKET_CLUSTER,
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

def start_archive(rundir):

    # Get year from rundir for use in archive destination path.
    start_date = rundir.get_start_date()
    year = "20%s" % start_date[0:2]  # Look, a Y2.1K bug!

    # Construct archive command.
    arch_cmd_list = [ARCH_PROCESS_EXEC,
                     "--ssh_socket", SSH_SOCKET_CLUSTER,
                     "--destDir", os.path.join(ARCH_DEST_RUN_ROOT, year),
                     ]
    
    # End command with run directory to archive.
    arch_cmd_list.append(rundir.get_path())

    # arch the directory.
    rundir.archive_proc = subprocess.Popen(arch_cmd_list,
                                        stdout=LOG_FILE, stderr=subprocess.STDOUT)
    rundir.archive_start_time = datetime.datetime.now()
    rundir.archive_end_time = None

    # Change the status to ARCHIVE_STARTED.
    rundir.status = RunDir.STATUS_ARCHIVE_STARTED
    rundir.drop_status_file()         # Drop "Archive started" file in directory.


def generate_run_status_line(run_root):
    copying_rundirs   = get_copying_rundirs(run_root)
    ready_rundirs     = get_ready_rundirs(run_root)
    running_rundirs   = get_running_rundirs(run_root)
    completed_rundirs = get_completed_rundirs(run_root)
    aborted_rundirs   = get_aborted_rundirs(run_root)
    failed_rundirs    = get_failed_rundirs(run_root)
    archiving_rundirs = get_archiving_rundirs(run_root)

    if COPY_PROCESSES == MAX_COPY_PROCESSES:
        ready_str = "Ready"
        newcopying_str = ""
    else:
        ready_str = "READY"
        newcopying_str = "(NEW COPYING TURNED OFF)"

    log("%s:" % run_root,
        "%d Copying," % len(copying_rundirs),
        "%d %s," % (len(ready_rundirs), ready_str),
        "%d Running," % len(running_rundirs),
        "%d Archiving," % len(archiving_rundirs),
        "%d Completed," % len(completed_rundirs),
        "%d Aborted," % len(aborted_rundirs),
        "%d Failed" % len(failed_rundirs),
        newcopying_str)

    
def generate_run_status_table():

    run_status_table = ""

    copying_rundirs = get_copying_rundirs()
    if COPY_PROCESSES == MAX_COPY_PROCESSES:
        run_status_table += "COPYING DIRECTORIES:\n"
    else:
        run_status_table += "COPYING DIRECTORIES: (New copying turned off)\n"
    run_status_table += "-------------------\n"
    if len(copying_rundirs):
        for rundir in copying_rundirs:
            run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
    else:
        run_status_table += "None\n"

    ready_rundirs = get_ready_rundirs()
    run_status_table += "\n"
    run_status_table += "READY DIRECTORIES:\n"
    run_status_table += "-----------------\n"
    if len(ready_rundirs):
        for rundir in ready_rundirs:
            rundir.update_status()
            run_status_table += "%s %s (%s)\n" % (rundir.get_dir().ljust(32), rundir.get_root(), rundir.get_status_string())
    else:
        run_status_table += "None\n"

    running_rundirs = get_running_rundirs()
    run_status_table += "\n"
    run_status_table += "RUNNING DIRECTORIES:\n"
    run_status_table += "-------------------\n"
    if len(running_rundirs):
        for rundir in running_rundirs:
            rundir.update_status()
            #run_status_table += rundir.get_dir().ljust(32) + rundir.get_root() + "(%s)" % rundir.get_status_string()
            run_status_table += "%s %s (Cycle %s of %s)\n" % (rundir.get_dir().ljust(32), rundir.get_root(), rundir.get_scored_cycle(), rundir.get_total_cycles())
    else:
        run_status_table += "None\n"

    archiving_rundirs = get_archiving_rundirs()
    run_status_table += "\n"
    run_status_table += "ARCHIVING DIRECTORIES:\n"
    run_status_table += "---------------------\n"
    if len(archiving_rundirs):
        for rundir in archiving_rundirs:
            run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
    else:
        run_status_table += "None\n"

    completed_rundirs = get_completed_rundirs()
    run_status_table += "\n"
    run_status_table += "COMPLETED DIRECTORIES:\n"
    run_status_table += "---------------------\n"
    if len(completed_rundirs):
        for rundir in completed_rundirs:
            run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
    else:
        run_status_table += "None\n"

    aborted_rundirs = get_aborted_rundirs()
    run_status_table += "\n"
    run_status_table += "ABORTED DIRECTORIES:\n"
    run_status_table += "-------------------\n"
    if len(aborted_rundirs):
        for rundir in aborted_rundirs:
            run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
    else:
        run_status_table += "None\n"

    failed_rundirs = get_failed_rundirs()
    run_status_table += "\n"
    run_status_table += "FAILED DIRECTORIES:\n"
    run_status_table += "------------------\n"
    if len(failed_rundirs):
        for rundir in failed_rundirs:
            run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
    else:
        run_status_table += "None\n"

    return run_status_table


def strftdelta(timedelta):
    hours, remainder = divmod(timedelta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return '%d:%02d:%02d' % (hours, minutes, seconds)


def check_run_roots_freespace():

    ####
    #
    # Scan run root list to see if their partitions have enough free space.
    #
    ####
    log("Directory\t\t\tFree Space")
    log("---------\t\t\t----------")
    run_root_stats = dict()
    too_full_dict = dict()
    for run_root in run_root_list:

        stats = os.statvfs(run_root)
        run_root_stats[run_root] = stats

        # What is the free space in this run root directory, in bytes?
        free_space = stats.f_bfree * stats.f_frsize

        alert_str = ""
        if free_space < MIN_FREE_SPACE:
            too_full_dict[run_root] = stats
            alert_str = "!!"

        log("%s\t%0.1f Gb\t%s" % (run_root, free_space/ONEGIG, alert_str))

    # If any directory's free space is too small, send out an email of warning.
    if len(too_full_dict.keys()) > 0:
        email_body = "The following run root directories have insufficient free space:\n\n"

        email_body += "Directory\t\t\tFree Space\n"
        email_body += "---------\t\t\t----------\n"
        for (run_root, stats) in too_full_dict.iteritems():
            email_body += "%s\t%0.1f GiB\n" % (run_root, stats.f_bfree * stats.f_frsize / ONEGIG)

        email_message(EMAIL_TO, "Insufficient Free Space in Run Root Directories", email_body)

        return too_full_dict
    else:
        return None


def phase3_examine_archiving_dirs():

    ####
    #
    # PHASE 2.5: Examine list of directories currently being archived.
    #
    ####
    archiving_rundirs = get_archiving_rundirs()
    for rundir in archiving_rundirs:

        # If we have a archive process running (and we should: each RunDir here should have one),
        #  check to see if it ended happily, and change status to ARCHIVE_COMPLETE if it did.
        if rundir.archive_proc:
            LOG_FILE.flush()
            retcode = rundir.archive_proc.poll()
            if retcode == 0:
                # Archive succeeded: advance to Archive Complete.
                rundir.status = RunDir.STATUS_ARCHIVE_COMPLETE
                rundir.drop_status_file()
                rundir.archive_proc = None
                rundir.archive_end_time = datetime.datetime.now()

                log("Archive of", rundir.get_dir(), "completed successfully [ time taken",
                    strftdelta(rundir.archive_end_time - rundir.archive_start_time), "].")

                # Send an email announcing the completed run directory archive.
                email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
                email_body += "Archive location:\t%s/YEAR/%s\n" % (ARCH_DEST_RUN_ROOT, rundir.get_root() + "*")
                email_body += "\n"
                email_body += "Archive time:\t\t%s\n" % strftdelta(rundir.archive_end_time - rundir.archive_start_time)

                email_subj_prefix = "Archived Run Dir "

                email_message(EMAIL_TO, email_subj_prefix + rundir.get_dir(), email_body)

                if False:
                    #
                    # LIMS: change Archiving Done flag to "True".
                    #
                    # WHAT IF NO RUN RECORD BY THIS POINT???
                    #
                    arch_done_dict = {'archiving_done': 'yes'}
                    if lims_obj.lims_run_modify_params(rundir.get_dir(), arch_done_dict):
                        log("LIMS: Set Archiving Done flag of %s to True" % rundir.get_dir())
                    else:
                        log("LIMS: COULD NOT Set Archiving Done flag of %s to True" % rundir.get_dir())

            elif retcode: # is not None
                # Archive failed, change to ARCHIVE_FAILED state.
                rundir.archive_proc = None
                rundir.archive_start_time = None
                rundir.archive_end_time = None

                rundir.status = RunDir.STATUS_ARCHIVE_FAILED
                rundir.drop_status_file()

                log("Archive of", rundir.get_dir(), "failed with retcode", str(retcode), ", emailing...")

                # Send an email announcing the failed run directory archive.
                email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
                email_body += "Original Location:\t%s:%s\n" % (HOSTNAME, rundir.get_path())
                email_body += "\n"
                email_body += "FAILED TO ARCHIVE to:\t%s/YEAR/%s\n" % (ARCH_DEST_RUN_ROOT, rundir.get_dir())
                email_body += "Return code:\t%d\n" % retcode

                email_message(EMAIL_TO, "ERROR ARCHIVING Run Dir " + rundir.get_dir(), email_body)

            else:    # retcode == None
                # Archive process is still running...
                pass
        else:
            # If we have a RunDir in this list with no process associated, must mean that a
            # new run dir was already in a "Archive Started" state.

            # Remove ARCHIVE_STARTED file.
            rundir.undrop_status_file()  # Remove "Archive_started.txt"

            rundir.archive_proc = None
            rundir.archive_start_time = None
            rundir.archive_end_time = None

            log("Archive of", rundir.get_dir(), "failed with no archive process attached -- previously started?")


def phase4_update_statuses():

    ####
    #
    # PHASE 3: Examine list of active RunDirs to check running status and LIMS status.
    #
    ####
    for rundir_status in reversed(active_rundirs_w_statuses):

        (rundir, old_status, lims_status, lims_fields) = rundir_status

        # Get up-to-date status for the run dir.
        cur_status = rundir.update_status()

        # If status is "Ready to Copy":
        if rundir.is_finished():

            log("Run", rundir.get_dir(), "has finished processing and is ready to copy.")

            # If there aren't too many copies already going on, ready this dir and copy it.
            if len(get_copying_rundirs()) < COPY_PROCESSES:

                # Make thumbnails subset tar.
                log(rundir.get_dir(), ": Making thumbnail subset tar")
                if rundir_utils.make_thumbnail_subset_tar(rundir,overwrite=True):
                    log(rundir.get_dir(), ": Thumbnail subset tar created")
                else:
                    log(rundir.get_dir(), ": Failed to make thumbnail subset tar")

                # Copy the directory.
                log("Starting copy of run %s" % (rundir.get_dir()))
                start_copy(rundir)

                # Get up-to-date status for the run dir following the copy initiation.
                cur_status = rundir.update_status()

        # else if status is "Aborted":
        elif cur_status == RunDir.STATUS_RUN_ABORTED:

            # Move the run directory to Aborted subdirectory.
            log("Moving aborted dir %s to %s subdirectory" % (rundir.get_dir(), SUBDIR_ABORTED))
            os.renames(rundir.get_path(),os.path.join(rundir.get_root(),SUBDIR_ABORTED,rundir.get_dir()))

            # No need to continue to LIMS status updates if the run was aborted.
            continue

        # else if status is "Copy Complete":
        elif cur_status == RunDir.STATUS_COPY_COMPLETE:

            # Start the archiving process for the run.
            log("Starting archiving to cluster of run %s" % rundir.get_dir())
            start_archive(rundir)

            # Get up-to-date status for the run dir following the archive initiation.
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

        #####
        # PHASE 3.5: Update LIMS status of all active rundirs.
        #####

        lims_fields = lims_obj.lims_run_get_fields(rundir)

        if lims_fields:
            check_lims_msg = lims_obj.lims_run_check_rundir(rundir, lims_fields, check_local_run_dir=True)
            hcs = False
            if check_lims_msg: 
              if check_lims_msg.startswith("RunDir software hcs_2_0"):
                  hcs = True
              log("Lims message regarding rundir " + rundir.get_dir() + " returned from lims_obj.lims_run_check_rundir() in autocopy_rundir.py: " + str(check_lims_msg))
            if hcs or not check_lims_msg:
                if lims_status != STATUS_LIMS_OK:
                    log("Found LIMS run record for %s" % rundir.get_dir())
                    lims_status = STATUS_LIMS_OK

                # If this RunDir has already been copied, check if we need to set the Sequencer Done flag.
                if rundir.is_copied():

                    if lims_fields['sequencer_done'] != 'yes':
                        # LIMS: change Sequencer Done flag to "True".
                        seq_done_dict = {'sequencer_done': 'yes'}
                        if lims_obj.lims_run_modify_params(rundir.get_dir(), seq_done_dict):
                            log("LIMS: Set Sequencer Done flag of %s to True" % rundir.get_dir())
                        else:
                            log("LIMS: COULD NOT Set Sequencer Done flag of %s to True" % rundir.get_dir())

                        # LIMS: change Flowcell Status to "Analyzing".
                        if lims_obj.lims_flowcell_modify_status(name=rundir.get_flowcell(),status='analyzing'):
                            log("LIMS: Set Flowcell Status of %s (%s) to 'analyzing'" % (rundir.get_dir(), rundir.get_flowcell()))
                        else:
                            log("LIMS: COULD NOT Set Flowcell Status of %s (%s) to 'analyzing'" % (rundir.get_dir(), rundir.get_flowcell()))

            else:
                lims_status = STATUS_LIMS_MISMATCH
                log("LIMS run record for %s has a mismatch:" % rundir.get_dir())
        else:
            lims_status = STATUS_LIMS_MISSING
            log("No LIMS run record for %s" % rundir.get_dir())


        # Update the active rundir status with new LIMS information.
        rundir_status[2] = lims_status
        rundir_status[3] = lims_fields

        if lims_status != STATUS_LIMS_MISSING:
            # Look to see if Sequencer Failed status.
            if lims_fields['seq_run_status'] == 'sequencing_failed':

                #   Mark run as aborted.
                log("LIMS: Run %s set to Sequencing Failed: marking as aborted..." % rundir.get_dir())
                rundir.status = RunDir.STATUS_RUN_ABORTED
                rundir.drop_status_file()

                # Set all the status flags in the LIMS for this run.
                all_flags_yes_dict = {'sequencer_done': 'yes', 'analysis_done': 'yes', 'dnanexus_done': 'yes',
                                      'notification_done': 'yes', 'archiving_done': 'yes'}
                if lims_obj.lims_run_modify_params(rundir.get_dir(), all_flags_yes_dict):
                     log("LIMS: Set all flags of %s to True" % rundir.get_dir())
                else:
                     log("LIMS: COULD NOT Set all flags of %s to True" % rundir.get_dir())

                #
                # LIMS: change Flowcell Status to "Done".
                #
                if lims_obj.lims_flowcell_modify_status(name=rundir.get_flowcell(),status='done'):
                    log("LIMS: Set Flowcell Status of %s (%s) to 'done'" % (rundir.get_dir(), rundir.get_flowcell()))
                else:
                    log("LIMS: COULD NOT Set Flowcell Status of %s (%s) to 'done'" % (rundir.get_dir(), rundir.get_flowcell()))


            # Look if Archiving Done checked.
            elif lims_fields['archiving_done'] == 'yes':

                #   Remove from active runs.
                log("%s has been archived: it can be deleted." % rundir.get_dir())
                active_rundirs_w_statuses.remove(rundir_status)

                #   Delete run directory.
                #log("Deleting %s from active runs. " % rundir.get_dir())
                # PUT DELETE DIR CODE HERE.
                log("Moving archived run %s to subdirectory %s" % (rundir.get_dir(), SUBDIR_ARCHIVE))
                os.renames(rundir.get_path(),os.path.join(rundir.get_root(),SUBDIR_ARCHIVE,rundir.get_dir()))


def phase5_query_LIMS_for_missing_runs():
    
    pass


#####
#
# SCRIPT BODY
#
#####

# The Dark Place.
DEV_NULL = open(os.devnull, "w")

"""

import os
import datetime
from optparse import OptionParser
import pwd
import re
import signal
import smtplib
import socket
import sys

from rundir import RunDir
from scgpm_lims.connection import Connection

class AutocopyRundir:

    LOG_DIR_DEFAULT = "/usr/local/log"

    # Subdirectories to be created/used within each run root.
    #   Archiving subdirectory, where run dirs are moved after they are reported as Archived in LIMS.
    #   Aborted subdirectory, where run dirs are moved when they are reported as aborted.
    SUBDIR_ARCHIVE = "Archived"
    SUBDIR_ABORTED = "Aborted"

    LIMS_API_VERSION = 'v1'

    # How many copy processes should be active simultaneously.
    MAX_COPY_PROCESSES = 2

    EMAIL_TO = 'nhammond@stanford.edu'  #'scg-auto-notify@lists.stanford.edu'
    EMAIL_FROM = 'nathankw@stanford.edu'
    EMAIL_SUBJ_PREFIX = 'AUTOCOPY (%m): '

    # LIMS Statuses for RunDirs.
    STATUS_LIMS_OK = 0
    STATUS_LIMS_MISSING  = 1
    STATUS_LIMS_MISMATCH = 2

    # Where to copy the run directories to.
    COPY_DEST_HOST  = "crick.stanford.edu"
    COPY_DEST_USER  = pwd.getpwuid(os.getuid()).pw_name
    COPY_DEST_GROUP = "scg-admin"
    COPY_DEST_RUN_ROOT = "/srv/gsfs0/projects/seq_center/Illumina/RunsInProgress"
    COPY_COMPLETED_FILE = RunDir.STATUS_FILES[RunDir.STATUS_COPY_COMPLETE] # "Autocopy_complete.txt"

    # Powers of two constants
    ONEKILO = 1024.0
    ONEMEG  = ONEKILO * ONEKILO
    ONEGIG  = ONEKILO * ONEMEG
    ONETERA = ONEKILO * ONEGIG

    # What is the minimum amount of free space desired in a run root directory's partition?
    MIN_FREE_SPACE = ONETERA * 2


    def __init__(self, run_root_list = None, log_file=None, no_copy=False, redirect_stdout_stderr_to_log=True, no_mail=False):
        # These options are for testing and not available from the commandline:
        #   redirect_stdout_stderr_to_log, no_mail
        
        if not run_root_list:
            run_root_list = [os.getcwd()]
        elif not len(run_root_list):
            run_root_list = [os.getcwd()]

        self.RUN_ROOT_LIST = run_root_list

        # Set local variable for number of copy processes.
        if no_copy:
            self.COPY_PROCESSES = 0
        else:
            self.COPY_PROCESSES = self.MAX_COPY_PROCESSES

        # List of active directories being monitored.
        #  (list of [RunDir, copy status, LIMS status, LIMS fields] lists)
        self.active_rundirs_w_statuses = []

        self.emailed_start_msg = False

        # Connect to the LIMS
        self.initialize_lims_connection()

        # Connect to mail server
        self.NO_MAIL = no_mail
        if not self.NO_MAIL:
            self.initialize_mail_server()

        # Open the requested log file.
        if log_file == "-":
            self.LOG_FILE = sys.stdout
        elif log_file:
            self.LOG_FILE = open(log_file, "w")
        else:
            self.LOG_FILE = open(os.path.join(self.LOG_DIR_DEFAULT,
                                              "autocopy_%s.log" % datetime.datetime.today().strftime("%y%m%d")),'a')

        if redirect_stdout_stderr_to_log:
            # Direct stdout and stderr to log file.
            # Any interactive startup functions should be finished before you do this.
            print "Logging all output to %s" % self.LOG_FILE.name
            sys.stdout = self.LOG_FILE
            sys.stderr = self.LOG_FILE

        self.prepare_run_roots()

        # This host.
        hostname = socket.gethostname()
        self.HOSTNAME = hostname[0:hostname.find('.')] # Remove domain part.

    def initialize_lims_connection(self):
        self.CONN = Connection(apiversion=self.LIMS_API_VERSION)

    def initialize_mail_server(self):
        # Try to get from env, then prompt user for input
        smtp_server = os.getenv('AUTOCOPY_SMTP_SERVER')
        smtp_port = os.getenv('AUTOCOPY_SMTP_PORT')
        smtp_username = os.getenv('AUTOCOPY_SMTP_USERNAME')
        smtp_token = os.getenv('AUTOCOPY_SMTP_TOKEN')

        if not (smtp_server and smtp_port and smtp_username and smtp_token):
            # get from user
            print ("SMTP server settings were not set by env variables or commandline input")
            print ("You can enter them manually now.")
            if smtp_server is None:
                smtp_server = raw_input("SMTP server URL: ")
            if smtp_port is None:
                smtp_port = raw_input("SMTP port: ")
            if smtp_username is None:
                smtp_username = raw_input("SMTP username: ")
            if smtp_token is None:
                smtp_token = raw_input("SMTP token: ")

        if not (smtp_server and smtp_port and smtp_username and smtp_token):
            raise Exception('SMTP server settings are required')

        sys.stdout.write("Connecting to mail server...")
        self.smtp = smtplib.SMTP(smtp_server, smtp_port, timeout=5)
        self.smtp.login(smtp_username, smtp_token)
        print "success."

    def run(self):
        self.initialize_log_file()
        try:
            while True:
                self.main_loop()
                time.sleep(TIME_LOOP_DELAY) # Let's not poll as hard as we can -- relax...

        except SystemExit, se:
            # TODO self.generate_run_status_table()
            log("Exiting gracefully with code %d" % (se.code))

            exit_msg_body = ("The autocopy daemon exited gracefully with code %d.\n\n" % se.code) + generate_run_status_table()
            self.email_message(EMAIL_TO, "Daemon Exited", exit_msg_body)
            raise se

        except Exception, e:
            tb = traceback.format_exc(e)
            log("Daemon crashed with Exception " + tb)

            exit_msg_body = ("The autocopy daemon crashed with Exception\n" + tb + "\n\n" + generate_run_status_table())
            self.email_message(EMAIL_TO, "Daemon Crashed", "The autocopy daemon crashed with Exception\n" + tb)
            raise e


    def main_loop(self):
        # PHASE 1: Scan run root list for new run directories.
        # PHASE 2: Examine list of directories currently being copied.
        # PHASE 3: Examine list of directories currently being archived.
        # PHASE 4: Examine list of active RunDirs to check running status and LIMS status.
        # PHASE 5: Query LIMS for recent run records and look for any without active RunDirs.

        self.scan_run_roots() 
        self.examine_copying_dirs() 
        self.phase3_examine_archiving_dirs() 
        self.phase4_update_statuses()
        self.phase5_query_LIMS_for_missing_runs()

        # Email a start-daemon message, if we haven't yet.
        if not self.emailed_start_msg:
            start_msg_body = "The Autocopy Daemon was started.\n\n" + generate_run_status_table()
            self.email_message(EMAIL_TO, "Daemon Started", start_msg_body)
            self.log("Sent startup email message.")
            self.emailed_start_msg = True

    def scan_run_roots(self):
        for run_root in self.RUN_ROOT_LIST:
            # Get list of current directories in the run root directory.
            current_dirs_found = os.listdir(run_root)

            # Get names of currently known run directories in this run_root directory.
            known_rundirs = self.get_known_rundirs(run_root)

            self.find_and_add_unknown_rundirs(run_root, current_dirs_found, known_rundirs)
            self.remove_known_rundirs_that_do_not_exist(run_root, current_dirs_found, known_rundirs)

    def examine_copying_dirs(self):

        copying_rundirs = self.get_copying_rundirs()

        for rundir in copying_rundirs:
            # If we have a copy process running (and we should: each RunDir here should have one),
            #  check to see if it ended happily, and change status to COPY_COMPLETE if it did.
            if rundir.copy_proc:
                retcode = rundir.copy_proc.poll()
                if retcode == 0:
                    is_rundir_valid = self.is_rundir_valid(rundir)
                    self.update_status_copy_complete(rundir)
                    self.send_email_rundir_copy_complete_complete(rundir, is_rundir_valid)

                    #TODO
                    #if is_rundir_valid:
                        # Check for active run record for this run directory.
                    #    run_fields = lims_obj.lims_run_get_fields(rundir)
                    #    if run_fields and run_fields["sequencer_done"] != "yes":
                            #
                            # LIMS: change Sequencer Done flag to "True".
                            #
                    #        seq_done_dict = {'sequencer_done': 'yes'}
                    #        if lims_obj.lims_run_modify_params(rundir.get_dir(), seq_done_dict):
                    #            log("LIMS: Set Sequencer Done flag of %s to True" % rundir.get_dir())
                    #        else:
                    #            log("LIMS: COULD NOT Set Sequencer Done flag of %s to True" % rundir.get_dir())
                                
                            #
                            # LIMS: change Flowcell Status to "Analyzing".
                            #
                    #        if lims_obj.lims_flowcell_modify_status(name=rundir.get_flowcell(),status='analyzing'):
                    #            log("Set Flowcell Status of %s (%s) to 'analyzing'" % (rundir.get_dir(), rundir.get_flowcell()))
                    #        else:
                    #            log("COULD NOT Set Flowcell Status of %s (%s) to 'analyzing'" % (rundir.get_dir(), rundir.get_flowcell()))

                elif retcode == 5:
                    #TODO
                    pass
                    # Run directory already exists at destination.

                    # If LIMS says "Sequencer Done", assume success.
#                run_fields = lims_obj.lims_run_get_fields(rundir)
#                if run_fields and run_fields["sequencer_done"] == "yes":
#                    rundir.status = RunDir.STATUS_COPY_COMPLETE
#                    rundir.drop_status_file()
#                    rundir.copy_proc = None
#                    rundir.copy_end_time = datetime.datetime.now()

#                    log("Copy of", rundir.get_dir(), "already done.")
#                else:
                    # Else start_copy with rsync.
#                    start_copy(rundir, rsync=True)
#                    log("Restarting copy of", rundir.get_dir(), "with rsync.")

                elif retcode: # is not None
                    # Copy failed, change to COPY_FAILED state.
                    rundir.copy_proc = None
                    rundir.copy_start_time = None
                    rundir.copy_end_time = None

                    rundir.status = RunDir.STATUS_COPY_FAILED
                    rundir.drop_status_file()

                    self.log("Copy of", rundir.get_dir(), "failed with retcode", str(retcode), ", emailing...")

                    self.send_email_copy_failed(rundir, retcode)

                else:    # retcode == None
                    # Copy process is still running...
                    pass
            else:
                # If we have a RunDir in this list with no process associated, must mean that a
                # new run dir was already in a "Copy Started" state.

                # Remove COPY_STARTED file.
                rundir.undrop_status_file()  # Remove "Copy_started.txt"

                rundir.copy_proc = None
                rundir.copy_start_time = None
                rundir.copy_end_time = None

                self.log("Copy of", rundir.get_dir(), "failed with no copy process attached -- previously started?")

    def update_status_copy_complete(self, rundir):
        # Copy succeeded: advance to Copy Complete.
        rundir.status = RunDir.STATUS_COPY_COMPLETE
        rundir.drop_status_file()
        rundir.copy_proc = None
        rundir.copy_end_time = datetime.datetime.now()

        self.log("Copy of", rundir.get_dir(), "completed successfully [ time taken",
                 strftdelta(rundir.copy_end_time - rundir.copy_start_time), "].")

    def is_rundir_valid(self, rundir):
        # Validate that the run directory has all the right files.
        self.log(rundir.get_dir(), ": Validating")
        is_rundir_valid = rundir_utils.validate(rundir)

        if is_rundir_valid:
            log(rundir.get_dir(), "is a valid run directory.")
        else:
            log(rundir.get_dir(), "is missing some files.")
        return is_rundir_valid

    def send_email_rundir_copy_complete(self, rundir, is_rundir_valid):
        # Calculate how large the run directory is.
        disk_usage = rundir.get_disk_usage()
        if disk_usage > self.ONEKILO:
            disk_usage /= self.ONEKILO
            disk_usage_units = "Tb"
        else:
            disk_usage_units = "Gb"

        # Send an email announcing the completed run directory copy.
        email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "NEW LOCATION:\t\t%s:%s/%s" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "\n"
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "Read count:\t\t%d\n" % rundir.get_reads()
        email_body += "Cycles:\t\t\t%s\n" % " ".join(map(lambda d: str(d), rundir.get_cycle_list()))
        email_body += "\n"
        email_body += "Copy time:\t\t%s\n" % strftdelta(rundir.copy_end_time - rundir.copy_start_time)
        email_body += "Disk usage:\t\t%.1f %s\n" % (disk_usage, disk_usage_units)

        email_subj_prefix = "Finished Run Dir "
        if not is_rundir_valid:
            email_body += "\n"
            email_body += "*** RUN HAS MISSING FILES ***"
            email_subj_prefix += "w/Missing Files "
            self.email_message(EMAIL_TO, email_subj_prefix + rundir.get_dir(), email_body)

    def remove_known_rundirs_that_do_not_exist(self, run_root, current_dirs_found, known_rundirs):
        # Dirs that are in active dirs list but not the current directory listing
        #  are removed from the active dirs list.
        for rundir_status in reversed(self.active_rundirs_w_statuses):
            if ((rundir_status[0].get_root() == run_root) and (rundir_status[0].get_dir() not in current_dirs_found)):
                self.active_rundirs_w_statuses.remove(rundir_status)
                self.log("Removing missing directory %s from active run directories." % rundir_status[0].get_dir())

    def get_known_rundirs(self, run_root):
        return map(lambda rundirstatus: rundirstatus[0].get_dir(),
                   filter(lambda rundirstatus: rundirstatus[0].get_root() == run_root,
                          self.active_rundirs_w_statuses))

    def find_and_add_unknown_rundirs(self, run_root, current_dirs_found, known_rundirs):
        # Look for newly created run directories to monitor.
        for entry in current_dirs_found:
            # HACK: Ignore special subdirectories, where completed directories are moved.
            if entry == self.SUBDIR_ARCHIVE: return
            if entry == self.SUBDIR_ABORTED: return
            
            entry_path = os.path.join(run_root, entry)

            # Filter list for directories previously unseen.
            #  Match the directory to a regexp for run names: does it start with 6 digits (start date)?
            if (os.path.isdir(entry_path) and
                entry not in known_rundirs and
                re.match("\d{6}_",entry)):

                self.add_to_active_rundirs(entry, run_root)

    def add_to_active_rundirs(self, entry, run_root):
        # Make new RunDir object.
        new_rundir = RunDir(run_root, entry)

        # Confirm that it is a valid run directory.
        if not new_rundir.is_valid():
            self.log("Ignoring invalid run dir %s" % entry)
            return

                #TODO
                # Look up this run in the LIMS.
#                log("Getting LIMS run record for %s" % entry)
#                lims_run_fields = lims_obj.lims_run_get_fields(new_rundir)

                # Check LIMS fields against new RunDir.
#                if not lims_run_fields:
#                    log("No LIMS run record for %s" % entry)
#                    lims_status = STATUS_LIMS_MISSING
#                else:
                    # Compare LIMS run record field against RunDir information.
#                    check_lims_msg = lims_obj.lims_run_check_rundir(new_rundir, lims_run_fields, check_local_run_dir=True)
#                    hcs = False
#                    if check_lims_msg:
#                        if check_lims_msg.startswith("RunDir software hcs_2_0"): 
#                            hcs = True
#                    if hcs or not check_lims_msg:
#                        lims_status = STATUS_LIMS_OK
#                    else:
#                        log("LIMS run record for %s doesn't match Run Dir:" % entry)
#                        for msg in check_lims_msg.split("\n"):
#                            log(msg)
#                        lims_status = STATUS_LIMS_MISMATCH
        lims_status = self.STATUS_LIMS_OK
        lims_run_fields = {}

        # Save new RunDir object and its first statuses.
        self.active_rundirs_w_statuses.append([new_rundir, new_rundir.get_status(), lims_status, lims_run_fields])

        # If this run has not been copied yet...
        if new_rundir.get_status() < RunDir.STATUS_COPY_COMPLETE:

            # Log the new directory.
            self.log("Discovered new run %s (%s) " % (entry, new_rundir.get_status_string()))

            # Email out the discovery.
            self.send_new_rundir_email(new_rundir, run_root)

    def send_new_rundir_email(self, new_rundir, run_root):
        email_body  = "NEW RUN:\t%s\n" % new_rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n" % (self.HOSTNAME, run_root, new_rundir.get_dir())
        if new_rundir.get_reads():
            email_body += "Read count:\t%d\n" % new_rundir.get_reads()
            email_body += "Cycles:\t\t%s\n" % " ".join(map(lambda d: str(d), new_rundir.get_cycle_list()))

            if lims_status == self.STATUS_LIMS_MISMATCH:
                email_body += "\n"
                email_body += "NOTE: Run dir fields don't match LIMS record.\n"
                email_body += "\n"
                email_body += check_lims_msg

                email_subj  = "New Run Dir (w/LIMS Mismatch) " + entry
            else:
                email_subj  = "New Run Dir " + entry

                self.email_message(EMAIL_TO, email_subj, email_body)
        else:
            pass
            # TODO: Otherwise, confirm that it's been flagged in the LIMS as finished.

    def get_copying_rundirs(self, run_root=None):
        rundirs = map(lambda rundirstatus: rundirstatus[0],
                  filter(lambda rundirstatus: rundirstatus[1] == RunDir.STATUS_COPY_STARTED, self.active_rundirs_w_statuses))
        if run_root is None:
            return rundirs
        else:
            return filter(lambda rundir: rundir.get_root() == run_root, rundirs)

    def send_email_copy_failed(self, rundir, retcode):
        # Send an email announcing the failed run directory copy.
        email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "\n"
        email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "Return code:\t%d\n" % retcode

        self.email_message(EMAIL_TO, "ERROR COPYING Run Dir " + rundir.get_dir(), email_body)


    def phase3_examine_archiving_dirs(self):
        pass

    def phase4_update_statuses(self):
        pass
        
    def phase5_query_LIMS_for_missing_runs(self):
        pass

    def prepare_run_roots(self):
        for run_root in self.RUN_ROOT_LIST:
            # If the run root directories don't exist, create them.
            if not os.path.exists(run_root):
                os.makedirs(run_root, 0775)

        # Create the sorting subdirectories, if necessary.
        aborted_subdir = os.path.join(run_root, self.SUBDIR_ABORTED)
        if not os.path.exists(aborted_subdir):
            os.mkdir(aborted_subdir, 0775)
        archive_subdir = os.path.join(run_root, self.SUBDIR_ARCHIVE)
        if not os.path.exists(archive_subdir):
            os.mkdir(archive_subdir, 0775)

    def initialize_log_file(self):
        log("-------------------------")
        log("STARTING AUTOCOPY DAEMON: (pid: %d)" % os.getpid())
        log("-------------------------")
        log("RUN ROOT DIRS:")
        for run_root in run_root_list:
            log("  %s" % run_root)
        log("")

    def log(self, *args):
        # Join args by spaces.
        log_text = ' '.join(args)
        # Split args into lines.
        log_lines = log_text.split("\n")
        # Print each line with a time stamp.
        for line in log_lines:
            print >> self.LOG_FILE, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), line)
        # Flush out the log file so the new text shows up immediately.
        self.LOG_FILE.flush()


    def send_email(self, to, subj, body):
        if self.no_mail:
            return
        # Add a prefix to the subject line, and substitute the host here for "%m".
        subj = self.EMAIL_SUBJ_PREFIX.replace("%m", self.HOSTNAME) + subj
        msg = email.mime.text.MIMEText(body)
        msg['Subject'] = subj
        msg['From'] = self.EMAIL_FROM
        if isinstance(to,basestring):
            msg['To'] = to
        else:
            msg['To'] = ','.join(to)
        self.smtp.sendmail(msg['From'], to, msg.as_string())



        """
    @classmethod
    def initialize_signals(cls):
        def sigUSR1(signum, frame):
            run_status_table = generate_run_status_table()
            log("")
            log(run_status_table)
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

            #
            # Print short status message in the log.
            #
            for run_root in run_root_list:
                generate_run_status_line(run_root)

            #
            # Check run root directories free space.
            #
            log("Checking run root freespace...")
            too_full_list = check_run_roots_freespace()
            if too_full_list:
                #        log("Insufficient free space found in the following run root dirs:")
                #        for run_root in too_full_list.iterkeys():
                #            log("\t%s" % run_root)
                log("Insufficient free space found -- warning email sent.")
            else:
                log("All run roots have sufficient free space.")

            signal.alarm(TIME_ALARM)


        def sig_die(signum, frame):
    
            log("Killed by signal %d" % signum)
            sys.exit(0)

            # Install signal handler for SIGUSR1, which dumps the state of the directories into the log file.
            signal.signal(signal.SIGUSR1, sigUSR1)
            # Install signal handler for SIGUSR2, which toggles whether the daemon copies or not.
            signal.signal(signal.SIGUSR2, sigUSR2)
            # Install signal handler for SIGALRM, which dumps the count of the directories into the log file.
            signal.signal(signal.SIGALRM, sigALRM)
            signal.alarm(300)  # 5 minutes to start
            # Install signal handler for termination signals.
            signal.signal(signal.SIGINT,  sig_die)
            signal.signal(signal.SIGTERM, sig_die)
"""

    @classmethod
    def parse_args(cls):
        usage = "%prog [options] run_root"
        parser = OptionParser(usage=usage)

        parser.add_option("-l", "--log_file", dest="log_file", type="string",
                          default=None,
                          help='the log file for the daemon [default = %s/autocopy_YYMMDD.log]' % cls.LOG_DIR_DEFAULT)
        parser.add_option("-c", "--no_copy", dest="no_copy", action="store_true",
                          default=False,
                          help="don't copy run directories [default = allow copies]")

        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__=='__main__':

    (opts, args) = AutocopyRundir.parse_args()
    autocopy = AutocopyRundir(run_root_list=args, no_copy=opts.no_copy, log_file=opts.log_file)
    autocopy.run()


    #DEBUG
    print autocopy.RUN_ROOT_LIST
    print autocopy.COPY_PROCESSES
    print autocopy.LOG_FILE.name
