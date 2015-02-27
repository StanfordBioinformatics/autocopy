#!/Usr/bin/env python

###############################################################################
#
# autocopy.py - Copy sequencing run directories from a local data store
#   to a remote server
#
# autocopy.py -h for help
#
# AUTHORS:
#   Keith Bettinger, Nathan Hammond
#
###############################################################################

#  Installation notes
#    1. Keys must be configured to allow passwordless ssh to DEST_HOST.
#    2. Most tests use DEST_HOST='localhost', so on your personal machine you 
#       need to allow SSH and add your own public key to authorized_hosts to run 
#       tests.
#    3. Use config.json.example as a guide to create a config.json file that
#       will override default settings. If saved in the install dir it will be 
#       used automatically. Otherwise pass it to autocopy with the --config_file 
#       setting.
#    4. Set the env variables described below for LIMS access and MANDRILL
#       mail server access:
#         AUTOCOPY_SMTP_USERNAME, AUTOCOPY_SMTP_TOKEN, AUTOCOPY_SMTP_PORT, 
#         AUTOCOPY_SMTP_SERVER, UHTS_LIMS_URL, UHTS_LIMS_TOKEN
#
# Developer guidelines for myself
#   1. Keep this program as stateless as possible. Avoid replicating any data that
#      is already in the run directory of LIMS. Autocopy just needs to remember what 
#      copy processes are in progress, and should remember as little else as possible.
#      a. The state of a RunDir (AUTOCOPY_STARTED, etc.) is stored as a sentinel 
#         file dropped by rundir.py. When autocopy is restarted, RunDir state is 
#         read from sentinal files.
#      b. No LIMS info is stored by autocopy, to avoid getting out of sync. Query, 
#         use, forget.
#      c. However, Autocopy does need to remember pid's for copy operations, and to 
#         do this it keeps a list of RunDirs stored in Autocopy.rundirs_monitored. 
#         Each RunDir may contain copy process info (pid, start and stop time).
#   2. Don't crash if you can avoid it, and err on the side of start_copy rather than
#      waiting for operator intervention. When LIMS is unavailable, a warning should 
#      be logged and emailed, but autocopy should continue as normal.
#   3. Emails should be explicit about the action required by the operator.
#   4. Unittests can by run with test/test_autocopy.py. Keep them up to date 
#      when changing autocopy. 
#      a. For testing LIMS connections, tests use the scgpm_lims --local_only option, 
#         which simulates a LIMS connection using flatfile data checked into the 
#         scgpm_lims repository. For new tests that need specific LIMS data, check 
#         LIMS test data into the scgpm_lims repo rather than depending on the state of 
#         data in the production or staging LIMS.
#   5. There are three modes of external communication. Each may be disabled for testing 
#      via --no_copy, --no_email, and --test_mode_lims
#      a. Communication with the LIMS is managed by the scgpm_lims class.
#         Connection is HTTP or HTTPS. scgpm_lims uses these env variables:
#           UHTS_LIMS_URL, UHTS_LIMS_TOKEN
#      b. Communication with the Mandrill mail server via HTTPS. 
#         Uses these env variables:
#           AUTOCOPY_SMTP_USERNAME, AUTOCOPY_SMTP_TOKEN, 
#           AUTOCOPY_SMTP_PORT, AUTOCOPY_SMTP_SERVER
#      c. SSH copy
#         An ssh port to the destination cluster is opened on startup, used by rsync
#         for copying data to the cluster. The following settings control the copy step
#         and can be set in a config.json file:
#           COPY_DEST_HOST, COPY_DEST_USER, COPY_DEST_GROUP, COPY_DEST_RUN_ROOT
#
# LIMS status notes
#   These are the LIMS status fields used or set by autocopy
#   1. SolexaRun.sequencing_status
#      a. Has status 'sequencing' while new run is generated
#      b. Autocopy sets to 'done' when it detects sequencing is completed
#      c. If status 'sequencing failed' is set, autocopy transfers the run to AbortedRuns 
#         for deletionsocket
#      d. If status 'sequencing exception' is set, the run will not be discarded and
#         autocopy processes it as usual.
#   

import email.mime.text
import datetime
import grp
import json
from optparse import OptionParser
import os
import pwd
import re
import signal
import smtplib
import socket
import subprocess
import sys
import threading
import time
import traceback

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from bin.rundir import RunDir
from bin import rundir_utils
from scgpm_lims import Connection
from scgpm_lims import RunInfo

class ValidationError(Exception):
    pass

class Autocopy:

    LOG_DIR_DEFAULT = '/var/log'

    SUBDIR_COMPLETED = "Runs_Completed" # Runs are moved here after copy
    SUBDIR_ABORTED = "Runs_Aborted" # Runs are moved here if flagged 'sequencing_failed'

    LIMS_API_VERSION = 'v1'

    #TODO apply this cap
    MAX_COPY_PROCESSES = 2 # Cap the number of copy procs

    EMAIL_TO = None
    EMAIL_FROM = None

    # Where to copy the run directories
    COPY_DEST_HOST  = 'localhost'
    COPY_DEST_USER  = pwd.getpwuid(os.getuid()).pw_name
    COPY_DEST_GROUP = grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
    COPY_DEST_RUN_ROOT = '~'

    # Powers of two constants
    ONEKILO = 1024.0
    ONEMEG  = ONEKILO * ONEKILO
    ONEGIG  = ONEKILO * ONEMEG
    ONETERA = ONEKILO * ONEGIG

    # TODO notify for low space
    MIN_FREE_SPACE = ONETERA * 2 # Warn when run_root space is below this value

    MAIN_LOOP_DELAY_SECONDS = 2
    RUNROOT_FREESPACE_CHECK_DELAY_SECONDS = 3600
    RUNDIRS_MONITORED_SUMMARY_DELAY_SECONDS = 3600*24

    last_runroot_freespace_check = None
    last_rundirs_monitored_summary = None

    # Set the copy executable and add the directory of this script to its path.
    COPY_PROCESS_EXEC_FILENAME = "copy_rundir.py"
    COPY_PROCESS_EXEC_COMMAND = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC_FILENAME)

    def __init__(self, run_root_dirs = None, log_file=None, no_copy=False, no_lims=False, no_email=False, test_mode_lims=False, config=None, errors_to_terminal=False):
        self.initialize_config(config)
        self.initialize_no_copy_option(no_copy)
        self.initialize_hostname()
        self.initialize_log_file(log_file)
        self.initialize_lims_connection(test_mode_lims, no_lims)
        self.initialize_mail_server(no_email)
        self.initialize_run_roots(run_root_dirs)
        self.initialize_ssh_socket(no_copy)
        self.initialize_signals()
        self.redirect_stdout_stderr_to_log(errors_to_terminal)

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        try:
            self.cleanup_ssh_socket()
        except Exception as e:
            print e
        try:
            self.restore_stdout_stderr()
        except Exception as e:
            print e

    def run(self):
        self.send_email_autocopy_started()
        while True:
            try:
                self._main()
            except Exception, e:
                print e
                self.send_email_autocopy_exception(e)
            self.log("Sleeping for %s seconds\n" % self.MAIN_LOOP_DELAY_SECONDS)
            time.sleep(self.MAIN_LOOP_DELAY_SECONDS)

    def _main(self):
        self.log("Starting main loop\n")
        self.update_rundirs_monitored()
        for rundir in self.rundirs_monitored:
            self.process_rundir(rundir)

        if self.is_time_for_rundirs_monitored_summary():
            self.log("Sending rundirs monitored summary\n")
            self.send_email_rundirs_monitored_summary()

        if self.is_time_for_runroot_freespace_check():
            self.log("Checking runroot free space\n")
            self.check_runroot_freespace()

    def process_rundir(self, rundir):
        lims_runinfo = self.get_runinfo_from_lims(rundir)

        if self.is_rundir_aborted(lims_runinfo):
            self.process_aborted_rundir(rundir)

        if rundir.is_copying():
            self.process_copying_rundir(rundir, lims_runinfo)

        # process_ready_for_copy_rundir goes after process_copying_rundir
        # because when a copy process fails, process_copying_rundir resets
        # it to a ready_for_copy state, and we can start the copy process
        # in process_ready_for_copy_rundir right away.
        if self.is_rundir_ready_for_copy(rundir):
            self.process_ready_for_copy_rundir(rundir, lims_runinfo)

    def is_rundir_aborted(self, lims_runinfo):
        if lims_runinfo is None:
            return False
        else:
            return lims_runinfo.get_run_status() == lims_runinfo.SEQUENCING_RUN_STATUS_FAILED

    def is_rundir_ready_for_copy(self, rundir):
        return rundir.is_finished()

    def get_rundir_status(self, rundir):
        if rundir.is_copying():
            return "copying"
        elif self.is_rundir_ready_for_copy(rundir):
            return "ready_for_copy"
        else:
            return "not_ready"

    def process_ready_for_copy_rundir(self, rundir, lims_runinfo):
        self.start_copy(rundir)
        # TODO fix LIMS status update
#        lims_runinfo.set_run_status(lims_runinfo.DONE)

    def process_copying_rundir(self, rundir, lims_runinfo):
        # Check if the copy process finished successfully
        retcode = rundir.copy_proc.poll()
        if retcode == 0:
            self.process_completed_rundir(rundir, lims_runinfo)
        elif retcode == None:
            pass # Still copying. Do nothing.
        else:
            self.process_failed_copy_rundir(rundir, retcode)

    def process_failed_copy_rundir(self, rundir, retcode):
        self.send_email_rundir_copy_failed(rundir, retcode)
        # Revert status so copy can restart.
        rundir.reset_to_copy_not_started()

    def process_completed_rundir(self, rundir, lims_runinfo):
        are_files_missing = self.are_files_missing(rundir)
        lims_problems = self.check_rundir_against_lims(rundir, lims_runinfo)
        disk_usage = rundir.get_disk_usage()
        rundir.unset_copy_proc_and_set_stop_time()
        # TODO LIMS updates?
        self.send_email_rundir_copy_complete(rundir, are_files_missing, lims_problems, disk_usage)
        os.renames(rundir.get_path(),os.path.join(rundir.get_root(),self.SUBDIR_COMPLETED,rundir.get_dir()))
        self.rundirs_monitored.remove(rundir)

    def process_aborted_rundir(self, rundir):
        source = rundir.get_path()
        dest = os.path.join(rundir.get_root(),self.SUBDIR_ABORTED,rundir.get_dir())
        os.renames(source, dest)
        self.rundirs_monitored.remove(rundir)
        # TODO
        # Set all the status flags in the LIMS for this run.                                                                                                                                        
        #        all_flags_yes_dict = {'sequencer_done': 'yes', 'analysis_done': 'yes', 'dnanexus_done': 'yes',
        # 'notification_done': 'yes', 'archiving_done': 'yes'}
        # flowcell = Done in LIMS
        self.send_email_rundir_aborted(rundir, dest)
        
    def get_freespace(self, directory):
        stats = os.statvfs(directory)
        freespace_bytes = stats.f_bfree * stats.f_frsize
        return freespace_bytes

    def is_time_for_rundirs_monitored_summary(self):
        if self.last_rundirs_monitored_summary == None:
            return True
        timedelta = datetime.datetime.now() - self.last_rundirs_monitored_summary
        if timedelta.total_seconds() > self.RUNDIRS_MONITORED_SUMMARY_DELAY_SECONDS:
            return True
        else:
            return False

    def is_time_for_runroot_freespace_check(self):
        if self.last_runroot_freespace_check == None:
            return True
        timedelta = datetime.datetime.now() - self.last_runroot_freespace_check
        if timedelta.total_seconds() > self.RUNROOT_FREESPACE_CHECK_DELAY_SECONDS:
            return True
        else:
            return False

    def check_runroot_freespace(self):
        for run_root in self.RUN_ROOT_DIRS:
            freespace_bytes = self.get_freespace(run_root)
            if freespace_bytes < self.MIN_FREE_SPACE:
                self.send_email_low_freespace(run_root, freespace_bytes)
        self.last_runroot_freespace_check = datetime.datetime.now()

    def initialize_hostname(self):
        hostname = socket.gethostname()
        self.HOSTNAME = hostname[0:hostname.find('.')] # Remove domain part.

    def initialize_lims_connection(self, is_test_mode, no_lims):
        if no_lims:
            self.LIMS = None
        else:
            self.LIMS = Connection(apiversion=self.LIMS_API_VERSION, local_only=is_test_mode)

    def initialize_mail_server(self, no_email):
        self.NO_EMAIL = no_email
        if self.NO_EMAIL:
            return

        (smtp_server, smtp_port, smtp_username, smtp_token) = self.get_mail_server_settings()

        self.log("Connecting to mail server...")
        try:
            self.smtp = smtplib.SMTP(smtp_server, smtp_port, timeout=5)
            self.smtp.login(smtp_username, smtp_token)
            self.log("success.")
        except socket.gaierror:
            print "Could not connect to SMTP server. Are you offline? Try running with --no_email."
            sys.exit(1)

    def get_mail_server_settings(self):
        settings = self.get_mail_server_settings_from_env()
        if not all(settings):
            settings = self.get_mail_server_settings_from_user()
        if not all(settings):
            raise ValidationError('SMTP server settings are required')
        return settings

    def get_mail_server_settings_from_env(self):
        smtp_server = os.getenv('AUTOCOPY_SMTP_SERVER')
        smtp_port = os.getenv('AUTOCOPY_SMTP_PORT')
        smtp_username = os.getenv('AUTOCOPY_SMTP_USERNAME')
        smtp_token = os.getenv('AUTOCOPY_SMTP_TOKEN')
        return (smtp_server, smtp_port, smtp_username, smtp_token)

    def get_mail_server_settings_from_user(self, smtp_server, smtp_port, smtp_username, smtp_token):
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
        return (smtp_server, smtp_port, smtp_username, smtp_token)

    def initialize_log_file(self, log_file):
        if log_file == "-":
            self.LOG_FILE = sys.stdout
        elif log_file:
            self.LOG_FILE = open(log_file, "w")
        else:
            self.LOG_FILE = open(os.path.join(self.LOG_DIR_DEFAULT,
                                              "autocopy_%s.log" % datetime.datetime.today().strftime("%y%m%d")),'a')

    def initialize_ssh_socket(self, no_copy=False):
        if no_copy:
            self.SSH_SOCKET = None
            return

        self.SSH_SOCKET = "/tmp/autocopy_copy_%d_%s.ssh" % (os.getpid(), time.time())
        ssh_cmd_list = ["ssh", "-o", "ConnectTimeout=10", "-l", self.COPY_DEST_USER,
                        "-S", self.SSH_SOCKET, "-M", "-f", "-N",
                        self.COPY_DEST_HOST]
        retcode = subprocess.call(ssh_cmd_list, stderr=subprocess.STDOUT)
        if retcode:
            print >> sys.stderr, os.path.basename(__file__), ": cannot create ssh socket into", self.COPY_DEST_HOST, "( retcode =", retcode, ")"
            sys.exit(1)

    def initialize_run_roots(self, run_root_dirs):
        # If run root dirs not provided, use current directory
        if not run_root_dirs:
            run_root_dirs = [os.getcwd()]
        elif not len(run_root_dirs):
            run_root_dirs = [os.getcwd()]
        self.RUN_ROOT_DIRS = run_root_dirs

        for run_root in self.RUN_ROOT_DIRS:
            self.create_run_root_on_disk(run_root)

    def create_run_root_on_disk(self, run_root):
        # Create and prepare run root dirs if they do not exist
        if not os.path.exists(run_root):
            os.makedirs(run_root, 0775)
        aborted_subdir = os.path.join(run_root, self.SUBDIR_ABORTED)
        completed_subdir = os.path.join(run_root, self.SUBDIR_COMPLETED)
        if not os.path.exists(aborted_subdir):
            os.mkdir(aborted_subdir, 0775)
        if not os.path.exists(completed_subdir):
            os.mkdir(completed_subdir, 0775)
        self.leave_ok_to_delete_readme(aborted_subdir)
        self.leave_ok_to_delete_readme(completed_subdir)

    def cleanup_ssh_socket(self):
        if not hasattr(self, 'SSH_SOCKET'):
            return
        if self.SSH_SOCKET is None:
            return
        retcode = subprocess.call(["ssh", "-O", "exit", "-S", self.SSH_SOCKET, self.COPY_DEST_HOST], stdout=self.LOG_FILE, stderr=self.LOG_FILE)
        if retcode:
            raise Exception("%s: cannot close ssh socket into %s ( retcode = %s )" % (os.path.basename(__file__), self.COPY_DEST_HOST, retcode))

    def leave_ok_to_delete_readme(self, directory):
        readme = os.path.join(directory, 'README.txt')
        if not os.path.exists(readme):
            with open(readme, 'w') as f:
                f.write('Runs in this directory are generally OK to delete.')

    def update_rundirs_monitored(self):
        # sync rundirs_monitored with what's on disk

        if not hasattr(self, 'rundirs_monitored'):
            # Initialize this instance var once after startup
            self.rundirs_monitored = []

        new_rundirs_monitored = []
        for run_root in self.RUN_ROOT_DIRS:
            new_rundirs_monitored.extend(self.scan_for_rundirs(run_root))

        # We just removed any rundirs we found on disk from rundirs_monitored.
        # Any dirs left are what we couldn't find on disk.
        # Send email warning and then forget them.
        for missing_rundir in self.rundirs_monitored:
            self.send_email_missing_rundir(missing_rundir)

        self.rundirs_monitored = new_rundirs_monitored

    def scan_for_rundirs(self, run_root):
        rundirs_found_on_disk = []
        for dirname in os.listdir(run_root): 
            # Get directories, not files
            if (os.path.isdir(os.path.join(run_root, dirname)) and
                # Exclude rundir if it doesn't begin with a 6-digit start date
                re.match("\d{6}_", dirname) and
                # Exclude special subdirs
                dirname not in [self.SUBDIR_COMPLETED, self.SUBDIR_ABORTED]):
                rundirs_found_on_disk.append(self.get_or_create_rundir(run_root, dirname, remove=True))
        return rundirs_found_on_disk

    def get_or_create_rundir(self, run_root, dirname, remove=False):
        matching_rundir = self.get_rundir(run_root=run_root, dirname=dirname)
        if matching_rundir:
            if remove:
                self.rundirs_monitored.remove(matching_rundir)
            return matching_rundir
        else:
            return RunDir(run_root, dirname)

    def are_files_missing(self, rundir):
        # Check that the run directory has all the right files.
        files_missing = not rundir_utils.validate(rundir)
        return files_missing

    def get_runinfo_from_lims(self, rundir):
        if self.LIMS == None:
            return None
        try:
            runinfo = RunInfo(conn=self.LIMS, run=rundir.get_dir())
        except:
            runinfo = None
        return runinfo

    def check_rundir_against_lims(self, rundir, runinfo, testproblem=None):
        if runinfo == None:
            return []
        fields_to_check = [
            ['Run name', rundir.get_dir(), runinfo.get_run_name()],
            #TODO, all the checks in lims.lims_run_check_rundir
#            ['Sequencer', rundir.get_machine().lower(), runinfo.get_machine().lower()],
#            ['Data volume', rundir.get_data_volume(), runinfo.get_data_volume()],
#            ['Local run dir', rundir.get]
        ]

        if testproblem:
            fields_to_check.append(testproblem)

        problems_found = []
        for (field, rundirval, limsval) in fields_to_check:
            if rundirval != limsval:
                problems_found.append('Mismatched value "%s". Value in run directory: %s. Value in LIMS: %s' % (field, rundirval, limsval))
        return problems_found

    def start_copy(self, rundir, rsync=True):
        COPY_COMPLETE_FILE = "Autocopy_complete.txt"
        copy_cmd_list = [self.COPY_PROCESS_EXEC_COMMAND,
                         "--host", self.COPY_DEST_HOST,
                         "--user", self.COPY_DEST_USER,
                         "--group", self.COPY_DEST_GROUP,
                         "--dest_root", self.COPY_DEST_RUN_ROOT,
                         "--status_file", COPY_COMPLETE_FILE,
                         "--ssh_socket", self.SSH_SOCKET,
                     ]
        if rsync:
            copy_cmd_list.append("--rsync")
        
        # End command with run directory to copy.
        copy_cmd_list.append(rundir.get_path())

        # Copy the directory.
        copy_proc = subprocess.Popen(copy_cmd_list,
                                     stdout=self.LOG_FILE, stderr=subprocess.STDOUT)
        rundir.set_copy_proc_and_start_time(copy_proc)

    def send_email_autocopy_exception(self, exception):
        tb = traceback.format_exc(exception)
        email_subj = "Autocopy unknown exception"
        email_body = "The autocopy daemon failed with Exception\n" + tb
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_autocopy_started(self):
        email_subj = "Daemon Started"
        email_body = "The Autocopy Daemon was started.\n\n"
        email_body += "You should receive a message with a summary of active run directories soon."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_autocopy_stopped(self):
        email_subj = "Daemon Stopped"
        email_body = "The Autocopy Daemon received a kill signal and is shutting down.\n\n"
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundir_aborted(self, rundir, dest_path):
        email_subj = "Run Directory Aborted: %s" % rundir.get_dir()
        email_body = "The following run was flagged as 'sequencing failed' in the LIMS:\n\n"
        email_body += "\t%s\n\n" % rundir.get_dir()
        email_body += "It has been moved to this directory:"
        email_body += "\t%s\n\n" % dest_path
        email_body += "If this was an error, please correct the sequencing status in the LIMS and manually move the run out of the %s folder.\n\n" % self.SUBDIR_ABORTED
        email_body += "Otherwise, this run may be safely deleted to free up disk space."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundir_copy_failed(self, rundir, retcode):
        email_subj = "ERROR COPYING Run Dir " + rundir.get_dir()
        email_body = "Please try to resolve the error. Autocopy will continue attempting to copy as long as the run remains in the run_root directory.\n\n"
        email_body = "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "\n"
        email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "Return code:\t%d\n" % retcode
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundir_copy_complete(self, rundir, are_files_missing, lims_problems, disk_usage):
        if disk_usage > self.ONEKILO:
            disk_usage /= self.ONEKILO
            disk_usage_units = "Tb"
        else:
            disk_usage_units = "Gb"

        if are_files_missing or (len(lims_problems) > 0):
            email_subj = "Problems copying run dir %s" % rundir.get_dir()
        else:
            email_subj = "Finished copying run dir %s" % rundir.get_dir()

        email_body = 'Finished copying run %s\n\n' % rundir.get_dir()

        if are_files_missing:
            email_body += "*** RUN HAS MISSING FILES ***\n\n"

        if len(lims_problems) > 0:
            email_body = "%s: *** RUN HAS INCONSISTENCIES WITH LIMS\n\n" % rundir.get_dir()
            email_body = "Check the problems below and correct any errors in the LIMS:\n\n"
        for problem in lims_problems:
            email_body += "%s\n" % problem

        # Send an email announcing the completed run directory copy.
        email_body += "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "NEW LOCATION:\t\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "\n"
        email_body += "Read count:\t\t%d\n" % rundir.get_reads()
        email_body += "Cycles:\t\t\t%s\n" % " ".join(map(lambda d: str(d), rundir.get_cycle_list()))
        email_body += "\n"
        email_body += "Copy time:\t\t%s\n" % str(rundir.copy_end_time - rundir.copy_start_time)
        email_body += "Disk usage:\t\t%.1f %s\n" % (disk_usage, disk_usage_units)
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_missing_rundir(self, rundir):
        email_subj = "Missing Run Dir %s" % rundir.get_dir()
        email_body = "MISSING RUN:\t%s\n" % rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, rundir.get_root(), rundir.get_dir())
        email_body += "Autocopy was tracking this run, but can no longer find it on disk."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_low_freespace(self, run_root, freebytes):
        email_subj = "Insufficient free space in %s" % os.path.abspath(run_root)
        email_body = "The following run root directory:\n\n %s\n\n" % os.path.abspath(run_root)
        email_body += "has %f GB remaining.\n\n" % (freebytes/self.ONEGIG)
        email_body += "A warning is sent when free space is less than %f GB" % (self.MIN_FREE_SPACE/self.ONEGIG)
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundirs_monitored_summary(self):
        email_subj = 'Run status summary'
        email_body = ''
        for run_root in self.RUN_ROOT_DIRS:
            email_body += '%s\n\n' % os.path.abspath(run_root)
            for run_dir in self.get_rundirs(run_root=run_root):
                status = self.get_rundir_status(run_dir)
                email_body += "%s\t%s\n" % (run_dir.get_dir(), status)
            email_body += "\n"
            email_body += '\t%f GB free\n\n' % (self.get_freespace(run_root)/self.ONEGIG)
        self.send_email(self.EMAIL_TO, email_subj, email_body)
        self.last_rundirs_monitored_summary = datetime.datetime.now()

    def send_email(self, to, subj, body, write_email_to_log=True):
        subj_prefix = "AUTOCOPY (%s): " % self.HOSTNAME
        msg = email.mime.text.MIMEText(body)
        msg['Subject'] = subj_prefix + subj
        msg['From'] = self.EMAIL_FROM
        if isinstance(to,list):
            msg['To'] = ','.join(to)
        else:
            msg['To'] = to
        if self.NO_EMAIL:
            self.log("email suppressed because --no_email is set")
        else:
            self.smtp.sendmail(msg['From'], to, msg.as_string())
        if write_email_to_log:
            self.log("v----------- begin email -----------v")
            self.log(msg.as_string())
            self.log("^------------ end email ------------^\n")

    def log(self, *args):
        log_text = ' '.join(args)
        log_lines = log_text.split("\n")
        for line in log_lines:
            print >> self.LOG_FILE, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), line)
        self.LOG_FILE.flush()

    def initialize_config(self, config):
        if config is None:
            if os.path.exists(os.path.join(os.path.dirname(__file__), 'config.json')):
                with open(os.path.join(os.path.dirname(__file__), 'config.json')) as f:
                    config = json.load(f)

        self.override_settings_with_config(config)

    def override_settings_with_config(self, config):
        # Allows certain class variables to be overridden

        if config is None:
            return

        # Input validators
        def validate_str(value):
            if not (isinstance(value, str) or isinstance(value, unicode)):
                raise ValidationError("Invalid value %s for config key %s. A string is required." %(value, key))
        def validate_cmdline_safe_str(value):
            pattern = '^[0-9a-zA-Z./_-]*$'
            if not re.match(pattern, value):
                raise ValidationError("Invalid value %s for config key %s. Must be a string matched by %s" %(value, key, pattern))
        def validate_int(value):
            if not isinstance(value, int):
                raise ValidationError("Invalid value %s for config key %s. An integer is required." %(value, key))

        def validate(key, value, config_fields):
            if key not in config_fields.keys():
                raise ValidationError("Config contains invalid key %s. Valid keys are %s" % 
                                (key, config_fields.keys()))
            run_validation_function = config_fields[key]
            run_validation_function(value)
 
        config_fields = {
            'LOG_DIR_DEFAULT': validate_str,
            'SUBDIR_COMPLETED': validate_str,
            'SUBDIR_ABORTED': validate_str,
            'LIMS_API_VERSION': validate_str,
            'MAX_COPY_PROCESSES': validate_int,
            'EMAIL_TO': validate_str,
            'EMAIL_FROM': validate_str,
            'COPY_DEST_HOST': validate_cmdline_safe_str,
            'COPY_DEST_USER':validate_cmdline_safe_str,
            'COPY_DEST_GROUP': validate_cmdline_safe_str,
            'COPY_DEST_RUN_ROOT': validate_cmdline_safe_str,
            'MIN_FREE_SPACE': validate_int,
            'LOOP_DELAY_SECONDS': validate_int,
        }

        for key in config.keys():
            value = config[key]
            validate(key, value, config_fields)
            setattr(self, key, value)
            
    def initialize_no_copy_option(self, no_copy):
        # Number of copy processes
        if no_copy:
            self.MAX_COPY_PROCESSES = 0

    def redirect_stdout_stderr_to_log(self, errors_to_terminal):
        if errors_to_terminal:
            return
        self.STDOUT_RESTORE = sys.stdout
        self.STDERR_RESTORE = sys.stderr
        sys.stdout = self.LOG_FILE
        sys.stderr = self.LOG_FILE

    def restore_stdout_stderr(self):
        if hasattr(self, 'STDOUT_RESTORE'):
            sys.stdout = self.STDOUT_RESTORE
        if hasattr(self, 'STDERR_RESTORE'):
            sys.stderr = self.STDERR_RESTORE

    def initialize_signals(self):
        signal.signal(signal.SIGINT,  self.receive_sig_die)
        signal.signal(signal.SIGTERM, self.receive_sig_die)
        signal.signal(signal.SIGUSR1, self.receive_sig_USR1)

    def receive_sig_die(self, signum, frame):
        self.send_email_autocopy_stopped()
        self.cleanup()
        sys.exit(0)

    def receive_sig_USR1(self, signum, frame):
        self.log("Received USR1 signal.")
        self.log("Sending rundirs monitored summary\n")
        self.send_email_rundirs_monitored_summary()


        """
    @classmethod
    def initialize_signals(cls):

        def sigUSR2(signum, frame):
            global COPY_PROCESSES
    
            # Toggle whether the copying out takes place.
            if COPY_PROCESSES != MAX_COPY_PROCESSES:
                COPY_PROCESSES = MAX_COPY_PROCESSES
                log("USR2 signal received: COPYING TURNED ON")
            else:
                COPY_PROCESSES = 0
                log("USR2 signal received: NEW COPYING TURNED OFF")

"""

#    def generate_run_status_line(run_root):
#        if self.COPY_PROCESSES == self.MAX_COPY_PROCESSES:
#            ready_str = "Ready"
#            newcopying_str = ""
#        else:
#            ready_str = "READY"
#            newcopying_str = "(NEW COPYING TURNED OFF)"
#        self.log("%s:" % run_root,
#                 "%d Copying," % len(self.get_copying_rundirs(run_root)),
#                 "%d %s," % (len(self.get_ready_rundirs(run_root)), ready_str),
#                 "%d Running," % len(self.get_running_rundirs(run_root)),
#                 "%d Archiving," % len(self.get_archiving_rundirs(run_root)),
#                 "%d Completed," % len(self.get_completed_rundirs(run_root)),
#                 "%d Aborted," % len(self.get_aborted_rundirs(run_root)),
#                 "%d Failed" % len(self.get_failed_rundirs(run_root)),
#                 newcopying_str)

#    def generate_run_status_table(self):
#        run_status_table = ""
#        copying_rundirs = self.get_copying_rundirs()
#        if self.COPY_PROCESSES == self.MAX_COPY_PROCESSES:
#            run_status_table += "COPYING DIRECTORIES:\n"
#        else:
#            run_status_table += "COPYING DIRECTORIES: (New copying turned off)\n"
#        run_status_table += "-------------------\n"
#        if len(copying_rundirs):
#            for rundir in copying_rundirs:
#                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
#        else:
#            run_status_table += "None\n"

#        ready_rundirs = self.get_ready_rundirs()
#        run_status_table += "\n"
#        run_status_table += "READY DIRECTORIES:\n"
#        run_status_table += "-----------------\n"
#        if len(ready_rundirs):
#            for rundir in ready_rundirs:
#                run_status_table += "%s %s (%s)\n" % (rundir.get_dir().ljust(32), rundir.get_root(), rundir.get_status_string())
#        else:
#            run_status_table += "None\n"

#        running_rundirs = self.get_running_rundirs()
#        run_status_table += "\n"
#        run_status_table += "RUNNING DIRECTORIES:\n"
#        run_status_table += "-------------------\n"
#        if len(running_rundirs):
#            for rundir in running_rundirs:
#                #run_status_table += rundir.get_dir().ljust(32) + rundir.get_root() + "(%s)" % rundir.get_status_string()
#                run_status_table += "%s %s (Cycle %s of %s)\n" % (rundir.get_dir().ljust(32), rundir.get_root(), rundir.get_scored_cycle(), rundir.get_total_cycles())
#        else:
#            run_status_table += "None\n"

#        archiving_rundirs = self.get_archiving_rundirs()
#        run_status_table += "\n"
#        run_status_table += "ARCHIVING DIRECTORIES:\n"
#        run_status_table += "---------------------\n"
#        if len(archiving_rundirs):
#            for rundir in archiving_rundirs:
#                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
#        else:
#            run_status_table += "None\n"

#        completed_rundirs = self.get_completed_rundirs()
#        run_status_table += "\n"
#        run_status_table += "COMPLETED DIRECTORIES:\n"
#        run_status_table += "---------------------\n"
#        if len(completed_rundirs):
#            for rundir in completed_rundirs:
#                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
#        else:
#            run_status_table += "None\n"

#        aborted_rundirs = self.get_aborted_rundirs()
#        run_status_table += "\n"
#        run_status_table += "ABORTED DIRECTORIES:\n"
#        run_status_table += "-------------------\n"
#        if len(aborted_rundirs):
#            for rundir in aborted_rundirs:
#                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
#        else:
#            run_status_table += "None\n"

#        failed_rundirs = self.get_failed_rundirs()
#        run_status_table += "\n"
#        run_status_table += "FAILED DIRECTORIES:\n"
#        run_status_table += "------------------\n"
#        if len(failed_rundirs):
#            for rundir in failed_rundirs:
#                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
#        else:
#            run_status_table += "None\n"

#        return run_status_table

    def get_rundir(self, run_root=None, dirname=None):
        rundirs = self.get_rundirs(run_root=run_root, dirname=dirname)
        if len(rundirs) == 0:
            return None
        if len(rundirs) > 1:
            raise Exception("More than one matching rundir with run_root=%s, dirname=%s. These all matched: %s" 
                            % (run_root, dirname, rundirs))
        return rundirs[0]

    def get_rundirs(self, run_root=None, dirname=None):
        rundirs = self.rundirs_monitored
        if dirname is not None:
            rundirs = filter(lambda rundir: rundir.get_dir() == dirname, rundirs)
        if run_root is not None:
            rundirs = filter(lambda rundir: rundir.get_root() == run_root, rundirs)
        return rundirs

    @classmethod
    def parse_args(cls):
        usage = "%prog [options] run_root"
        parser = OptionParser(usage=usage)

        parser.add_option("-l", "--log_file", dest="log_file", type="string",
                          default=None,
                          help='Log file path and filename. Use "-" to write to stdout instead of file. [default = %s/autocopy_{YYMMDD}.log]' 
                          % cls.LOG_DIR_DEFAULT)
        parser.add_option("-c", "--no_copy", dest="no_copy", action="store_true",
                          default=False,
                          help="Don't copy run directories")
        parser.add_option("-m", "--no_lims", dest="no_lims", action="store_true",
                          default=False,
                          help="Don't query or write to the LIMS")
        parser.add_option("-e", "--no_email", dest="no_email", action="store_true",
                          default=False,
                          help="Don't send email notifications")
        parser.add_option("-d", "--dry_run", dest="dry_run", action="store_true",
                          default=False,
                          help='Same as "--no_copy --no_lims --no_email"')
        parser.add_option("-g", "--config", dest="config_file", type="string",
                          default=None,
                          help='Config file to override default settings [default = ./config.json]')
        parser.add_option("-t", "--test_mode_lims", dest="test_mode_lims", action="store_true", default=False,
                          help="Use a simulated LIMS connection")
        parser.add_option("-s", "--errors_to_terminal", dest="errors_to_terminal", action="store_true", default=False,
                          help="Do not redirect all stderr and stdout to log. Set this when using the debugger, or to see error messages in the terminal.")
        parser.add_option("-i", "--interactive", dest="interactive", action="store_true", default=False,
                          help='Same as "--errors_to_terminal --log_file -"')

        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__=='__main__':

    (opts, args) = Autocopy.parse_args()

    if opts.config_file:
        with open(opts.config_file) as f:
            config = json.load(f)
    else:
        config = None

    if opts.dry_run:
        (no_lims, no_copy, no_email) = (True, True, True)
    else:
        (no_lims, no_copy, no_email) = (opts.no_lims, opts.no_copy, opts.no_email)

    if opts.interactive:
        (log_file, errors_to_terminal) = ('-', True)
    else:
        (log_file, errors_to_terminal) = (opts.log_file, opts.errors_to_terminal)

    autocopy = Autocopy(run_root_dirs=args, no_copy=no_copy, no_email=no_email, no_lims=no_lims, log_file=log_file, 
                        config=config, test_mode_lims=opts.test_mode_lims, errors_to_terminal=errors_to_terminal)
    autocopy.run()
