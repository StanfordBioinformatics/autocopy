#!/Usr/bin/env python

###############################################################################
#
# autocopy_rundir.py - Copy run directories to the cluster as they are created.
#
# ARGS:
#   all: Directories below which to monitor for run directories.
#
# SWITCHES:
#     TODO update
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

import email.mime.text
import datetime
from optparse import OptionParser
import os
import pwd
import re
import signal
import smtplib
import socket
import subprocess
import sys

from rundir import RunDir
import rundir_utils
from scgpm_lims.connection import Connection, RunInfo

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

    LOOP_DELAY_SECONDS = 600

    # Set the copy executable and add the directory of this script to its path.
    COPY_PROCESS_EXEC = "copy_rundir.py"
    COPY_PROCESS_EXEC = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC)

    def __init__(self, run_root_list = None, log_file=None, no_copy=False, no_lims=False, redirect_stdout_stderr_to_log=True, no_mail=False, test_mode_lims=False):
        # Some options are for testing only and not available from the commandline:
        #   redirect_stdout_stderr_to_log, no_mail

        # If run root dirs not provided, use current directory
        if not run_root_list:
            run_root_list = [os.getcwd()]
        elif not len(run_root_list):
            run_root_list = [os.getcwd()]
        self.RUN_ROOT_LIST = run_root_list

        # List of directories being monitored
        self.rundirs_monitored = [] 

        # Number of copy processes
        if no_copy:
            self.MAX_COPY_PROCESSES = 0
    
        self.initialize_hostname()
        self.initialize_lims_connection(test_mode_lims) # Connect to the LIMS
        self.initialize_mail_server(no_mail=no_mail) # Connect to mail server
        self.initialize_log_file(log_file)
        if redirect_stdout_stderr_to_log:
            # Direct stdout and stderr to log file.
            # Any interactive startup functions should be finished before you do this.
            print "Logging all output to %s" % self.LOG_FILE.name
            sys.stdout = self.LOG_FILE
            sys.stderr = self.LOG_FILE
        self.initialize_run_roots()
        self.initialize_ssh_socket(no_copy)

    def __del__(self):
        self.cleanup_ssh_socket()

    def run(self):
        self.send_email_start_msg()
        try:
            while True:
                self._main()
                time.sleep(self.LOOP_DELAY_SECONDS)
        except Exception, e:
            tb = traceback.format_exc(e)
            self.send_email(EMAIL_TO, "Autocopy crashed", "The autocopy daemon crashed with Exception\n" + tb)
            raise e

    def _main(self):
        self.scan_run_roots_to_discover_rundirs()
        for rundir in self.rundirs_monitored:
            rundir.update_status()
        self.process_ready_for_copy_dirs()
        self.process_copying_dirs()
        self.process_completed_dirs()
        self.process_aborted_dirs()

    def process_ready_for_copy_dirs(self):
        for rundir in self.get_ready_for_copy_dirs():
            self.scan_rundir_for_missing_files(rundir)
            self.check_rundir_against_lims(rundir)
            self.start_copy(rundir)
            rundir.set_status(RunDir.STATUS_COPY_STARTED)

    def process_copying_dirs(self):
        # get them
        # poll for status
        #   None -> pass
        #   O or 5 -> set status to complete
        #   error -> ??
        pass

    def process_completed_dirs(self):
        # update LIMS
        # move to Completed
        pass

    def process_aborted_dirs(self):
        # How did we get here exactly? Is this anybody with sequencing_failed?
        pass

    def initialize_hostname(self):
        hostname = socket.gethostname()
        self.HOSTNAME = hostname[0:hostname.find('.')] # Remove domain part.

    def initialize_lims_connection(self, is_test_mode):
        self.LIMS = Connection(apiversion=self.LIMS_API_VERSION, local_only=is_test_mode)

    def initialize_mail_server(self, no_mail=False):
        self.NO_MAIL = no_mail
        if self.NO_MAIL:
            return

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

    def initialize_log_file(self, log_file):
        if log_file == "-":
            self.LOG_FILE = sys.stdout
        elif log_file:
            self.LOG_FILE = open(log_file, "w")
        else:
            self.LOG_FILE = open(os.path.join(self.LOG_DIR_DEFAULT,
                                              "autocopy_%s.log" % datetime.datetime.today().strftime("%y%m%d")),'a')

    # sync rundirs_monitored with what's on disk
    def scan_run_roots_to_discover_rundirs(self):
        new_rundirs_monitored = []
        for run_root in self.RUN_ROOT_LIST:
            for dirname in os.listdir(run_root): # Directories on disk
                # Include only directories that begin with a 6-digit start date
                if (os.path.isdir(os.path.join(run_root, dirname)) and
                    re.match("\d{6}_", dirname)):
                    new_rundirs_monitored.append(self.retrieve_or_create_rundir(run_root, dirname, remove=True))

        for missing_rundir in self.rundirs_monitored:
            # These were being monitored but are no longer found on disk
            #   Send notice before we overwrite the rundirs_monitored list
            self.send_email_missing_rundir(missing_rundir, run_root)

        self.rundirs_monitored = new_rundirs_monitored

    def retrieve_or_create_rundir(self, run_root, dirname, remove=False):
        matching_rundir = self.get_rundir(run_root=run_root, dirname=dirname)
        if matching_rundir:
            if remove:
                self.rundirs_monitored.remove(matching_rundir)
            return matching_rundir
        else:
            # TODO we found a new dir. Send an email.
            # if new_rundir.get_status() < RunDir.STATUS_COPY_COMPLETE
            return RunDir(run_root, dirname)

    def add_to_rundirs_monitored(self, run_root, dirname):
        self.rundirs_monitored.append(RunDir(run_root, dirname))

    def scan_rundir_for_missing_files(self, rundir):
        # Check that the run directory has all the right files.
        isvalid = rundir_utils.validate(rundir)
        if not isvalid:
            self.send_email_invalid_rundir(rundir)
        return isvalid

    def check_rundir_against_lims(self, rundir, testproblem=None):
        try:
            runinfo = RunInfo(conn=self.LIMS, run=rundir.get_dir())
        except:
            problems_found = ["The query for runinfo from the LIMS failed"]
            self.send_email_check_rundir_against_lims(rundir, problems_found)
            return problems_found

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
        self.send_email_check_rundir_against_lims(rundir, problems_found)
        return problems_found


    """
            ['hostname
            #
            # Check local run dir.
            #
            if field_dict['local_run_dir']:
                hostpath_split = field_dict['local_run_dir'].split(":")
                if len(hostpath_split) == 2:
                    lims_hostname = hostpath_split[0]
                    lims_run_root = hostpath_split[1]
                else:
                    lims_hostname = None
                    lims_run_root = hostpath_split[0]

                # Compare possible hostname in LIMS local_run_dir to this host.
                if lims_hostname and lims_hostname != HOSTNAME:
                    check_msg += "RunDir local run dir hostname %s does not match LIMS local run dir hostname %s\n" % (HOSTNAME, lims_hostname)

                # Compare run roots.
                if rundir.get_root() != lims_run_root:
                    check_msg += "RunDir local run dir root %s does not match LIMS local run dir root %s\n" % (rundir.get_root(), lims_run_root)

        # Check sequencer kit version.
#        if rundir.get_seq_kit_version() != field_dict['seq_kit_version']:
#            check_msg += "RunDir sequencer kit %s does not match LIMS sequencer kit %s\n" % (rundir.get_seq_kit_version(), field_dict['seq_kit_version'])

        # Prepare for checking RunDir's sequencer software.
        sw_version = rundir.get_control_software_version()
        if sw_version:
            if rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA:
                seq_software = 'scs_%s' % sw_version[:3].replace('.','_')
            elif rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ:
                if sw_version == "1.3.8" or \
                   sw_version == "1.4.8" or \
                   sw_version == "1.5.15":
                    seq_software = 'hcs_%s' % sw_version.replace('.','_')
                elif sw_version.startswith("1.3.8") or \
                     sw_version.startswith("1.4.8"):
                    seq_software = 'hcs_%s' % sw_version[:5].replace('.','_')
                elif sw_version.startswith("1.5.15"):
                    seq_software = 'hcs_%s' % sw_version[:6].replace('.','_')
                else:
                    seq_software = 'hcs_%s' % sw_version[:3].replace('.','_')
            elif rundir.get_platform() == rundir.PLATFORM_ILLUMINA_MISEQ:
                seq_software = "mcs_%s" % sw_version.replace('.','_')
            else:
                print >> sys.stderr, "WARNING: platform unknown (%s)" % rundir.get_platform()
                seq_software = None
        else:
            seq_software = None

        # Check software version.
        if seq_software != field_dict['seq_software']:
            check_msg += "RunDir software %s does not match LIMS software %s\n" % (seq_software, field_dict['seq_software'])

        # Check paired end.
        paired_end = rundir.is_paired_end()
        if ((paired_end and field_dict['paired_end'] != 'yes') or
            (not paired_end and field_dict['paired_end'] == 'yes')):
            check_msg += "RunDir paired end %s does not match LIMS paired end %s\n" % (paired_end, field_dict['paired_end'])

        # Prepare to compare read cycles.
        cycle_list = rundir.get_cycle_list()
        read1_cycles = cycle_list[0]
        if len(cycle_list) == 1:
            read2_cycles = None
        elif len(cycle_list) == 2:
            if paired_end:
                read2_cycles = cycle_list[1]
            else:  # Two reads without paired end means second read is indexed read.
                read2_cycles = None
        elif len(cycle_list) == 3:
            read2_cycles = cycle_list[2]
        elif len(cycle_list) == 4:
            read2_cycles = cycle_list[3]
        else:
            read2_cycles = None

        # Check index read.
        index_read = rundir.is_index_read()
        if ((index_read and field_dict['index_read'] != 'yes') or
            (not index_read and field_dict['index_read'] == 'yes')):
            check_msg += "RunDir index read %s does not match LIMS index read %s\n" % (index_read, field_dict['index_read'])

        # Check read1_cycles.
        if read1_cycles != int(field_dict['read1_cycles']):
            check_msg += "RunDir read1 cycles %s does not match LIMS read1 cycles %s\n" % (read1_cycles, field_dict['read1_cycles'])

        # Prepare LIMS read2_cycles for comparison.
        if len(field_dict['read2_cycles']) > 0:
            lims_read2_cycles = int(field_dict['read2_cycles'])
        else:
            lims_read2_cycles = None

        # Check read2_cycles.
        if read2_cycles != lims_read2_cycles:
            check_msg += "RunDir read2 cycles %s does not match LIMS read2 cycles %s\n" % (read2_cycles, lims_read2_cycles)

        # Return the list of mismatch messages, if any.
        if len(check_msg):
            return check_msg
        else:
            return None
        pass
"""

    def cleanup_ssh_socket(self):
        if not self.SSH_SOCKET:
            return
        retcode = subprocess.call(["ssh", "-O", "exit", "-S", self.SSH_SOCKET, self.COPY_DEST_HOST],
                                  stdout=self.LOG_FILE, stderr=subprocess.STDOUT)
        if retcode:
            print >> sys.stderr, os.path.basename(__file__), ": cannot close ssh socket into", self.COPY_DEST_HOST, "( retcode =", retcode, ")"

    def initialize_ssh_socket(self, no_copy=False):
        if no_copy:
            self.SSH_SOCKET = None
            return
        try:
            self.SSH_SOCKET = "/tmp/autocopy_copy_%d.ssh" % os.getpid()
            ssh_cmd_list = ["ssh", "-o", "ConnectTimeout=10", "-l", self.COPY_DEST_USER,
                            "-S", self.SSH_SOCKET, "-M", "-f", "-N",
                            self.COPY_DEST_HOST]
            retcode = subprocess.call(ssh_cmd_list, stderr=subprocess.STDOUT)
            if retcode:
                print >> sys.stderr, os.path.basename(__file__), ": cannot create ssh socket into", self.COPY_DEST_HOST, "( retcode =", retcode, ")"
                sys.exit(1)
        except SystemExit, se:
            self.log("Exiting with code %d" % (se.code))
            # Close the ssh socket and shutdown the query server.
            self.cleanup_ssh_socket()
            sys.exit(se.code)

    def start_copy(self, rundir, rsync=True):
        copy_cmd_list = [self.COPY_PROCESS_EXEC,
                         "--host", self.COPY_DEST_HOST,
                         "--user", self.COPY_DEST_USER,
                         "--group", self.COPY_DEST_GROUP,
                         "--dest_root", self.COPY_DEST_RUN_ROOT,
                         "--status_file", RunDir.STATUS_FILES[RunDir.STATUS_COPY_COMPLETE],
                         "--ssh_socket", self.SSH_SOCKET,
                     ]
        if rsync:
            copy_cmd_list.append("--rsync")
        
        # End command with run directory to copy.
        copy_cmd_list.append(rundir.get_path())

        # Copy the directory.
        rundir.copy_proc = subprocess.Popen(copy_cmd_list,
                                            stdout=self.LOG_FILE, stderr=subprocess.STDOUT)
        rundir.copy_start_time = datetime.datetime.now()
        rundir.copy_end_time = None

    """
###

        try:
            runinfo = RunInfo(conn=self.LIMS, run=dirname)
            self.log("Found LIMS run record for %s" % dirname)
        except:
            runinfo = None
            self.log("No LIMS run record found for %s" % dirname)

        # Check LIMS fields against new RunDir.
        if runinfo:
            #TODO
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
            lims_run_status = RunDir.STATUS_LIMS_OK

        # Save new RunDir object and its first statuses.
        new_rundir.cached_rundir_status = new_rundir.get_status()
        new_rundir.cached_lims_run_status = lims_run_status
        new_rundir.cached_lims_runinfo = runinfo
        self.active_rundirs.append(new_rundir)

        # If this run has not been copied yet...
        if new_rundir.get_status() < RunDir.STATUS_COPY_COMPLETE:

            # Log the new directory.
            self.log("Discovered new run %s (%s) " % (entry, new_rundir.get_status_string()))

            # Email out the discovery.
            self.send_email_new_rundir(new_rundir, run_root)
"""

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

#                    TODO
#                    if is_rundir_valid:
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

    def send_email_invalid_rundir(self, rundir):
        email_body = "MISSING FILES IN RUN:\t%s\n" %rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, rundir.get_root(), rundir.get_dir())
        email_subj = "Missing files in run %s" % rundir.get_dir()
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_missing_rundir(self, missing_rundir, run_root):
        email_body = "MISSING RUN:\t%s\n" % missing_rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, run_root, missing_rundir.get_dir())
        email_body += "Autocopy was tracking this run, but can no longer find it on disk."
        email_subj = "Missing Run Dir %s" % missing_rundir.get_dir()
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_check_rundir_against_lims(self, rundir, problems_found):
        email_body = "%s: Inconsistencies between run directory and LIMS\n" % rundir.get_dir()
        email_body = "Check the problems below and correct any errors in the LIMS:\n\n"
        for problem in problems_found:
            email_body += "%s\n" % problem
        email_subj = "Problems with run %s LIMS data" % rundir.get_dir()
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_new_rundir(self, new_rundir, run_root):
        email_body  = "NEW RUN:\t%s\n" % new_rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n" % (self.HOSTNAME, run_root, new_rundir.get_dir())
        if new_rundir.get_reads():
            email_body += "Read count:\t%d\n" % new_rundir.get_reads()
            email_body += "Cycles:\t\t%s\n" % " ".join(map(lambda d: str(d), new_rundir.get_cycle_list()))

            if rundir.cached_lims_run_status == RunDir.STATUS_LIMS_MISMATCH:
                email_body += "\n"
                email_body += "NOTE: Run dir fields don't match LIMS record.\n"
                email_body += "\n"
                email_body += check_lims_msg

                email_subj  = "New Run Dir (w/LIMS Mismatch) " + entry
            else:
                email_subj  = "New Run Dir " + entry

                self.send_email(self.EMAIL_TO, email_subj, email_body)
        else:
            pass
            # TODO: Otherwise, confirm that it's been flagged in the LIMS as finished.

    def send_email_copy_failed(self, rundir, retcode):
        email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "\n"
        email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "Return code:\t%d\n" % retcode
        self.send_email(self.EMAIL_TO, "ERROR COPYING Run Dir " + rundir.get_dir(), email_body)

    def send_email_start_msg(self):
        start_msg_body = "The Autocopy Daemon was started.\n\n" + self.generate_run_status_table()
        self.send_email(self.EMAIL_TO, "Daemon Started", start_msg_body)

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
            self.email_message(self.EMAIL_TO, email_subj_prefix + rundir.get_dir(), email_body)

    def examine_archiving_dirs(self):
        archiving_rundirs = self.get_archiving_dirs()
        for rundir in archiving_rundirs:

            # If we have a archive process running (and we should: each RunDir here should have one),
            #  check to see if it ended happily, and change status to ARCHIVE_COMPLETE if it did.
            if rundir.archive_proc:
                self.LOG_FILE.flush()
                retcode = rundir.archive_proc.poll()
                if retcode == 0:
                    # Archive succeeded: advance to Archive Complete.
                    rundir.status = RunDir.STATUS_ARCHIVE_COMPLETE
                    rundir.drop_status_file()
                    rundir.archive_proc = None
                    rundir.archive_end_time = datetime.datetime.now()

                    # Send an email announcing the completed run directory archive.
                    email_body  = "Run:\t\t\t%s\n" % rundir.get_dir()
                    email_body += "Archive location:\t%s/YEAR/%s\n" % (ARCH_DEST_RUN_ROOT, rundir.get_root() + "*")
                    email_body += "\n"
                    email_body += "Archive time:\t\t%s\n" % strftdelta(rundir.archive_end_time - rundir.archive_start_time)

                    email_subj_prefix = "Archived Run Dir "

                    email_message(self.EMAIL_TO, email_subj_prefix + rundir.get_dir(), email_body)

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

                self.log("Archive of", rundir.get_dir(), "failed with no archive process attached -- previously started?")

    def update_statuses(self):
        for rundir_status in reversed(active_rundirs):

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
                active_rundirs.remove(rundir_status)

                #   Delete run directory.
                #log("Deleting %s from active runs. " % rundir.get_dir())
                # PUT DELETE DIR CODE HERE.
                log("Moving archived run %s to subdirectory %s" % (rundir.get_dir(), SUBDIR_ARCHIVE))
                os.renames(rundir.get_path(),os.path.join(rundir.get_root(),SUBDIR_ARCHIVE,rundir.get_dir()))


    def query_LIMS_for_missing_runs(self):
        pass

    def initialize_run_roots(self):

        # Create and prepare run root dirs if they do not exist
        for run_root in self.RUN_ROOT_LIST:
            if not os.path.exists(run_root):
                os.makedirs(run_root, 0775)
            aborted_subdir = os.path.join(run_root, self.SUBDIR_ABORTED)
            archive_subdir = os.path.join(run_root, self.SUBDIR_ARCHIVE)
            if not os.path.exists(aborted_subdir):
                os.mkdir(aborted_subdir, 0775)
            if not os.path.exists(archive_subdir):
                os.mkdir(archive_subdir, 0775)

    def send_email(self, to, subj, body, log=True):
        # Add a prefix to the subject line, and substitute the host here for "%m".
        subj = self.EMAIL_SUBJ_PREFIX.replace("%m", self.HOSTNAME) + subj
        msg = email.mime.text.MIMEText(body)
        msg['Subject'] = subj
        msg['From'] = self.EMAIL_FROM
        if isinstance(to,basestring):
            msg['To'] = to
        else:
            msg['To'] = ','.join(to)
        if not self.NO_MAIL:
            self.smtp.sendmail(msg['From'], to, msg.as_string())
        if log:
            self.log("Sent email FROM: %s TO: %s" % (msg['From'], msg['To']))
            self.log("SUBJ: %s" % msg['Subject'])
            self.log("BODY: %s" % msg.as_string())

    def log(self, *args):
        log_text = ' '.join(args)
        log_lines = log_text.split("\n")
        for line in log_lines:
            print >> self.LOG_FILE, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), line)
        self.LOG_FILE.flush()

        """
    @classmethod
    def initialize_signals(cls):
        def sigUSR1(signum, frame):
            run_status_table = self.generate_run_status_table()
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
                self.generate_run_status_line(run_root)

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

    def generate_run_status_line(run_root):
        if self.COPY_PROCESSES == self.MAX_COPY_PROCESSES:
            ready_str = "Ready"
            newcopying_str = ""
        else:
            ready_str = "READY"
            newcopying_str = "(NEW COPYING TURNED OFF)"
        self.log("%s:" % run_root,
                 "%d Copying," % len(self.get_copying_rundirs(run_root)),
                 "%d %s," % (len(self.get_ready_rundirs(run_root)), ready_str),
                 "%d Running," % len(self.get_running_rundirs(run_root)),
                 "%d Archiving," % len(self.get_archiving_rundirs(run_root)),
                 "%d Completed," % len(self.get_completed_rundirs(run_root)),
                 "%d Aborted," % len(self.get_aborted_rundirs(run_root)),
                 "%d Failed" % len(self.get_failed_rundirs(run_root)),
                 newcopying_str)

    def generate_run_status_table(self):
        run_status_table = ""
        copying_rundirs = self.get_copying_rundirs()
        if self.COPY_PROCESSES == self.MAX_COPY_PROCESSES:
            run_status_table += "COPYING DIRECTORIES:\n"
        else:
            run_status_table += "COPYING DIRECTORIES: (New copying turned off)\n"
        run_status_table += "-------------------\n"
        if len(copying_rundirs):
            for rundir in copying_rundirs:
                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
        else:
            run_status_table += "None\n"

        ready_rundirs = self.get_ready_rundirs()
        run_status_table += "\n"
        run_status_table += "READY DIRECTORIES:\n"
        run_status_table += "-----------------\n"
        if len(ready_rundirs):
            for rundir in ready_rundirs:
                rundir.update_status()
                run_status_table += "%s %s (%s)\n" % (rundir.get_dir().ljust(32), rundir.get_root(), rundir.get_status_string())
        else:
            run_status_table += "None\n"

        running_rundirs = self.get_running_rundirs()
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

        archiving_rundirs = self.get_archiving_rundirs()
        run_status_table += "\n"
        run_status_table += "ARCHIVING DIRECTORIES:\n"
        run_status_table += "---------------------\n"
        if len(archiving_rundirs):
            for rundir in archiving_rundirs:
                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
        else:
            run_status_table += "None\n"

        completed_rundirs = self.get_completed_rundirs()
        run_status_table += "\n"
        run_status_table += "COMPLETED DIRECTORIES:\n"
        run_status_table += "---------------------\n"
        if len(completed_rundirs):
            for rundir in completed_rundirs:
                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
        else:
            run_status_table += "None\n"

        aborted_rundirs = self.get_aborted_rundirs()
        run_status_table += "\n"
        run_status_table += "ABORTED DIRECTORIES:\n"
        run_status_table += "-------------------\n"
        if len(aborted_rundirs):
            for rundir in aborted_rundirs:
                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
        else:
            run_status_table += "None\n"

        failed_rundirs = self.get_failed_rundirs()
        run_status_table += "\n"
        run_status_table += "FAILED DIRECTORIES:\n"
        run_status_table += "------------------\n"
        if len(failed_rundirs):
            for rundir in failed_rundirs:
                run_status_table += "%s %s\n" % (rundir.get_dir().ljust(32), rundir.get_root())
        else:
            run_status_table += "None\n"

        return run_status_table

    def get_rundir(self, run_root=None, dirname=None, rundir_status=None):
        rundirs = self.get_rundirs(run_root=run_root, dirname=dirname)
        if len(rundirs) == 0:
            return None
        if len(rundirs) > 1:
            raise Exception("More than one matching rundir with run_root=%s, dirname=%s. These all matched: %s" 
                            % (run_root, dirname, rundirs))
        return rundirs[0]

    def get_rundirs(self, run_root=None, dirname=None, rundir_status=None):
        rundirs = self.rundirs_monitored
        if rundir_status is not None:
            rundirs = filter(lambda rundir: rundir.get_status() == rundir_status, rundirs)
        if dirname is not None:
            rundirs = filter(lambda rundir: rundir.get_dir() == dirname, rundirs)
        if run_root is not None:
            rundirs = filter(lambda rundir: rundir.get_root() == run_root, rundirs)
        return rundirs

    def get_ready_for_copy_rundirs(self, run_root=None):
        rundirs = self.get_rundirs(run_root=run_root)
        rundirs = filter(lambda rundir: rundir.is_finished(), rundirs)
        return rundirs

    def get_copying_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_COPY_STARTED, run_root=run_root)

    def get_running_rundirs(self, run_root=None):
        rundirs = self.get_rundirs(run_root=run_root)
        rundirs =  filter((lambda rundir: rundir.status < RunDir.STATUS_COPY_STARTED and
                           not rundir.is_finished() ), rundirs)
        return rundirs

    def get_completed_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_COPY_COMPLETE, run_root=run_root)

    def get_aborted_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_RUN_ABORTED, run_root=run_root)

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
        parser.add_option("-c", "--no_lims", dest="no_lims", action="store_true",
                          default=False,
                          help="don't query or write info to the LIMS [default = allow copies]")

        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__=='__main__':

    (opts, args) = AutocopyRundir.parse_args()
    autocopy = AutocopyRundir(run_root_list=args, no_copy=opts.no_copy, log_file=opts.log_file)
    autocopy.run()

