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
#    1. Keys must be configured to allow passwordless ssh to DEST_HOST
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
#   3. Emails should be explicit about the action required by the operator
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
#         for deletion
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

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from bin.rundir import RunDir
from bin import rundir_utils
from scgpm_lims import Connection
from scgpm_lims import RunInfo

class ValidationError(Exception):
    pass

class Autocopy:

    LOG_DIR_DEFAULT = '/var/log'

    SUBDIR_COMPLETED = "CopyCompleted" # Runs are moved here after copy
    SUBDIR_ABORTED = "RunAborted" # Runs are moved here if flagged 'sequencing_failed'

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

    LOOP_DELAY_SECONDS = 600

    # Set the copy executable and add the directory of this script to its path.
    COPY_PROCESS_EXEC_FILENAME = "copy_rundir.py"
    COPY_PROCESS_EXEC_COMMAND = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC_FILENAME)

    def __init__(self, run_root_dirs = None, log_file=None, no_copy=False, no_lims=False, no_email=False, test_mode_lims=False, config=None):
        self.initialize_config(config)
        self.initialize_no_copy_option(no_copy)
        self.initialize_hostname()
        self.initialize_log_file(log_file)
        self.initialize_lims_connection(test_mode_lims)
        self.initialize_mail_server(no_email)
        self.initialize_run_roots(run_root_dirs)
        self.initialize_ssh_socket(no_copy)
        self.redirect_stdout_stderr_to_log()

    def __del__(self): 
        self.cleanup_ssh_socket()
        self.restore_stdout_stderr()

    def run(self):
        self.send_email_start_msg()

        try:
            while True:
                self._main()
                time.sleep(self.LOOP_DELAY_SECONDS)

        except Exception, e:
            self.send_email_autocopy_crashed(e)
            raise e

    def _main(self):
        self.scan_run_roots_to_discover_rundirs()

        for rundir in self.rundirs_monitored:
            rundir.update_status(lims_runinfo)
            lims_runinfo = self.get_runinfo_from_lims(rundir)
            if self.is_aborted(lims_runinfo):
                self.process_aborted_dir(rundir)
            if self.is_ready_for_copy(rundir):
                self.process_ready_for_copy_dir(rundir)
            if self.is_copying(rundir, lims_runinfo):
                self.process_copying_dir(rundir)
            if self.is_completed(rundir):
                self.process_completed_dir(rundir)

            #TODO Daily email with status of all runs

    def process_ready_for_copy_dir(self, rundir, lims_runinfo):
        self.start_copy(rundir)
        rundir.set_status(RunDir.STATUS_COPY_STARTED)
        lims_runinfo.set_run_status(lims_runinfo.DONE)

    def process_copying_dir(self, rundir, lims_runinfo):
        if not rundir.copy_proc:
            # Indicates that a new RunDir was already in a Copy Started state
            # Remove COPY_STARTED status file so copy can restart.
            rundir.reset_to_copy_not_started()
            self.log(rundir.get_dir(), "failed to copy and had no copy process attached")
            self.log("Usually this is because Autocopy restarted while a copy was in progress")
            self.log("Unsetting COPY_STARTED status to re-attempt copy.")
        else:
            # Check if the copy process finished successfully
            retcode = rundir.copy_proc.poll()
            if retcode == 0:
                are_files_missing = self.are_files_missing(rundir)
                lims_problems = self.check_rundir_against_lims(rundir, lims_runinfo)
                disk_usage = rundir.get_disk_usage()
                #TODO send one email per run, with info re flags above
                rundir.set_status(RunDir.STATUS_COPY_COMPLETE)
                rundir.copy_proc = None
                rundir.copy_end_time = datetime.datetime.now()
            elif retcode == None:
                # Still copying. Do nothing.
                pass
            else:
                # TODO email notice that copy failed.
                # Revert status so copy can restart.
                rundir.reset_to_copy_not_started()

    def process_completed_dir(self, rundir):
        log("%s copy is complete. Moving to %s. It can be deleted." % (
            rundir.get_dir(), os.path.join(rundir.get_root(), self.SUBDIR_COMPLETED)))
        os.renames(rundir.get_path(),os.path.join(rundir.get_root(),self.SUBDIR_COMPLETED,rundir.get_dir()))
        self.rundirs_monitored.remove(rundir)
        # TODO send email notification

    def process_aborted_dir(self, rundir):
        log("%s has been flagged as Sequencing Failed. Moving to %s. It can be deleted." % (
            rundir.get_dir(), os.path.join(rundir.get_root(), self.SUBDIR_ABORTED)))
        rundir.set_status(RunDir.STATUS_RUN_ABORTED)
        os.renames(rundir.get_path(),os.path.join(rundir.get_root(),self.SUBDIR_ABORTED,rundir.get_dir()))
        self.rundirs_monitored.remove(rundir)
        # TODO send email
        # TODO
        # Set all the status flags in the LIMS for this run.                                                                                                                                        
        #        all_flags_yes_dict = {'sequencer_done': 'yes', 'analysis_done': 'yes', 'dnanexus_done': 'yes',
        # 'notification_done': 'yes', 'archiving_done': 'yes'}
        # flowcell = Done in LIMS

    def initialize_hostname(self):
        hostname = socket.gethostname()
        self.HOSTNAME = hostname[0:hostname.find('.')] # Remove domain part.

    def initialize_lims_connection(self, is_test_mode):
        self.LIMS = Connection(apiversion=self.LIMS_API_VERSION, local_only=is_test_mode)

    def initialize_mail_server(self, no_email):
        self.NO_EMAIL = no_email
        if self.NO_EMAIL:
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
            raise ValidationError('SMTP server settings are required')

        self.log("Connecting to mail server...")
        self.smtp = smtplib.SMTP(smtp_server, smtp_port, timeout=5)
        self.smtp.login(smtp_username, smtp_token)
        self.log("success.")

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
        retcode = subprocess.call(["ssh", "-O", "exit", "-S", self.SSH_SOCKET, self.COPY_DEST_HOST],
                                  stdout=self.LOG_FILE, stderr=subprocess.STDOUT)
        if retcode:
            print >> sys.stderr, os.path.basename(__file__), ": cannot close ssh socket into", self.COPY_DEST_HOST, "( retcode =", retcode, ")"

    def leave_ok_to_delete_readme(self, directory):
        readme = os.path.join(directory, 'README.txt')
        if not os.path.exists(readme):
            with open(readme, 'w') as f:
                f.write('Runs in this directory are generally OK to delete.')

    # sync rundirs_monitored with what's on disk
    def scan_run_roots_to_discover_rundirs(self):
        if not hasattr(self, 'rundirs_monitored'):
            # Initialize this instance var once after startup
            self.rundirs_monitored = []

        new_rundirs_monitored = []
        for run_root in self.RUN_ROOT_DIRS:
            for dirname in os.listdir(run_root): # Directories on disk
                # Include only directories that begin with a 6-digit start date
                # Exclude special subdirs
                if (os.path.isdir(os.path.join(run_root, dirname)) and
                    re.match("\d{6}_", dirname) and
                    dirname not in [self.SUBDIR_COMPLETED, self.SUBDIR_ABORTED]):
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
            return RunDir(run_root, dirname)

    def add_to_rundirs_monitored(self, run_root, dirname):
        self.rundirs_monitored.append(RunDir(run_root, dirname))

    def are_files_missing(self, rundir):
        # Check that the run directory has all the right files.
        files_missing = not rundir_utils.validate(rundir)
        return files_missing

    def get_runinfo_from_lims(self, rundir):
        try:
            runinfo = RunInfo(conn=self.LIMS, run=rundir.get_dir())
        except:
            runinfo = None
        return runinfo

    def check_rundir_against_lims(self, rundir, runinfo, testproblem=None):
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

    def start_copy(self, rundir, rsync=True):
        copy_cmd_list = [self.COPY_PROCESS_EXEC_COMMAND,
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

    def send_email_autocopy_crashed(self, exception):
        tb = traceback.format_exc(exception)
        email_subj = "Autocopy crashed"
        email_body = "The autocopy daemon crashed with Exception\n" + tb
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_invalid_rundir(self, rundir):
        email_subj = "Missing files in run %s" % rundir.get_dir()
        email_body = "MISSING FILES IN RUN:\t%s\n" %rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, rundir.get_root(), rundir.get_dir())
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_missing_rundir(self, missing_rundir, run_root):
        email_subj = "Missing Run Dir %s" % missing_rundir.get_dir()
        email_body = "MISSING RUN:\t%s\n" % missing_rundir.get_dir()
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, run_root, missing_rundir.get_dir())
        email_body += "Autocopy was tracking this run, but can no longer find it on disk."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_check_rundir_against_lims(self, rundir, problems_found):
        email_subj = "Problems with run %s LIMS data" % rundir.get_dir()
        email_body = "%s: Inconsistencies between run directory and LIMS\n" % rundir.get_dir()
        email_body = "Check the problems below and correct any errors in the LIMS:\n\n"
        for problem in problems_found:
            email_body += "%s\n" % problem
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
        email_subj = "ERROR COPYING Run Dir " + rundir.get_dir()
        email_body = "Run:\t\t\t%s\n" % rundir.get_dir()
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundir.get_path())
        email_body += "\n"
        email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundir.get_dir())
        email_body += "Return code:\t%d\n" % retcode
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_start_msg(self):
        email_subj = "Daemon Started"
        email_body = "The Autocopy Daemon was started.\n\n" + self.generate_run_status_table()
        self.send_email(self.EMAIL_TO, email_subj, email_body)

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

    def send_email(self, to, subj, body, write_email_to_log=True):
        subj_prefix = "AUTOCOPY (%s): " % self.HOSTNAME
        msg = email.mime.text.MIMEText(body)
        msg['Subject'] = subj_prefix + subj
        msg['From'] = self.EMAIL_FROM
        if isinstance(to,list):
            msg['To'] = ','.join(to)
        else:
            msg['To'] = to
        if not self.NO_EMAIL:
            self.smtp.sendmail(msg['From'], to, msg.as_string())
        if write_email_to_log:
            self.log("Sent email FROM: %s TO: %s" % (msg['From'], msg['To']))
            self.log("SUBJ: %s" % msg['Subject'])
            self.log("BODY: %s" % msg.as_string())

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

    def redirect_stdout_stderr_to_log(self):
        self.STDOUT_RESTORE = sys.stdout
        self.STDERR_RESTORE = sys.stderr
        sys.stdout = self.LOG_FILE
        sys.stderr = self.LOG_FILE

    def restore_stdout_stderr(self):
        if hasattr(self, 'STDOUT_RESTORE'):
            sys.stdout = self.STDOUT_RESTORE
        if hasattr(self, 'STDERR_RESTORE'):
            sys.stderr = self.STDERR_RESTORE

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
            for run_root in run_root_dirs:
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

    def is_ready_for_copy(self, rundir):
        return rundir.is_finished()

    def get_copying_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_COPY_STARTED, run_root=run_root)

    def is_copying(self, rundir):
        return rundir.get_status() == RunDir.STATUS_COPY_STARTED

    def get_running_rundirs(self, run_root=None):
        rundirs = self.get_rundirs(run_root=run_root)
        rundirs =  filter((lambda rundir: rundir.status < RunDir.STATUS_COPY_STARTED and
                           not rundir.is_finished() ), rundirs)
        return rundirs

    def get_completed_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_COPY_COMPLETE, run_root=run_root)

    def is_completed(self, rundir):
        return rundir.get_status() == RunDir.STATUS_COPY_COMPLETE

    def get_aborted_rundirs(self, run_root=None):
        return self.get_rundirs(rundir_status=RunDir.STATUS_RUN_ABORTED, run_root=run_root)

    def is_aborted(self, lims_runinfo):
        return lims_runinfo.get_run_status() == lims_runinfo.SEQUENCING_RUN_STATUS_FAILED

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
        parser.add_option("-m", "--no_lims", dest="no_lims", action="store_true",
                          default=False,
                          help="don't query or write info to the LIMS [default = allow copies]")
        parser.add_option("-e", "--no_email", dest="no_email", action="store_true",
                          default=False,
                          help="don't send email [default = send email]")
        parser.add_option("-d", "--dry_run", dest="dry_run", action="store_true",
                          default=False,
                          help="same as --no_copy --no_lims --no_email [default = live run]")
        parser.add_option("-g", "--config", dest="config_file", type="string",
                          default=None,
                          help='config file in JSON format to override default settings')
        parser.add_option("-t", "--test_mode_lims", dest="test_mode_lims", action="store_true", default=False,
                          help="instead of a real LIMS connection, connect to LIMS test data")

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

    autocopy = Autocopy(run_root_dirs=args, no_copy=no_copy, no_email=no_email, no_lims=no_lims, log_file=opts.log_file, 
                        config=config, test_mode_lims=opts.test_mode_lims)
    autocopy.run()
