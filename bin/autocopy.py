#!/usr/bin/env python

###############################################################################
#
# autocopy.py - Copy sequencing run directories from a local data store
#   to a remote server
#
# autocopy.py -h for help
#
# AUTHORS:
#   Keith Bettinger, Nathan Hammond, Nathaniel Watson, Paul Billing-Ross
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
#    4. Set the env variables described below for LIMS access and SMTP
#       mail server access:
#         UHTS_LIMS_URL, UHTS_LIMS_TOKEN
#         AUTOCOPY_SMTP_SERVER, AUTOCOPY_SMTP_PORT, 
#         (optional: AUTOCOPY_SMTP_USERNAME, AUTOCOPY_SMTP_TOKEN)
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
#      b. Communication with the mail server via HTTPS. 
#         Uses these env variables:
#           AUTOCOPY_SMTP_USERNAME, AUTOCOPY_SMTP_TOKEN, 
#           AUTOCOPY_SMTP_PORT, AUTOCOPY_SMTP_SERVER
#      c. SSH copy
#         An ssh port to the destination cluster is opened on startup, used by rsync
#         for copying data to the cluster. The following settings control the copy step
#         and can be set in a config.json file:
#           COPY_DEST_HOST, COPY_DEST_USER, COPY_DEST_GROUP, COPY_DEST_RUN_ROOT
#
# Warning re aborted runs
#   1. If SolexaRun.sequencing_status is set to 'sequencing failed' in the LIMS,
#      autocopy will discard it by moving it to the Runs_Aborted subdirectory.
#   2. If SolexaRun.sequencing_status is set to 'sequencing exception', autocopy
#      will ignore it and proceed as usual.

import os
import re
import sys
import grp
import pwd
import json
import time
import signal
import socket
import smtplib
import requests
import datetime
import requests
import threading
import traceback
import subprocess
import email.mime.text
from optparse import OptionParser

import dxpy
import fnmatch
from distutils.version import StrictVersion

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from bin.rundir import RunDir
from bin import rundir_utils

from scgpm_lims import Connection
from scgpm_lims import RunInfo, SolexaRun, SolexaFlowCell

class ValidationError(Exception):
    pass

class DNAnexusUpload:

    def __init__(self, rundir, tar_dir, LOG_FILE, initiate_analysis_script, lims_url, 
                 lims_token, test, upload_mode, viewers, contributors, administrators, dx_env_config, 
                 dx_workflow_config_dir, release, develop, region, upload_agent, ua_token):
        self.rundir = rundir            # RunDir object
        self.tar_dir = tar_dir
        self.LOG_FILE = LOG_FILE
        self.initiate_analysis_script = initiate_analysis_script
        self.lims_url = lims_url
        self.lims_token = lims_token
        self.test = test
        self.upload_mode = upload_mode  # ['API', 'UploadAgent']
        self.viewers = viewers
        self.contributors = contributors
        self.administrators = administrators
        self.dx_workflow_config_dir = dx_workflow_config_dir
        self.dx_env_config = dx_env_config
	self.release = release
        self.develop = develop
        self.region = region
        self.upload_agent = upload_agent
        self.ua_token = ua_token

        self.project_dxid = None
        self.interop_tar = None
        self.metadata_tar = None
	#self.thumbnails_tar = None
        self.lane_tar_files = None
        self.rta_version = None
        self.file_dxids = {}

        dxpy.set_security_context({"auth_token_type": "bearer", "auth_token": self.ua_token})

    def run(self):
        if not self.rta_version:
            self.rta_version = self.get_rta_version()

        # Tar interop and metadata files
        self.interop_tar = self.tar_interop_dir()

        # Tar metadata files
        print 'RTA version is: %s' % self.rta_version
        if StrictVersion(self.rta_version) < StrictVersion('2.0.0'):
            print 'Tarring metadata according to RTA v1 pattern'
            #print 'ERROR: workflow does not support RTA versions older than 1.18.54'
            #sys.exit()
            self.metadata_tar = self.tar_rta_v1_metadata()
        elif StrictVersion(self.rta_version) >= StrictVersion('2.0.0'):
            print 'Tarring metadata files according to RTA v2 pattern'
            self.metadata_tar = self.tar_rta_v2_metadata()
        else:
            print 'Error: Could not figure out how to tar metadata'
            sys.exit()
        print 'Getting lane paths'
        self.lane_paths = self.get_lane_paths()  # self.lane_tar_files["L001"] = lane_dir_pathi

        # Tar metadata files
        #print 'RTA version is: %s' % self.rta_version
        #self.metadata_tar = self.tar_rta_v2_metadata()
        #self.lane_paths = self.get_lane_paths()  # self.lane_tar_files["L001"] = lane_dir_path

        # Tars all lanes before uploading files
        for lane_name in self.lane_paths:
            lane_index = int(lane_name[-1:])
            #lane_tar = self.tar_rta_v2_lane_path(lane_name, lane_index)
            if StrictVersion(self.rta_version) < StrictVersion('2.0.0'):
                lane_tar = self.tar_rta_v1_lane_path(lane_name, lane_index)
            elif StrictVersion(self.rta_version) >= StrictVersion('2.0.0'):
                print 'Tarring lane %d according to RTA v2 pattern' % lane_index
                lane_tar = self.tar_rta_v2_lane_path(lane_name, lane_index)

            self.project_dxid = self.get_dnanexus_project(lane_index)
            dxids = self.upload_lane(lane_index=lane_index, lane_tar=lane_tar)
            self.file_dxids[lane_index] = dxids
            self.call_initiate_analysis(lane_index)

    def get_rta_version(self):
        params_file = os.path.join(self.rundir.get_path(), 'runParameters.xml')
        with open(params_file, 'r') as PARAM:
            for line in PARAM:
                match = re.search(r'<RTAVersion>([\d\.]+)</RTAVersion>', line)
                if match:
                    rta_version = match.group(1)
                    break
        return rta_version
 
    def tar_interop_dir(self):
        ''' Description: tar and upload InterOp directory to lane DNAnexus project
        '''
        tar_name = '%s.InterOp.tar' % self.rundir.get_dir()
        tar_path = os.path.join(self.tar_dir, tar_name)
        
        if os.path.isfile(tar_path):
            return tar_path

        interop_tar_list = ['tar', '-C', self.rundir.get_path(), 
                                '-cvf', tar_path, 'InterOp', 'runParameters.xml', 'RunInfo.xml'
                            ]
        interop_tar_proc = subprocess.call(interop_tar_list, stdout=self.LOG_FILE, 
                                            stderr=self.LOG_FILE
                       )
        # Check that InterOp tar file exists
        return tar_path

    def tar_rta_v1_metadata(self):
        ''' Description:
        '''

        tar_name = '%s.metadata.tar' % self.rundir.get_dir() # get_dir() = basename/run name
        tar_path = os.path.join(self.tar_dir, tar_name)

        if os.path.isfile(tar_path):
                return tar_path

        meta_tar_list = ['tar', '-C', '%s' % self.rundir.get_path(),
                         '-cvf', '%s' % tar_path,
                         'runParameters.xml', 'RunInfo.xml',
                         'Data/RTALogs', 'Data/Intensities/config.xml', 'Data/Intensities/BaseCalls/config.xml',
                         'RTAComplete.txt',
                         'Data/Intensities/RTAConfiguration.xml', 'Data/Intensities/config.xml',
                         'Data/Intensities/Offsets',
                         'Recipe', 'Config']

        meta_tar_proc = subprocess.call(meta_tar_list, stdout=self.LOG_FILE,
                                         stderr=self.LOG_FILE)
        return tar_path

    def tar_rta_v2_metadata(self):
        ''' DEV: Change this to mirror formatting of tar_rta_v1_metadata method and specify files/dirs
        Description:
        '''

        tar_name = '%s.metadata.tar' % self.rundir.get_dir() # get_dir() = basename/run name
        tar_path = os.path.join(self.tar_dir, tar_name)

        if os.path.isfile(tar_path):
                return tar_path

        meta_tar_list = ['tar', '-C', '%s' % self.rundir.get_path(),
                         '-cvf', '%s' % tar_path,
                         'runParameters.xml', 'RunInfo.xml', 'RTAConfiguration.xml',
                         'RTALogs',
                         'RTAComplete.txt',
                         'Recipe', 'Config',
             		 'Data/Intensities/s.locs'
           	 	]
        
        meta_tar_proc = subprocess.call(meta_tar_list, stdout=self.LOG_FILE,
                                         stderr=self.LOG_FILE)
        return tar_path
 
    def get_lane_paths(self):

        lane_paths = {}
        basecalls_dir = os.path.join(self.rundir.get_path(), 'Data', 'Intensities', 'BaseCalls')

        for filename in os.listdir(basecalls_dir):
            if fnmatch.fnmatch(filename, 'L0*'):
                lane_name = filename
                lane_path = os.path.join(self.rundir.get_path(), lane_name)
                lane_paths[lane_name] = lane_path
        return lane_paths

    def tar_rta_v1_lane_path(self, lane_name, lane_index):

        tar_name = '%s_L%d.tar' % (self.rundir.get_dir(), lane_index)
        tar_path = os.path.join(self.tar_dir, tar_name)
        if os.path.isfile(tar_path):
            pass
        else:
            intens_rel_path = os.path.join('Data', 'Intensities', lane_name)
            basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
            lane_tar_list = ['tar', '-C', self.rundir.get_path(),
                             '-cf', tar_path, intens_rel_path, basecall_rel_path
                            ]
            lane_tar_proc = subprocess.call(lane_tar_list, stdout=self.LOG_FILE,
                                                         stderr=self.LOG_FILE)
        return tar_path


    def tar_rta_v2_lane_path(self, lane_name, lane_index):

        tar_name = '%s_L%d.tar' % (self.rundir.get_dir(), lane_index)
        tar_path = os.path.join(self.tar_dir, tar_name)
        if os.path.isfile(tar_path):
                pass
        else :
                basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
                lane_tar_list = ['tar', '-C', self.rundir.get_path(),
                                 '-cf', tar_path, basecall_rel_path
                                ]
                lane_tar_proc = subprocess.call(lane_tar_list, stdout=self.LOG_FILE,
                                                 stderr=self.LOG_FILE)
        return tar_path

    def upload_file(self, file_path, class_name, project_dxid, folder): 
        upload_file_dxid = None
        file_basename = os.path.basename(file_path)
        file_glob_name = file_basename + '*'
	print 'Info: Checking upload status of file: %s' % file_path
        # Find any existing copies of this file on DNAnexus
        dxfile_generator = dxpy.find_data_objects(
                                                  classname = class_name, 
                                                  name = file_glob_name,
                                                  name_mode = 'glob',
                                                  project = project_dxid,
                                                  folder = folder,
                                                  properties = {'upload_complete': True})

        # Determine whether they are complete file based on 'upload_complete' boolean flag
        dxfiles = list(dxfile_generator)
        if len(dxfiles) == 0:
            print 'Info: Did not find any existing %s files on DNAnexus' % file_basename
        for dxfile_dict in dxfiles:
            print 'Info: Found existing file %s on DNAnexus' % file_basename
            dxfile = dxpy.DXFile(dxid=dxfile_dict['id'], project=dxfile_dict['project'])
            properties = dxfile.get_properties()
            if properties['upload_complete'] == 'false':
                print 'Warning: Incomplete upload of file %s found; removing' % file_basename
                dxfile.remove()
            elif properties['upload_complete'] == 'true':
                print 'Info: File %s completed upload; skipping upload' % file_basename
                upload_file_dxid = dxfile.get_id()
            else:
                print 'Error: Unable to determine upload status of %s' % file_basename
                sys.exit()
        # If complete copy of file does not exist on DNAnexus, upload it
        
        if not upload_file_dxid:
            if self.upload_mode == 'API':
                print 'Uploading file %s to DNAnexus' % file_basename
                upload_file_dxfile = dxpy.upload_local_file(
                                                            filename = file_path,
                                                    	    project = project_dxid,
                                                	    folder = folder,
                                            		    properties = {'upload_complete': 'false'},
                                            		    parents = True)
      		upload_file_dxid = upload_file_dxfile.get_id()
           	upload_file_dxfile.set_properties(properties = {'upload_complete': 'true'})
            
            elif self.upload_mode == 'UploadAgent':
		
                if self.develop:
                    log_file = '/seqctr/Runs/DNAnexus_Logs/dev_%s.ua.log' % file_basename
                else:
                    log_file = '/seqctr/Runs/DNAnexus_Logs/%s.ua.log' % file_basename

                command = '%s ' % self.upload_agent
                command += '--auth-token %s ' % self.ua_token
                command += '--project %s ' % project_dxid
                command += '--folder /raw_data '
                command += '--progress '                # output progress to stderr
                command += '--read-threads 2 '          # default == 2
                #command += '--compress-threads 8 '       # default == 1
                command += '--upload-threads 200 '        # default == 8
                command += '--chunk-size 40M '          # default == 75M
                command += '--do-not-compress '
		command += '--no-round-robin-dns '
		command += '%s '	% file_path 
                command += '&> %s' % log_file

                self.createSubprocess(cmd=command)
		upload_file_dxid = None
		with open(log_file, 'r') as LOG:
			for line in LOG:
				match = re.search(r'(^file-\w+)', line)
				if match:
					upload_file_dxid = match.group(1)
					break
                		else:
					pass
		if upload_file_dxid == None:
			print 'Error: Could not determine file dxid from log: %s' % log_file
			sys.exit()
		else:
			upload_file_dxfile = dxpy.DXFile(dxid=upload_file_dxid, project=project_dxid)
			upload_file_dxfile.set_properties(properties = {'upload_complete': 'true'})
        return upload_file_dxid
    
    def upload_lane(self, lane_index, lane_tar):
        interop_dxid = self.upload_file(file_path = self.interop_tar,
                                        class_name = 'file', 
                                        project_dxid = self.project_dxid, 
                                        folder = '/raw_data')
        metadata_dxid = self.upload_file(file_path = self.metadata_tar, 
                                         class_name = 'file', 
                                         project_dxid = self.project_dxid, 
                                         folder = '/raw_data')
        lane_dxid = self.upload_file(file_path = lane_tar, 
                                     class_name = 'file',
                                     project_dxid = self.project_dxid, 
                                     folder='/raw_data')
        return [interop_dxid, metadata_dxid, lane_dxid]

    def get_dnanexus_project(self, lane_index, contains_phi=False):
        if self.develop:
            project_name = 'dev_%s_L%d' % (self.rundir.get_dir(), lane_index)
        else:
            project_name = '%s_L%d' % (self.rundir.get_dir(), lane_index)

        properties = {
                      'seq_lane_name': '%s_L%d' % (self.rundir.get_dir(), lane_index),
                      'seq_run_name': '%s' % (self.rundir.get_dir()),
                      'seq_lane_index': '%d' % lane_index,
                     }
        if self.develop:
            properties['production'] = 'false'
        else:
            properties['production'] = 'true'

        # Check whether project already exists
        '''
        project_generator = dxpy.find_projects(
                                               name = project_name, 
                                               name_mode = 'exact',
                                               region = self.region)
        '''
        projects_dict = dxpy.api.system_find_projects({
                                                       'name': project_name,
                                                       'region': 'azure:westus'
                                                      })
        projects = projects_dict['results']
        #projects = list(project_generator)
        if len(projects) == 1:
            # Find existing DNAnexus project
            project = projects[0]
            project_dxid = project['id']
        elif len(projects) == 0:
            # Create new DNAnexus project
            input_params = {'name': project_name,
                            'containsPHI': contains_phi,
                            'properties': properties,
                            'region': self.region
                           }
            project_dxid = dxpy.api.project_new(input_params=input_params)['id']
            # Add project viewers & contributors
            for viewer in self.viewers:
                user_id = 'user-%s' % viewer
                input_params = {
                                'invitee': user_id, 
                                'level': 'VIEW',
                                'suppressEmailNotification': False
                               }
                dxpy.api.project_invite(object_id=project_dxid, input_params=input_params)
            for contributor in self.contributors:
                user_id = 'user-%s' % contributor
                input_params = {
                                'invitee': user_id, 
                                'level': 'CONTRIBUTE',
                                'suppressEmailNotification': False
                               }
            for admin in self.administrators:
                user_id = 'user-%s' % admin
                input_params = {
                                'invitee': user_id, 
                                'level': 'ADMINISTER',
                                'suppressEmailNotification': False
                               }
                dxpy.api.project_invite(object_id=project_dxid, input_params=input_params)
        elif len(projects) > 1:
            # DEV: Change to STDERR message
            print('Warning: multiple DNAnexus projects matching name ' + 
                  '%s ' % project_name +
                  'were found. Using the first.')
            project = projects[0]
            project_dxid = project['id']
        else:
            print('Error: an unexpected error arose when trying to create a ' +
                  'new DNAnexus project of name %s' % project_name)
            sys.exit()
        return project_dxid

    def call_initiate_analysis(self, lane_index):
        # Initiate analysis
        print 'Info: Initiating analysis for %s L%d' % (self.rundir.get_dir(), int(lane_index))
        # List must contain only strings
        analysis_list = [self.initiate_analysis_script, 
                 '-n', self.rundir.get_dir(),
                 '-l', str(lane_index),
                 '-p', self.project_dxid,
                 '-r', self.rta_version,
                 '-u', self.lims_url,
                 '-o', self.lims_token,
                 '-t', str(self.test),
                 '-v', str(self.dx_env_config),
                 '-w', str(self.dx_workflow_config_dir)
                ]
        # Optional arguments
	if self.release:
	    analysis_list.append('-e')
        if self.develop:
            analysis_list.append('-d')
        print analysis_list
        analysis_str = ' '.join(str(element) for element in analysis_list)
        print analysis_str
        analysis_proc = subprocess.Popen(analysis_str, stdout=self.LOG_FILE, stderr=self.LOG_FILE, shell=True) 

    def createSubprocess(self, cmd, pipeStdout=False, checkRetcode=True):
        """
        Function: Creates a subprocess via a call to subprocess.Popen with the argument 
                  'shell=True', and pipes stdout and stderr. Stderr is always  piped, but 
                  stdout can be turned off. If the argument checkRetcode is True, which 
                  it is by default, then for any non-zero return code, an Exception is
                  raised that will print out the the command, stdout, stderr, and the 
                  returncode when not caught. Otherwise, the Popen instance will be 
                  return, in which case the caller must call the instance's communicate()
                  method (and not it's wait() method!) in order to get the return code to 
                  see if the command was a success. communicate() will return a tuple 
                  containing (stdout, stderr). But at that point, you can then check 
                  the return code with Popen instance's 'returncode' attribute.
        Args    : cmd (str): Command line argument for the subprocess wrapped in the 
                    subprocess.Popen instance. Will be printed to stdout when 
                    there is an error in the subprocess.
                  pipeStdout (bool): If 'True' pipe stdout of the subprocess.
                  checkRetcode (bool): See documentation above.
        Returns : A two-item tuple containing stdout and stderr, respectively.
        """
        stdout = None
        if pipeStdout:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE
        popen = subprocess.Popen(cmd,shell=True,stdout=stdout,stderr=subprocess.PIPE)
        if checkRetcode:
            stdout,stderr = popen.communicate()
            if not stdout: #will be None if not piped
                stdout = ""
            stdout = stdout.strip()
            stderr = stderr.strip()
            retcode = popen.returncode
            if retcode:
                #below, I'd like to raise a subprocess.SubprocessError, but that doens't exist until Python 3.3.
                raise Exception("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is: '{stdout}'.\n\nstderr is: '{stderr}'.".format(cmd=cmd,returncode=retcode,stdout=stdout,stderr=stderr))
            return stdout,stderr
        else:
            return popen

class Autocopy:

    RUNDIR_REG = re.compile(r'^\d{6}_')

    LOG_DIR_DEFAULT = '/var/log'

    SUBDIR_COMPLETED = "DNAnexus_Runs_Completed" # Runs are moved here after copy
    SUBDIR_ABORTED = "DNAnexus_Runs_Aborted" # Runs are moved here if flagged 'sequencing_failed'

    LIMS_API_VERSION = 'v1'

    MAX_COPY_PROCESSES = 1 # Cap the number of copy procs
                           # if --no_copy, this is set to 0.
    EMAIL_TO = None
    EMAIL_FROM = None

    AUTOCOPY_SMTP_SERVER = None
    AUTOCOPY_SMTP_PORT = None
    AUTOCOPY_SMTP_USERNAME = None
    AUTOCOPY_SMTP_TOKEN = None

    UHTS_LIMS_URL = None
    UHTS_LIMS_TOKEN = None

    # Where to copy the run directories
    COPY_DEST_HOST  = 'localhost'
    COPY_DEST_USER  = pwd.getpwuid(os.getuid()).pw_name
    COPY_DEST_GROUP = grp.getgrgid(pwd.getpwuid(os.getuid()).pw_gid).gr_name
    COPY_SOURCE_RUN_ROOTS = [os.getcwd()]
    COPY_DEST_RUN_ROOT = '~/copied_runs'

    # Powers of two constants
    ONEKILO = 1024.0
    ONEMEG  = ONEKILO * ONEKILO
    ONEGIG  = ONEKILO * ONEMEG
    ONETERA = ONEKILO * ONEGIG

    MIN_FREE_SPACE = ONETERA * 2 # Warn when run_root space is below this value

    MAIN_LOOP_DELAY_SECONDS = 600
    RUNROOT_FREESPACE_CHECK_DELAY_SECONDS = 3600
    RUNDIRS_MONITORED_SUMMARY_DELAY_SECONDS = 3600*24
    SECONDS_BEFORE_COPY_RESTART = 3600*24

    last_runroot_freespace_check = None
    last_rundirs_monitored_summary = None

    # Set the copy executable and add the directory of this script to its path.
    COPY_PROCESS_EXEC_FILENAME = "copy_rundir.py"
    COPY_PROCESS_EXEC_COMMAND = os.path.join(os.path.dirname(__file__), COPY_PROCESS_EXEC_FILENAME)

    def __init__(self, dnanexus, upload_mode, release, develop,
                 log_file=None, no_copy=False, no_lims=False, no_email=False, 
                 test_mode_lims=False, config=None, errors_to_terminal=False):

        self.dnanexus = dnanexus        # Boolean flag
        self.upload_mode = upload_mode  # ['API', 'UploadAgent']
	self.release = release
        self.develop = develop
        
        print 'Initializing config'
        self.initialize_config(config)
        print 'Initializing log file'
        self.initialize_log_file(log_file)
        print 'Log starting autocopy message'
        self.log_starting_autocopy_message()
        print 'Initializ no copy option'
        self.initialize_no_copy_option(no_copy)
        print 'Initialize hostname'
        self.initialize_hostname()
        print 'Initialize LIMS connection'
        self.initialize_lims_connection(test_mode_lims, no_lims)
        print 'Initialize mail server'
        self.initialize_mail_server(no_email)
        print 'Initialize run roots'
        self.initialize_run_roots()
        print 'Initialize signals'
        self.initialize_signals()
        print 'Redirect output to log'
        self.redirect_stdout_stderr_to_log(errors_to_terminal)
        print 'Init complete'


    def cleanup(self):
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
            self.log_sleep()
            time.sleep(self.MAIN_LOOP_DELAY_SECONDS)

    def _main(self):
        self.log_main_loop
        self.update_rundirs_monitored()
        for rundir in self.rundirs_monitored:
            self.process_rundir(rundir)

        if self.is_time_for_rundirs_monitored_summary():
            self.send_email_rundirs_monitored_summary()

        if self.is_time_for_runroot_freespace_check():
            self.check_runroot_freespace()

    def copy_processes_counter(self):
        count = 0
        for rundir in self.rundirs_monitored:
            if rundir.is_copying():
                count += 1
        return count

    def process_rundir(self, rundir):
        """
        Function : Figures out if a run is aborted, copying, or ready to be copied, then launches the next step accordingly. 
                   If the run is aborted, sends an email and moves the run directory to the aborted directory.
                   If the run is copying, sends an email, moves the run directory to the copy completed folder.
                   If the run isn't aborted or copying, then 
                   
        Args     : rundir - a rundir.RunDir object
        """
 
        self.log_processing_dir(rundir)
	print "Info: Current status = %s" % rundir.get_status()
	print "Info: Current status string = %s" % rundir.get_status_string()	
        lims_runinfo = self.get_runinfo_from_lims(rundirObject=rundir) # A scgpm_lims.components.models.RunInfo object

        if self.is_rundir_aborted(lims_runinfo):
            if rundir.is_copying():
                # Ignore "sequencing failed" flag after copy started.
                # No mechanism to clean up on the other end of the copy,
                # so just go with it.
		print "Info: %s is still copying. Skipping." % rundir.get_dir() 
                pass
            else:
                self.process_aborted_rundir(rundirObject=rundir, lims_runinfo=lims_runinfo)


        if rundir.is_copying():
            self.process_copying_rundir(rundir, lims_runinfo)
            print "Info: %s is still copying. Skipping." % rundir.get_dir()

        # process_ready_for_copy_rundir goes after process_copying_rundir
        # because when a copy process fails, process_copying_rundir resets
        # it to a ready_for_copy state, and we can start the copy process
        # in process_ready_for_copy_rundir right away.
        if self.is_rundir_ready_for_copy(rundir):
            self.process_ready_for_copy_rundir(rundir, lims_runinfo)

    def is_rundir_aborted(self, lims_runinfo):
        """
        Args  : lims_runinfo - a scgpm_lims.components.models.RunInfo object.
        """
        if lims_runinfo is None and self.LIMS: 
            return False #It could just be that the techs are late entering the run in the LIMS.
                         # lims_runinfo can be None (see get_or_create_rundir()) if the HTTP call returns a 404.
                         # In such a case, we'll let autocopy still monitor it. It just won't be copied to its final
                         # destination until its entered in UHTS.
        elif lims_runinfo is None and not self.LIMS:
            return True
        else:
            return lims_runinfo.has_status_sequencing_failed()

    def is_rundir_ready_for_copy(self, rundir):
        return rundir.is_finished() and not rundir.is_copying()

    def get_rundir_status(self, rundir):
        if rundir.is_copying():
            return "copying"
        elif self.is_rundir_ready_for_copy(rundir):
            return "ready_for_copy"
        else:
            return "not_ready"

    def process_ready_for_copy_rundir(self, rundir, lims_runinfo):
        if self.copy_processes_counter() >= self.MAX_COPY_PROCESSES:
            self.log_reached_copy_processes_max(rundir)
            return

        if not lims_runinfo:
            self.send_email_run_not_found_in_lims(rundir.get_dir())
        self.log_start_copy(rundir)
        self.start_copy(rundir, dnanexus=self.dnanexus)
        #if lims_runinfo:
        # Commented out for testing
            #lims_runinfo.set_flags_for_sequencing_finished_analysis_started()

    def process_copying_rundir(self, rundir, lims_runinfo):
        # Check if the copy process finished successfully
        retcode = rundir.copy_proc.poll()
        if retcode == 0:
            self.process_completed_rundir(rundir, lims_runinfo)
        elif retcode == None:
            if rundir.seconds_since_copy_started() > self.SECONDS_BEFORE_COPY_RESTART:
                self.restart_copy(rundir)
                self.send_email_copy_restarted(rundir.get_dir())
        else:
            self.process_failed_copy_rundir(rundir, retcode)

    def restart_copy(self, rundir):
        rundir.kill_copy_process()
        self.start_copy(rundir, dnanexus=self.dnanexus)

    def process_failed_copy_rundir(self,rundir,retcode):
        """
        Args : rundirObject - a rundir.RunDir instance.
        """
        rundirPath = rundir.get_path()
        self.send_email_rundir_copy_failed(rundirPath=rundirPath,retcode=retcode)
        # Revert status so copy can restart.
        if rundir:
            rundir.reset_to_copy_not_started()

    def process_completed_rundir(self, rundir, lims_runinfo):
        are_files_missing = self.are_files_missing(rundir)
        lims_problems = self.check_rundir_against_lims(rundir, lims_runinfo)
        disk_usage = rundir.get_disk_usage()
        rundir.unset_copy_proc_and_set_stop_time()
        self.send_email_rundir_copy_complete(rundir, are_files_missing, lims_problems, disk_usage)
        dest = os.path.join(rundir.get_root(),self.SUBDIR_COMPLETED,rundir.get_dir())
        try:
            os.renames(rundir.get_path(),dest)
        except OSError as e:
            raise OSError("Cant move run %s to %s. %s" % (rundir.get_dir(),dest,e.message))
        self.create_copy_complete_sentinel_file(rundir)
        self.rundirs_monitored.remove(rundir)

    def create_copy_complete_sentinel_file(self, rundir):
        COPY_COMPLETED_SENTINEL_FILE = 'Autocopy_complete.txt'
        self.log_creating_copy_complete_sentinel_file(rundir, COPY_COMPLETED_SENTINEL_FILE)
        touch_cmd_list = ['ssh', 
                          '-l', self.COPY_DEST_USER,
                          self.COPY_DEST_HOST,
                          'touch', os.path.join(self.COPY_DEST_RUN_ROOT, rundir.get_dir(), COPY_COMPLETED_SENTINEL_FILE)
        ]
        subprocess.call(touch_cmd_list,
                        stdout=self.LOG_FILE, stderr=self.LOG_FILE)

    def process_aborted_rundir(self,lims_runinfo,rundirObject=None,rundirPath=None):
        if rundirObject:
            rundirPath = rundirObject.get_path()
        rundirPathBasename = os.path.dirname(rundirPath)
        rundirName = os.path.basename(rundirPath)
            
        dest = os.path.join(rundirPathBasename,self.SUBDIR_ABORTED,rundirName)
        try:
            os.renames(rundirPath, dest)
        except OSError as e:
            raise OSError("Cant move run %s to %s. %s" % (rundirName,dest,e.message))
        if rundirObject:
            self.rundirs_monitored.remove(rundirObject)
        # Commented out for testing
            # lims_runinfo.set_flags_for_sequencing_failed() #may not be a flow cell, which is where scgpm_lims makes the status flag updates.
        self.send_email_rundir_aborted(rundirPath=rundirPath,dest_path=dest)
        
    def get_freespace(self, directory):
        stats = os.statvfs(directory)
        freespace_bytes = stats.f_bfree * stats.f_frsize
        return freespace_bytes

    def is_time_for_rundirs_monitored_summary(self):
        if self.last_rundirs_monitored_summary == None:
            return True
        timedelta = time.time() - self.last_rundirs_monitored_summary
        if timedelta > self.RUNDIRS_MONITORED_SUMMARY_DELAY_SECONDS:
            return True
        else:
            return False

    def is_time_for_runroot_freespace_check(self):
        if self.last_runroot_freespace_check == None:
            return True
        timedelta = time.time() - self.last_runroot_freespace_check
        if timedelta > self.RUNROOT_FREESPACE_CHECK_DELAY_SECONDS:
            return True
        else:
            return False

    def check_runroot_freespace(self):
        for run_root in self.COPY_SOURCE_RUN_ROOTS:
            freespace_bytes = self.get_freespace(run_root)
            if freespace_bytes < self.MIN_FREE_SPACE:
                self.send_email_low_freespace(run_root, freespace_bytes)
        self.last_runroot_freespace_check = time.time()

    def initialize_hostname(self):
        hostname = socket.gethostname()
        self.HOSTNAME = hostname[0:hostname.find('.')] # Remove domain part.

    def initialize_lims_connection(self, is_test_mode, no_lims):
        if no_lims:
            self.LIMS = None
        else:
            self.LIMS = Connection(apiversion=self.LIMS_API_VERSION, local_only=is_test_mode, lims_url=self.UHTS_LIMS_URL, lims_token=self.UHTS_LIMS_TOKEN)

    def initialize_mail_server(self, no_email=None):
        if no_email is not None:
            self.NO_EMAIL = no_email
        if no_email:
            return

        if not all((self.AUTOCOPY_SMTP_SERVER, self.AUTOCOPY_SMTP_PORT)):
                self.get_mail_server_settings_from_env()

        if not all((self.AUTOCOPY_SMTP_SERVER, self.AUTOCOPY_SMTP_PORT)):
            self.get_mail_server_settings_from_user()

        if not all((self.AUTOCOPY_SMTP_SERVER, self.AUTOCOPY_SMTP_PORT)):
            raise Exception("AUTOCOPY_SMTP_SERVER and AUTOCOPY_SMTP_PORT must be defined to send mail. Don't want mail? Try --no_mail.")

        self.log_connecting_to_mail_server()
        try:
            self.smtp = smtplib.SMTP(self.AUTOCOPY_SMTP_SERVER, self.AUTOCOPY_SMTP_PORT, timeout=5)
            self.smtp.starttls()
            if self.AUTOCOPY_SMTP_USERNAME and self.AUTOCOPY_SMTP_TOKEN:
                self.smtp.login(self.AUTOCOPY_SMTP_USERNAME, self.AUTOCOPY_SMTP_TOKEN)
        except socket.gaierror:
            raise Exception("Could not connect to SMTP server. Are you offline? Try running with --no_email.")

    def get_mail_server_settings_from_env(self):
        self.AUTOCOPY_SMTP_SERVER = os.getenv('AUTOCOPY_SMTP_SERVER')
        self.AUTOCOPY_SMTP_PORT = os.getenv('AUTOCOPY_SMTP_PORT')
        self.AUTOCOPY_SMTP_USERNAME = os.getenv('AUTOCOPY_SMTP_USERNAME')
        self.AUTOCOPY_SMTP_TOKEN = os.getenv('AUTOCOPY_SMTP_TOKEN')

    def get_mail_server_settings_from_user(self):
        print ("SMTP server settings were not set by env variables or commandline input")
        print ("You can enter them manually now.")
        self.AUTOCOPY_SMTP_SERVER = raw_input("SMTP server URL: ")
        self.AUTOCOPY_SMTP_PORT = raw_input("SMTP port: ")
        self.AUTOCOPY_SMTP_USERNAME = raw_input("SMTP username: ")
        self.AUTOCOPY_SMTP_TOKEN = raw_input("SMTP token: ")

    def initialize_log_file(self, log_file):
        if log_file == "-":
            self.LOG_FILE = sys.stdout
        elif log_file:
            self.LOG_FILE = open(log_file, "w")
        else:
            self.LOG_FILE = open(os.path.join(self.LOG_DIR_DEFAULT,
                                              "autocopy_%s.log" % datetime.datetime.today().strftime("%y%m%d")),'a')

    def initialize_run_roots(self):
        for run_root in self.COPY_SOURCE_RUN_ROOTS:
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

    def leave_ok_to_delete_readme(self, directory):
        readme = os.path.join(directory, 'README.txt')
        if not os.path.exists(readme):
            with open(readme, 'w') as f:
                f.write('Runs in this directory are generally OK to delete.')

    def update_rundirs_monitored(self):
        if not hasattr(self, 'rundirs_monitored'):
            # Initialize this instance var once after startup
            self.rundirs_monitored = []

        new_rundirs_monitored = []
        for run_root in self.COPY_SOURCE_RUN_ROOTS:
            new_rundirs_monitored.extend(self.scan_for_rundirs(run_root))

        # We just removed any rundirs we found on disk from rundirs_monitored.
        # Any dirs left are what we couldn't find on disk.
        # Send email warning and then forget them.
#        for missing_rundir in self.rundirs_monitored:
#            self.send_email_missing_rundir(missing_rundir)
        self.rundirs_monitored = new_rundirs_monitored

    def scan_for_rundirs(self, run_root):
        """
        Returns : A list of rundir.RunDir objects.
        """
        rundirs_found_on_disk = []
        for dirname in os.listdir(run_root):
            # Get directories, not files
            if (os.path.isdir(os.path.join(run_root, dirname))) and self.RUNDIR_REG.match(dirname):
                rundir = self.get_or_create_rundir(run_root, dirname, remove=True)
                if rundir:
                    rundirs_found_on_disk.append(rundir)
        return rundirs_found_on_disk

    def get_or_create_rundir(self, run_root, dirname, remove=False):
        rundirPath = os.path.join(run_root,dirname)
        matching_rundir = self.get_rundir(run_root=run_root, dirname=dirname)
        if matching_rundir:
            if remove:
                self.rundirs_monitored.remove(matching_rundir)
            return matching_rundir
        else:
            #Don't create a rundir object unless we know that in the LIMS it's not aborted or failed.
            # If it's aborted or failed, then there may not be the required run directory files needed
            # to create a RunDir object. For example, the platform will be set to "Unknown" if a HiSeq doens't
            # have the runParameters.xml file, and that will resultin the rundir.py script raising an Exception.

            #Before creating the RunDir object, need to check UHTS to make sure it's not aborted or failed.
            #Note that the possible sequncing run statuses in UHTS are given in app/helpers/sequencing_run_status.rb in the RAILS app.
            try:
        	print >> self.LOG_FILE, 'Getting LIMS RunInfo for %s\n' % dirname
        	self.LOG_FILE.flush()                
        	limsRunInfo = self.get_runinfo_from_lims(rundirName=dirname)
                if limsRunInfo == None:
                    print("Run " + dirname + " not found in UHTS; perhaps it just wasn't entered in yet. Skipping.")
                    return None
            except requests.exceptions.HTTPError as e:
                #perhaps run wasn't entered in UHTS yet.
                response = e.response
                status_code = response.status_code
                if status_code == 404:
                    print("Run " + dirname + " not found in UHTS; perhaps it just wasn't entered in yet. Skipping.")
                    return None
                else:
                    raise(e)

            if limsRunInfo.has_status_sequencing_failed():
                self.process_aborted_rundir(lims_runinfo=limsRunInfo,rundirPath=rundirPath)
                return None
            else:
                rundir = RunDir(run_root, dirname)
                return rundir

    def are_files_missing(self, rundir):
        # Check that the run directory has all the right files.
        files_missing = not rundir_utils.validate(rundir)
        return files_missing

    def get_runinfo_from_lims(self, rundirObject=None,rundirName=None):
        """
        Returns : A scgpm_lims.components.models.RunInfo object
        """
        if self.LIMS == None:
            return None
        if not rundirName:
            rundirName = rundirObject.get_dir()
        try:
            runinfo = RunInfo(conn=self.LIMS, run=rundirName)
        except Exception as e:
            print >> self.LOG_FILE, 'Error when getting LIMS RunInfo: %s' % e
            self.LOG_FILE.flush()
            if e.__class__ == requests.exceptions.HTTPError:
                raise e
            print(str(e.__class__))
            self.log_lims_error(e)
            runinfo = None
        return runinfo

    def check_rundir_against_lims(self, rundir, runinfo, test_only_dummy_problem=None):
        # Testproblem is for testing only
        if runinfo == None:
            return []

        fields_to_check = [
            ['Run name', rundir.get_dir(), runinfo.get_solexa_run_name()],
            ['Sequencing instrument', rundir.get_machine().lower(), runinfo.get_sequencing_instrument().lower()],
            # Comparing different formats e.g. "HCS 1.5.15.1" with "hcs_1.5.15.1"
            # 170425 pbilling: Commented out software check because versions no longer match pattern in LIMS"
            #['Sequencer software version', rundir.get_control_software_version_string().replace(' ','_').replace('.','_').lower(),
            # runinfo.get_sequencer_software().lower()],
            ['Paired end', rundir.is_paired_end(), runinfo.is_paired_end()],
            ['Read 1 cycles', rundir.get_read1_cycles(), runinfo.get_read1_cycles()],
            ['Read 2 cycles', rundir.get_read2_cycles(), runinfo.get_read2_cycles()],
            ['Is indexed', rundir.has_index_read(), runinfo.has_index_read()],
        ]

        if test_only_dummy_problem:
            fields_to_check.append(test_only_dummy_problem)

        problems_found = []
        for (field, rundirval, limsval) in fields_to_check:
            if rundirval != limsval:
                problems_found.append('Mismatched value "%s". Value in run directory: "%s". Value in LIMS: "%s"' % (field, rundirval, limsval))
        return problems_found

    def start_copy(self, rundir, dnanexus, rsync=True):
        if dnanexus == True:
            self.log_start_dnanexus_upload(rundir)
            #try:
            dnanexus_upload = DNAnexusUpload(
                                             rundir = rundir, 
                                             tar_dir = self.COPY_SOURCE_RUN_TARS, 
                                             LOG_FILE = self.LOG_FILE,
                                             initiate_analysis_script = self.INITIATE_ANALYSIS_SCRIPT, 
                                             lims_url = self.UHTS_LIMS_URL,
                                             lims_token = self.UHTS_LIMS_TOKEN,
                                             test = False,
                                             upload_mode = self.upload_mode,
                                             viewers = self.VIEWERS,
                                             contributors = self.CONTRIBUTORS,
                                             administrators = self.ADMINISTRATORS,
                                             dx_env_config = self.DX_ENV_CONFIG,
                                             dx_workflow_config_dir = self.DX_WORKFLOW_CONFIG_DIR,
					     release = self.release,
                                             develop = self.develop,
                                             region = self.REGION,
                                             upload_agent = self.UPLOAD_AGENT,
                                             ua_token = self.UA_TOKEN)
            dnanexus_upload.run()
            #lims_runinfo = self.get_runinfo_from_lims(rundirObject=rundir) 
            #self.process_completed_rundir(rundir, lims_runinfo)
            #except:
            #lims_runinfo = self.get_runinfo_from_lims(rundirObject=rundir)
            #self.process_aborted_rundir(lims_runinfo=lims_runinfo,rundirObject=rundir)
        else:
            source = rundir.get_path().rstrip('/')
            dest = self.COPY_DEST_RUN_ROOT.rstrip('/')
            copy_cmd_list = ['rsync', '-rlptc', 
                             '-e', 'ssh -l %s' % self.COPY_DEST_USER,
                             '--exclude=Thumbnail_Images/', 
                             '--chmod=Dug=rwX,Do=rX,Fug=rw,Fo=r',
                             source,
                             '%s:%s' % (self.COPY_DEST_HOST, dest),
                         ]
            copy_proc = subprocess.Popen(copy_cmd_list,
                                         stdout=self.LOG_FILE, stderr=self.LOG_FILE)
            rundir.set_copy_proc_and_start_time(copy_proc)

    def send_email_autocopy_exception(self, exception):
        tb = traceback.format_exc(exception)
        email_subj = "Autocopy unknown exception"
        email_body = "The autocopy daemon failed with Exception\n" + tb
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_autocopy_started(self):
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

    def send_email_rundir_aborted(self, rundirPath, dest_path):
        rundirName = os.path.basename(rundirPath)
        email_subj = "Run Directory Aborted: %s" % rundirName
        email_body = "The following run was flagged as 'sequencing failed' in the LIMS:\n\n"
        email_body += "\t%s\n\n" % rundirName
        email_body += "It has been moved to this directory:"
        email_body += "\t%s\n\n" % dest_path
        email_body += "If this was an error, please correct the sequencing status in the LIMS and manually move the run out of the %s folder.\n\n" % self.SUBDIR_ABORTED
        email_body += "Otherwise, this run may be safely deleted to free up disk space."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundir_copy_failed(self, rundirPath, retcode):
        rundirName = os.path.basename(rundirPath)
        email_subj = "ERROR COPYING Run Dir " + rundirName
        email_body = "Please try to resolve the error. Autocopy will continue attempting to copy as long as the run remains in the run_root directory.\n\n"
        email_body = "Run:\t\t\t%s\n" % rundirName
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundirPath)
        email_body += "\n"
        email_body += "FAILED TO COPY to:\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundirName)
        email_body += "Return code:\t%d\n" % retcode
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundir_copy_complete(self, rundir, are_files_missing, lims_problems, disk_usage):
        rundirName = rundir.get_dir()
        rundirPath = rundir.get_path()
        if disk_usage > self.ONEKILO:
            disk_usage /= self.ONEKILO
            disk_usage_units = "Tb"
        else:
            disk_usage_units = "Gb"

        if are_files_missing or (len(lims_problems) > 0):
            email_subj = "Problems found. Finished copying run dir %s" % rundirName
        else:
            email_subj = "Finished copying run dir %s" % rundirName

        email_body = 'Finished copying run %s\n\n' % rundirName

        if are_files_missing:
            email_body += "*** RUN HAS MISSING FILES ***\n\n"

        if len(lims_problems) > 0:
            email_body = "%s: *** RUN HAS INCONSISTENCIES WITH LIMS\n\n" % rundirName
            email_body = "Check the problems below and correct any errors in the LIMS:\n\n"
        for problem in lims_problems:
            email_body += "%s\n" % problem

        email_body += "Run:\t\t\t%s\n" % rundirName
        email_body += "NEW LOCATION:\t\t%s:%s/%s\n" % (self.COPY_DEST_HOST, self.COPY_DEST_RUN_ROOT, rundirName)
        email_body += "Original Location:\t%s:%s\n" % (self.HOSTNAME, rundirPath)
        email_body += "\n"
        email_body += "Read count:\t\t%d\n" % rundir.get_reads()
        email_body += "Cycles:\t\t\t%s\n" % " ".join(map(lambda d: str(d), rundir.get_cycle_list()))
        email_body += "\n"
        email_body += "Copy time:\t\t%s\n" % str(rundir.copy_end_time - rundir.copy_start_time)
        email_body += "Disk usage:\t\t%.1f %s\n" % (disk_usage, disk_usage_units)
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_missing_rundir(self, rundir):
        rundirName = rundir.get_dir()
        email_subj = "Missing Run Dir %s" % rundirName
        email_body = "MISSING RUN:\t%s\n" % rundirName
        email_body += "Location:\t%s:%s/%s\n\n" % (self.HOSTNAME, rundirName, rundirName)
        email_body += "Autocopy was tracking this run, but can no longer find it on disk."
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_low_freespace(self, run_root, freebytes):
        email_subj = "Insufficient free space in %s" % os.path.abspath(run_root)
        email_body = "The following run root directory:\n\n %s\n\n" % os.path.abspath(run_root)
        email_body += "has %0.1f GB remaining.\n\n" % (freebytes/self.ONEGIG)
        email_body += "A warning is sent when free space is less than %0.1f GB" % (self.MIN_FREE_SPACE/self.ONEGIG)
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_rundirs_monitored_summary(self):
        email_subj = 'Run status summary'
        email_body = ''
        for run_root in self.COPY_SOURCE_RUN_ROOTS:
            email_body += '%s\n\n' % os.path.abspath(run_root)
            for run_dir in self.get_rundirs(run_root=run_root):
                status = self.get_rundir_status(run_dir)
                email_body += "%s\t%s\n" % (run_dir.get_dir(), status)
            email_body += "\n"
            email_body += '\t%0.1f GB free\n\n' % (self.get_freespace(run_root)/self.ONEGIG)
        self.send_email(self.EMAIL_TO, email_subj, email_body)
        self.last_rundirs_monitored_summary = time.time()

    def send_email_run_not_found_in_lims(self, run_name):
        email_subj = 'Run not found in LIMS %s' % run_name
        email_body = 'Autocopy could not find run %s in the LIMS.\n' % run_name
        email_body += 'Autocopy will proceed with the copy anyway.'
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email_copy_restarted(self, run_name):
        email_subj = 'Stalled copy suspected. Restarted run %s' % run_name
        email_body = 'The copy process for run %s was in progress for longer than %s hours.\n' % (run_name, self.SECONDS_BEFORE_COPY_RESTART/3600)
        email_body += 'Just in case this was a stalled process, autocopy killed and restarted the rsync.\n'
        email_body += 'The copy should resume where it left off.\n'
        email_body += 'If you see this email again, you may need to troubleshoot.\n'
        self.send_email(self.EMAIL_TO, email_subj, email_body)

    def send_email(self, to, subj, body, write_email_to_log=True):
        body += "\nSent at %s\n" % time.strftime('%X %x %Z') 
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
            try:
                self.smtp.sendmail(msg['From'], to, msg.as_string())
            except smtplib.SMTPServerDisconnected:
                self.log_lost_smtp_connection()
                self.initialize_mail_server()
                self.smtp.sendmail(msg['From'], to, msg.as_string())
        if write_email_to_log:
            self.log("v----------- begin email -----------v")
            self.log(msg.as_string())
            self.log("^------------ end email ------------^\n")


    def log_starting_autocopy_message(self):
        self.log('\n')
        self.log('Autocopy is initializing\n')

    def log_main_loop(self):
        self.log("Starting main loop\n")

    def log_sleep(self):
        self.log("Sleeping for %s seconds\n" % self.MAIN_LOOP_DELAY_SECONDS)

    def log_processing_dir(self, rundir):
        self.log("processing %s" % rundir.get_dir())

    def log_start_copy(self, rundir):
        self.log("Starting copy of run %s\n" % rundir.get_dir())

    def log_start_dnanexus_upload(self, rundir):
        self.log("Startuing DNAnexus upload of run %s\n" % rundir.get_dir())

    def log_lims_error(self, error):
        self.log("Encountered an error accessing the LIMS: %s" % error.message)

    def log_connecting_to_mail_server(self):
        self.log("Connecting to mail server...")

    def log_lost_smtp_connection(self):
        self.log("Lost SMTP Connection. Attempting to reconnect.")

    def log_reached_copy_processes_max(self, rundir):
        self.log("Postponing copy of run %s because MAX_COPY_PROCESSES=%s has been reached\n" % (rundir.get_dir(), self.MAX_COPY_PROCESSES))
    
    def log_creating_copy_complete_sentinel_file(self, rundir, filename):
        self.log("Creating copy complete file '%s' in destination folder of run %s" % (filename, rundir.get_dir()))

    def log(self, *args):
        if len(args) > 0:
            log_text = ' '.join(args)
        else:
            log_text = ''
        log_lines = log_text.split("\n")
        for line in log_lines:
            print >> self.LOG_FILE, "[%s] %s" % (datetime.datetime.now().strftime("%Y %b %d %H:%M:%S"), line)
        self.LOG_FILE.flush()

    def initialize_config(self, config):
        if config is None:
            DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')
            if os.path.exists(DEFAULT_CONFIG_PATH):
                with open(DEFAULT_CONFIG_PATH) as f:
                    config = json.load(f)

        self.override_settings_with_config(config)

    def override_settings_with_config(self, config):
        # Allows certain class variables to be overridden

        if config is None:
            return

        # Input validators
        def validate_str(key, value):
            if not (isinstance(value, str) or isinstance(value, unicode)):
                raise ValidationError("Invalid value %s for config key %s. A string is required." %(value, key))
        def validate_cmdline_safe_str(key, value):
            pattern = '^[0-9a-zA-Z./_-]*$'
            if not re.match(pattern, value):
                raise ValidationError("Invalid value %s for config key %s. Must be a string matched by %s" %(value, key, pattern))
        def validate_int(key, value):
            if not isinstance(value, int):
                raise ValidationError("Invalid value %s for config key %s. An integer is required." %(value, key))
        def validate_list(key, value):
            if not isinstance(value, list):
                raise ValidationError("Invalid value %s for config key %s. A list is required." %(value, key))

        def validate(key, value, config_fields):
            if key not in config_fields.keys():
                raise ValidationError("Config contains invalid key %s. Valid keys are %s" % 
                                (key, config_fields.keys()))
            run_validation_function = config_fields[key]
            run_validation_function(key, value)
 
        config_fields = {
            'LOG_DIR_DEFAULT': validate_str,
            'SUBDIR_COMPLETED': validate_str,
            'SUBDIR_ABORTED': validate_str,
            'LIMS_API_VERSION': validate_str,
            'MAX_COPY_PROCESSES': validate_int,
            'EMAIL_TO': validate_str,
            'EMAIL_FROM': validate_str,
            'COPY_SOURCE_RUN_ROOTS': validate_list,
            'COPY_SOURCE_RUN_TARS': validate_str,
            'COPY_DEST_RUN_ROOT': validate_cmdline_safe_str,
            'MIN_FREE_SPACE': validate_int,
            'MAIN_LOOP_DELAY_SECONDS': validate_int,
            'RUNROOT_FREESPACE_CHECK_DELAY_SECONDS': validate_int,
            'RUNDIRS_MONITORED_SUMMARY_DELAY_SECONDS': validate_int,
            'UHTS_LIMS_URL': validate_str,
            'UHTS_LIMS_TOKEN': validate_str,
            'AUTOCOPY_SMTP_USERNAME': validate_str,
            'AUTOCOPY_SMTP_TOKEN': validate_str,
            'AUTOCOPY_SMTP_PORT': validate_int,
            'AUTOCOPY_SMTP_SERVER': validate_str,
            'INITIATE_ANALYSIS_SCRIPT': validate_str,
            'UPLOAD_AGENT': validate_str,
            'UA_TOKEN': validate_str,
            'REGION': validate_str,
            'VIEWERS': validate_list,
            'CONTRIBUTORS': validate_list,
            'ADMINISTRATORS': validate_list,
            'DX_ENV_CONFIG': validate_str,
            'DX_WORKFLOW_CONFIG_DIR': validate_str
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

    def get_rundir(self, run_root=None, dirname=None):
        """
        Function : Does the same as self.get_rundirs, but raises an Exception if more than one rundir.RunDir object is retrieved.
        """
        rundirs = self.get_rundirs(run_root=run_root, dirname=dirname)
        if len(rundirs) == 0:
            return None
        if len(rundirs) > 1:
            raise Exception("More than one matching rundir with run_root=%s, dirname=%s. These all matched: %s" 
                            % (run_root, dirname, rundirs))
        return rundirs[0]

    def get_rundirs(self, run_root=None, dirname=None):
        """
        Function : Looks through the list of monitored rundir.RunDir objects, and
                   1) If dirname only is specified, returns the rundir.RunDir object with a matching directory name,
                   2) If run_root only is specified, returns all rundir.RunDir objects within the run_root path,
                   3) If both run_root and dirname are specified, returns the rundir.RunDir objects whose directory
                      name matches dirname AND exists within run_root.
        """
        rundirs = self.rundirs_monitored
        if dirname is not None:
            rundirs = filter(lambda rundir: rundir.get_dir() == dirname, rundirs)
        if run_root is not None:
            rundirs = filter(lambda rundir: rundir.get_root() == run_root, rundirs)
        return rundirs

    @classmethod
    def parse_args(cls):
        usage = "%prog [options]"
        parser = OptionParser(usage=usage)

        parser.add_option("-l", "--log_file", dest="log_file", type="string",
                          default=None,
                          help='Log file path and filename. Use "-" to write to stdout instead of file. [default = %s/autocopy_{YYMMDD}.log, '\
                          'or this directory may be overridden by LOG_DIR_DEFAULT in CONFIG_FILE]'
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
                          help='Config file to override default settings [default = {autocopy_root}/bin/config.json]')
        parser.add_option("-t", "--test_mode_lims", dest="test_mode_lims", action="store_true", default=False,
                          help="Use a simulated LIMS connection")
        parser.add_option("-x", "--dnanexus", dest="dnanexus", action="store_true",
                          default=False, 
                          help="Upload runs to DNAnexus instead of SCG")
        parser.add_option("-u", "--upload_mode", dest="upload_mode",
                          default='UploadAgent', help="Specify how to upload files to DNAnexus", 
                          choices=['API', 'UploadAgent'])
	parser.add_option("-r", "--release", dest="release", action="store_true", default=False,
			  help='Specify whether to automatically release DNAnexus projects to user')
        parser.add_option("-v", "--develop", dest="develop", action="store_true", default=False,
                          help="Use development settings for creating objects on DNAnexus")

        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__=='__main__':

    (opts, args) = Autocopy.parse_args()

    if args:
        raise Exception('Extra arguments were not recognized: %s' % args)

    if opts.config_file:
        with open(opts.config_file) as f:
            config = json.load(f)
    else:
        config = None

    if opts.dry_run:
        (no_lims, no_copy, no_email) = (True, True, True)
    else:
        (no_lims, no_copy, no_email) = (opts.no_lims, opts.no_copy, opts.no_email)
    print("Starting Autocopy")
    autocopy = Autocopy(no_copy = no_copy, 
                        no_email = no_email, 
                        no_lims = no_lims, 
                        log_file = opts.log_file, 
                        config = config, 
                        test_mode_lims = opts.test_mode_lims, 
                        dnanexus = opts.dnanexus,
                        upload_mode = opts.upload_mode,
			release = opts.release,
                        develop = opts.develop)
    print("Running")
    autocopy.run()
