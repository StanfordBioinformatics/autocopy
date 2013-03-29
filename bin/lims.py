#!/usr/bin/env python

import os
import pwd
import random
import re
import socket
import subprocess
import sys

import rundir

class LIMS:

    ###
    # CONSTANTS
    ###

    LIMS_HOST_PRODUCTION      = "uhts-archive.stanford.edu"
    LIMS_RAKEFILE_PRODUCTION  = "/opt/scg/uhts-archive/current/Rakefile"
    LIMS_HOST_DEVELOPMENT     = "genescene.stanford.edu"
    LIMS_RAKEFILE_DEVELOPMENT = "/Users/bettingr/Projects/RubyLIMS/trunk/Rakefile"

    LIMS_USER = pwd.getpwuid(os.getuid()).pw_name

    PRODUCTION = True

    @classmethod
    def LIMS_HOST(cls):
        if cls.PRODUCTION:
            return cls.LIMS_HOST_PRODUCTION
        else:
            return cls.LIMS_HOST_DEVELOPMENT

    @classmethod
    def LIMS_RAKEFILE(cls):
        if cls.PRODUCTION:
            return cls.LIMS_RAKEFILE_PRODUCTION
        else:
            return cls.LIMS_RAKEFILE_DEVELOPMENT

    # This host.
    HOSTNAME = socket.gethostname()
    HOSTNAME = HOSTNAME[0:HOSTNAME.find('.')] # Remove domain part.

    TECHNICIAN_LIST = ["Addleman,Nick", "Ramirez,Lucia", "Eastman,Catharine"]

    ###
    # CONSTRUCTORS/DESTRUCTORS
    ###
    def __init__(self, ssh_socket_name=None):
        self.ssh_socket = None
        if ssh_socket_name:
            self.open_ssh_socket(ssh_socket_name)

    def __del__(self):
        if self.ssh_socket:
            self.close_ssh_socket()

    ###
    # METHODS
    ###

    def open_ssh_socket(self, ssh_socket_name):
        retcode = subprocess.call(["ssh", "-o", "ConnectTimeout=10", "-l", self.LIMS_USER,
                                   "-S", ssh_socket_name, "-M", "-f", "-N",
                                   self.LIMS_HOST()],
                                  stderr=subprocess.STDOUT)
        if retcode:
            print >> sys.stderr, os.path.basename(__file__), ": cannot create ssh socket into", self.LIMS_HOST(), "( retcode =", retcode, ")"
        else:
            self.ssh_socket = ssh_socket_name

        return retcode
    
    def close_ssh_socket(self):
        if self.ssh_socket:
            retcode = subprocess.call(["ssh", "-O", "exit", "-S", self.ssh_socket, self.LIMS_HOST()],
                                      stderr=subprocess.STDOUT)
            if retcode:
                print >> sys.stderr, os.path.basename(__file__), ": cannot close ssh socket into", self.LIMS_HOST(), "( retcode =", retcode, ")"
            else:
                self.ssh_socket = None

            return retcode

    def init_rake_cmd_list(self, task, trace=False):
        if self.ssh_socket and os.path.exists(self.ssh_socket):
            cmd_list = ["ssh", "-n", "-S", self.ssh_socket, self.LIMS_HOST(), "rake", "-s", "-f", self.LIMS_RAKEFILE()]
        else:
            cmd_list = ["ssh", "-n", self.LIMS_HOST(), "rake", "-s", "-f", self.LIMS_RAKEFILE()]
        if self.PRODUCTION:
            cmd_list.append("RAILS_ENV=production")
        if trace:
            cmd_list.append("--trace")
        cmd_list.append(task)
        return cmd_list
    def add_to_rake_cmd_list(self, rake_cmd_list, var, value):
        return rake_cmd_list.append("%s=%s" % (var, value))

    def add_params_from_rundir(self, rundir, rake_cmd_list):

        self.add_to_rake_cmd_list(rake_cmd_list, 'name', rundir.get_dir())
        self.add_to_rake_cmd_list(rake_cmd_list, 'start_date', rundir.get_start_date())
        self.add_to_rake_cmd_list(rake_cmd_list, 'seq_instrument', rundir.get_machine())

        # Run directory location: parse path for RunDir to see if it matches an IlluminaRuns* standard place.
        standard_root_dir = False
        standard_root_prefix = re.match("/Volumes/IlluminaRuns(\d)/Runs",rundir.get_root())
        if standard_root_prefix:
            if self.HOSTNAME.startswith("poirot"):
                standard_root_dir = (standard_root_prefix.group(1) == "1" or
                                     standard_root_prefix.group(1) == "2" or
                                     standard_root_prefix.group(1) == "3" or
                                     standard_root_prefix.group(1) == "4" )
            elif self.HOSTNAME.startswith("ashton"):
                standard_root_dir = (standard_root_prefix.group(1) == "5" or
                                     standard_root_prefix.group(1) == "6" )
            elif self.HOSTNAME.startswith("maigret"):
                standard_root_dir = (standard_root_prefix.group(1) == "7" or
                                     standard_root_prefix.group(1) == "8" )

        if standard_root_dir:
            self.add_to_rake_cmd_list(rake_cmd_list, 'data_volume', "illumina_runs%s" % (standard_root_prefix.group(1)))
        else:
            self.add_to_rake_cmd_list(rake_cmd_list, 'local_run_dir', "%s:%s" % (self.HOSTNAME,rundir.get_path()))

        add_to_rake_cmd_list(rake_cmd_list, 'seq_kit_version', rundir.get_seq_kit_version())

        sw_version = rundir.get_control_software_version()
        if rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA:
            self.add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'scs_%s' % sw_version[:3].replace('.','_'))
        elif rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ:
            if sw_version.startswith("1.3") or sw_version == "1.4.8":
                self.add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'hcs_%s' % sw_version.replace('.','_'))
            else:
                self.add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'hcs_%s' % sw_version[:3].replace('.','_'))
        elif rundir.get_platform() == rundir.PLATFORM_ILLUMINA_MISEQ:
            self.add_to_rake_cmd_list(rake_cmd_list, 'seq_software', 'mcs_%s' % sw_version.replace('.','_'))
        else:
            print >> sys.stderr, "WARNING: platform unknown (%s)" % rundir.get_platform()

        paired_end = rundir.is_paired_end()
        self.add_to_rake_cmd_list(rake_cmd_list, 'paired_end', paired_end)
        index_read = rundir.is_index_read()
        self.add_to_rake_cmd_list(rake_cmd_list, 'index_read', index_read)

        cycle_list = rundir.get_cycle_list()
        add_to_rake_cmd_list(rake_cmd_list, 'read1_cycles', cycle_list[0])
        if len(cycle_list) == 2:
            self.add_to_rake_cmd_list(rake_cmd_list, 'read2_cycles', cycle_list[1])
        elif len(cycle_list) == 3:
            self.add_to_rake_cmd_list(rake_cmd_list, 'read2_cycles', cycle_list[2])


    def lims_run_create_from_rundir(self, rundir, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:create_run")

        # Add the necessary run directory parameters to the rake command.
        self.add_params_from_rundir(rundir, rake_cmd_list)

        # Choose a random technician.
        self.add_to_rake_cmd_list(rake_cmd_list, 'technician', TECHNICIAN_LIST[random.randrange(len(TECHNICIAN_LIST))])

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
    #  object, then modifies it to match data from the RunDir object.
    #
    def lims_run_modify_from_rundir(self, rundir, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:modify_run")

        # Add the necessary run directory parameters to the rake command.
        self.add_params_from_rundir(rundir, rake_cmd_list)

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

    #technician=None, analysis_dir=None, backup_dir=None,
    #seqdone=None, anadone=None, dnadone=None, archdone=None, notifdone=None,
    #comments=None,
    #    if technician:
    #        add_to_rake_cmd_list(rake_cmd_list, 'technician', technician)
    #
    #    if analysis_dir:
    #        add_to_rake_cmd_list(rake_cmd_list, 'analysis_dir', analysis_dir)
    #    if backup_dir:
    #        add_to_rake_cmd_list(rake_cmd_list, 'backup_dir', backup_dir)
    #
    #    if seqdone:
    #        add_to_rake_cmd_list(rake_cmd_list, 'sequencer_done', seqdone)
    #    if anadone:
    #        add_to_rake_cmd_list(rake_cmd_list, 'analysis_done', anadone)
    #    if dnadone:
    #        add_to_rake_cmd_list(rake_cmd_list, 'dnanexus_done', dnadone)
    #    if archdone:
    #        add_to_rake_cmd_list(rake_cmd_list, 'archiving_done', archdone)
    #    if notifdone:
    #        add_to_rake_cmd_list(rake_cmd_list, 'notification_done', notifdone)
    #
    #    if comments:
    #        add_to_rake_cmd_list(rake_cmd_list, 'comments', comments)

    #
    # lims_run_modify_params() finds the run record matching the name of the run record
    #  and modifies the run record with field/value pairs in the field_dict.
    #
    # Possible keys for the field names in the input dictionary:
    #   name
    #   flowcell
    #   start_date
    #   seq_instrument
    #   technician
    #   seq_kit_version
    #   seq_software
    #   seq_run_status
    #   data_volume
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
    #   dnanexus_done
    #
    def lims_run_modify_params(self, run, field_dict, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:modify_run")

        # Add the name of the run to modify.
        if isinstance(run, rundir.RunDir):
            self.add_to_rake_cmd_list(rake_cmd_list, 'name', run.get_dir())
        elif run:
            self.add_to_rake_cmd_list(rake_cmd_list, 'name', run)
        else:
            print >> sys.stderr, "lims_run_modify_params(): Need run_name"
            return False

        # Add the parameters from the field dictionary.
        for (field, value) in field_dict.iteritems():
            self.add_to_rake_cmd_list(rake_cmd_list,field,value)

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
    #   seq_software
    #   seq_run_status
    #   data_volume
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
    #   dnanexus_done
    #
    # Example: I want the flowcell, technician, and paired end of run 111213_ILLUMINA_FCHAMMER.
    #
    # field_dict = lims_run_get_fields("111213_ILLUMINA_FCHAMMER", field_dict)
    # if field_dict:
    #   print field_dict["flowcell"], field_dict["technician"], field_dict["paired_end"]
    #
    def lims_run_get_fields(self, run_dir, verbose=False):

        # Dictionary of field names/values to return.
        field_dict = {}

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:query_run")

        if isinstance(run_dir, rundir.RunDir):
            self.add_to_rake_cmd_list(rake_cmd_list, 'name', run_dir.get_dir())
        else:
            self.add_to_rake_cmd_list(rake_cmd_list, 'name', run_dir)

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
            if verbose:
                print rake_stdout
            if rake_stdout and rake_stdout != '':
                for line in rake_stdout.split('\n'):
                    keyvalue = line.split("\t")

                    if not keyvalue[0].startswith("**"):
                        if len(keyvalue) > 1:
                            field_dict[keyvalue[0]] = keyvalue[1]
                        else:
                            field_dict[keyvalue[0]] = None

                return field_dict
            else:
                return None

    #
    # This method takes a bunch of fields from a run record and returns a list of
    # run names which match the fields given.
    #
    def lims_run_query(self, field_dict, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:query_run", trace=True)
        for key in field_dict.iterkeys():
            self.add_to_rake_cmd_list(rake_cmd_list, key, field_dict[key])

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
            run_name_list = []
            if rake_stdout and rake_stdout != '':
                for line in rake_stdout.split('\n'):
                    if not line.startswith("**"):
                        run_name_list.append(line)

                return run_name_list[0:-1]
            else:
                return None


    @classmethod
    # Second argument is from lims_run_get_fields
    def lims_run_check_rundir(cls, rundir, field_dict, check_local_run_dir=False):
        
        # This string will hold a list of messages about mismatches
        # between the RunDir and the LIMS run record, if any.
        check_msg = ""

        # Check name.
        if rundir.get_dir() != field_dict['name']:
            check_msg += "RunDir name %s does not match LIMS name %s\n" % (rundir.get_dir(), field_dict['name'])

        # Check start date.  [COMMENTED OUT BECAUSE THESE OFTEN DON'T MATCH.]
        #if rundir.get_start_date() != field_dict['start_date']:
        #    check_msg += "RunDir start date %s does not match LIMS start date %s\n" % (rundir.get_start_date(), field_dict['start_date'])

        # Check sequencing instrument.
        if rundir.get_machine().lower() != field_dict['seq_instrument'].lower():
            check_msg += "RunDir machine %s does not match LIMS seq instrument %s\n" % (rundir.get_machine(), field_dict['seq_instrument'])

        if check_local_run_dir:
            # Check data volume.
            standard_root_prefix = re.match("/Volumes/IlluminaRuns(\d)",rundir.get_root())
            if standard_root_prefix:
                rundir_data_volume = "illumina_runs%s" % standard_root_prefix.group(1)
            else:
                rundir_data_volume = "other"
            if rundir_data_volume != field_dict['data_volume']:
                check_msg += "RunDir data volume %s does not match LIMS data volume %s\n" % (rundir_data_volume, field_dict['data_volume'])

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
        if rundir.get_seq_kit_version() != field_dict['seq_kit_version']:
            check_msg += "RunDir sequencer kit %s does not match LIMS sequencer kit %s\n" % (rundir.get_seq_kit_version(), field_dict['seq_kit_version'])

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


    def lims_pipeline_create(self, run=None, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:create_pipeline_run")

        # Add the name of the run to modify.
        if isinstance(run, rundir.RunDir):
            self.add_to_rake_cmd_list(rake_cmd_list, 'run_name', run.get_dir())
        elif run:
            self.add_to_rake_cmd_list(rake_cmd_list, 'run_name', run)
        else:
            print >> sys.stderr, "lims_pipeline_create(): Need run_name"
            return None

        # Execute the rake command.
        if verbose:
            print >> sys.stderr, rake_cmd_list

        rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (rake_stdout, rake_stderr) = rake_popen.communicate()

        # Parse the output for error messages.
        if len(rake_stderr):
            # ERROR: "must have either 'id' or 'flowcell_id' env variables"
            # ERROR: "no flowcell found"

            print >> sys.stderr, rake_stderr
            return None
        else:
            if verbose:
                print rake_stdout

            # Parse output for pipeline ID created.
            id_regexp = re.match('Pipeline run (.+) created', rake_stdout)
            if id_regexp:
                return id_regexp.group(1)
            else:
                print >> sys.stderr, "lims_pipeline_create(): Parsing error out on output of LIMS command."
                return None


    def lims_pipeline_modify(self, pipeline_id, field_dict, verbose=False):

        # Set up the rake command.
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:modify_pipeline_run")

        # Add the rake command arguments.
        self.add_to_rake_cmd_list(rake_cmd_list, 'id', pipeline_id)

        # Add the parameters from the field dictionary.
        for (field, value) in field_dict.iteritems():
            self.add_to_rake_cmd_list(rake_cmd_list,field,value)

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
        else:
            if verbose:
                print rake_stdout

        return True
        

    def lims_flowcell_modify_status(self, id=None, name=None, status=None, verbose=False):

        # Set up the rake command
        rake_cmd_list = self.init_rake_cmd_list("analyze:solexa:modify_flowcell_status")
        if id:
            self.add_to_rake_cmd_list(rake_cmd_list, 'id', id)
        elif name:
            self.add_to_rake_cmd_list(rake_cmd_list, 'flowcell_id', name)
        else:
            print >> sys.stderr, "lims_flowcell_modify_status(): Need either id or name argument"
            return False

        if status:
            self.add_to_rake_cmd_list(rake_cmd_list,'status', status.lower())
        else:
            print >> sys.stderr, "lims_flowcell_modify_status(): Need status argument"
            return False

        # Execute the rake command.
        if verbose:
            print >> sys.stderr, rake_cmd_list

        rake_popen = subprocess.Popen(rake_cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (rake_stdout, rake_stderr) = rake_popen.communicate()

        # Parse the output for error messages.
        if len(rake_stderr):
            # ERROR: "must have either 'id' or 'flowcell_id' env variables"
            # ERROR: "no flowcell found"

            print >> sys.stderr, rake_stderr
            return None
        else:
            if verbose:
                print rake_stdout

            return True


#
# Code to run the LIMS module from the command line.
#
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

    # Create a LIMS object.
    lims_obj = LIMS()

    #
    # Argument 1 is command:
    #  "createRun RUNDIR [RUNDIR*]"
    #  "modifyRun RUNDIR FIELD VALUE [FIELD VALUE]"
    #  "getRunFields RUNDIR FIELD FIELD FIELD"
    #  "queryRun FIELD VALUE [FIELD VALUE]"
    #  "checkRun RUNDIR"
    #  "setFCStatus FLOWCELL STATUS"
    #
    command = args[0]
    args = args[1:]

    if command == "createRun":
        for arg in args:
            (root, dir) = os.path.split(os.path.abspath(arg))
            run_dir = rundir.RunDir(root,dir)

            if lims_obj.lims_run_create_from_rundir(run_dir, verbose=True):
                print >> sys.stderr, os.path.basename(__file__), ": Run creation of %s succeeded." % run_dir.get_dir()
            else:
                print >> sys.stderr, os.path.basename(__file__), ": Run creation of %s FAILED." % run_dir.get_dir()
                sys.exit(1)

    elif command == "modifyRun":
        (root, dir) = os.path.split(os.path.abspath(args[0]))
        run_dir = rundir.RunDir(root,dir)

        # Make a dictionary of the remaining arguments.
        arg_dict = {"verbose" : True}
        for idx in range(2,len(args),2):
            arg_dict[args[idx-1]] = args[idx]

        if lims_obj.lims_run_modify_params(run_dir, arg_dict):
            print >> sys.stderr, os.path.basename(__file__), ": Run modification of %s succeeded." % run_dir.get_dir()
        else:
            print >> sys.stderr, os.path.basename(__file__), ": Run modification of %s FAILED." % run_dir.get_dir()
            sys.exit(1)

    elif command == "getRunFields":
        (root, dir) = os.path.split(os.path.abspath(args[0]))
        run_dir = rundir.RunDir(root,dir)

        fields = lims_obj.lims_run_get_fields(run_dir, verbose=True)

        if fields:
            #print fields
            for arg in args[1:]:
                if arg in fields:
                    print "%s: %s=%s" % (run_dir.get_dir(), arg, fields[arg])
                else:
                    print "%s: No value for %s" % (run_dir.get_dir(), arg)
        else:
            print >> sys.stderr, os.path.basename(__file__), ": No fields found."
            sys.exit(1)


    elif command == "queryRun":
        # Make a dictionary of the arguments.
        arg_dict = {}
        for idx in range(1,len(args),2):
            arg_dict[args[idx-1]] = args[idx]

        run_name_list = lims_obj.lims_run_query(arg_dict, verbose=True)

        print run_name_list

    elif command == "checkRun":

        for arg in args:
            (root, dir) = os.path.split(os.path.abspath(arg))
            run_dir = rundir.RunDir(root,dir)

            arg_dict = lims_obj.lims_run_get_fields(run_dir) # , verbose=True)
            if arg_dict:

                check_msg = lims_obj.lims_run_check_rundir(run_dir, arg_dict)
                if not check_msg:
                    print >> sys.stderr, os.path.basename(__file__), ": Checking %s with LIMS succeeded." % run_dir.get_dir()
                else:
                    print >> sys.stderr, os.path.basename(__file__), ": Checking %s with LIMS FAILED:" % run_dir.get_dir()
                    print >> sys.stderr, check_msg
                    sys.exit(1)
            else:
                print >> sys.stderr, os.path.basename(__file__), ": No run record found for %s" % run_dir.get_dir()
                sys.exit(1)

    elif command == "setFCStatus":

        flowcell_name = args[0]
        status        = args[1]

        if lims_obj.lims_flowcell_modify_status(name=flowcell_name, status=status,verbose=True):
            print >> sys.stderr, os.path.basename(__file__), ": Setting flowcell %s status to '%s' succeeded." % (flowcell_name,status)
        else:
            print >> sys.stderr, os.path.basename(__file__), ": Setting flowcell %s status to '%s' FAILED." % (flowcell_name,status)
            sys.exit(1)

    else:
        print >> sys.stderr, os.path.basename(__file__), ": Command %s not understood" % command
        sys.exit(-1)

    sys.exit(0)


