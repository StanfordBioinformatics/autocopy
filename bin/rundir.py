#!/usr/bin/env python

import datetime
import glob
import os
import os.path
import platform
import re
import subprocess
import sys
import xml.dom.minidom
import xml.dom.pulldom

import rundir_utils

#
# The RunDir object encapsulates all the functionality associated with an Illumina run directory.
#
# Files read from run directory to support the methods below:
#  Data/reports/Status.xml
#  Data/reports/StatusUpdate.xml
#  Recipe_GA2_*Cycle_v8.3.xml [for GA runs]
#  runParameters.xml [for HiSeq runs]
#  RunInfo.xml
#  <Status Files>
#
class RunDir:

    ###
    # CONSTANTS
    ###

    # Status strings
    STATUS_STRS = [
        "Initialized",
        "Started",
        "Image Analysis Complete (SingleRead)",
        "Image Analysis Complete (Read 1)",
        "Image Analysis Complete (Read 2)",
        "Image Analysis Complete (Read 3)",
        "Image Analysis Complete (Read 4)",
        "Base Calling Complete (SingleRead)",
        "Base Calling Complete (Read 1)",
        "Base Calling Complete (Read 2)",
        "Base Calling Complete (Read 3)",
        "Base Calling Complete (Read 4)",
        ]

    # Status constants
    STATUS_MAX_INDEX = len(STATUS_STRS)
    (STATUS_INITIALIZED,
     STATUS_STARTED,
     STATUS_IMAGEANALYSIS_COMPLETE_SINGLEREAD,
     STATUS_IMAGEANALYSIS_COMPLETE_READ1,
     STATUS_IMAGEANALYSIS_COMPLETE_READ2,
     STATUS_IMAGEANALYSIS_COMPLETE_READ3,
     STATUS_IMAGEANALYSIS_COMPLETE_READ4,
     STATUS_BASECALLING_COMPLETE_SINGLEREAD,
     STATUS_BASECALLING_COMPLETE_READ1,
     STATUS_BASECALLING_COMPLETE_READ2,
     STATUS_BASECALLING_COMPLETE_READ3,
     STATUS_BASECALLING_COMPLETE_READ4) = range(STATUS_MAX_INDEX)

    # Status files
    STATUS_FILES= [
        None,
        "First_Base_Report.txt",
        "ImageAnalysis_Netcopy_complete_SINGLEREAD.txt",
        "ImageAnalysis_Netcopy_complete_READ1.txt",
        "ImageAnalysis_Netcopy_complete_READ2.txt",
        "ImageAnalysis_Netcopy_complete_READ3.txt",
        "ImageAnalysis_Netcopy_complete_READ4.txt",
        "Basecalling_Netcopy_complete_SINGLEREAD.txt",
        "Basecalling_Netcopy_complete_READ1.txt",
        "Basecalling_Netcopy_complete_READ2.txt",
        "Basecalling_Netcopy_complete_READ3.txt",
        "Basecalling_Netcopy_complete_READ4.txt",
    ]

    #
    # Analysis Statuses
    #
    
    # NOTE:

    # Elements of each list = [ HUMAN READABLE STRING, COMPUTER READABLE STRING, FILENAME ]
    ANALYSIS_STATUS_ARRAY = [
        [ "None", "None", None ],
        [ "Analysis Started", "STARTED", "Analysis_started.txt" ],
        [ "Pre-run Started", "PRERUN_STARTED", "Pre-run_started.txt"],
        [ "Pre-run Complete", "PRERUN_COMPLETE", "Pre-run_complete.txt"],
        [ "Bcl LANE Started", "BCL_STARTED", "Bcl_LANE_started.txt"],
        [ "Bcl LANE Complete", "BCL_COMPLETE", "Bcl_LANE_complete.txt"],
        [ "Mapping LANE Started", "MAPPING_STARTED", "Mapping_LANE_started.txt"],
        [ "Mapping LANE Complete", "MAPPING_COMPLETE", "Mapping_LANE_complete.txt"],
        [ "Publish LANE Started", "PUBLISH_STARTED", "Publish_LANE_started.txt"],
        [ "Publish LANE Complete", "PUBLISH_COMPLETE", "Publish_LANE_complete.txt"],
        [ "Post-run Started", "POSTRUN_STARTED", "Post-run_started.txt"],
        [ "Post-run Complete", "POSTRUN_COMPLETE", "Post-run_complete.txt"],
        [ "Analysis Complete", "COMPLETE", "Analysis_complete.txt"]
    ]
    # Indices for the sublists above.
    (ANALYSIS_STATUS_IDX_HUMAN_STRING,
     ANALYSIS_STATUS_IDX_COMP_STRING,
     ANALYSIS_STATUS_IDX_FILENAME) = range(3)
    
    # Analysis Status constants
    ANALYSIS_STATUS_MAX_INDEX = len(ANALYSIS_STATUS_ARRAY)
    (ANALYSIS_STATUS_NONE,
     ANALYSIS_STATUS_STARTED,
     ANALYSIS_STATUS_PRERUN_STARTED,
     ANALYSIS_STATUS_PRERUN_COMPLETE,
     ANALYSIS_STATUS_BCL_STARTED,
     ANALYSIS_STATUS_BCL_FINISHED,
     ANALYSIS_STATUS_MAPPING_STARTED,
     ANALYSIS_STATUS_MAPPING_COMPLETE,
     ANALYSIS_STATUS_PUBLISH_STARTED,
     ANALYSIS_STATUS_PUBLISH_FINISHED,
     ANALYSIS_STATUS_POSTRUN_STARTED,
     ANALYSIS_STATUS_POSTRUN_COMPLETE,
     ANALYSIS_STATUS_COMPLETE
    ) = range(ANALYSIS_STATUS_MAX_INDEX)

    ANALYSIS_STATUS_PATH = os.path.join("Analysis","Status")

    # Path relative to run directory to Status.xml file,
    #  which contains reads and cycles.
    DATA_STATUS_PATH = os.path.join("Data","reports","Status.xml")
    DATA_STATUSUPDATE_PATH = os.path.join("Data","reports","StatusUpdate.xml")

    # The length of Illumina barcodes if reads = 3.
    ILLUMINA_BARCODE_LENGTH = 7

    # Platforms for run directory.
    PLATFORM_NAMES = [
        "Unknown",
        "Illumina GAIIx",
        "Illumina HiSeq",
        "Illumina MiSeq"
    ]
    PLATFORM_MAX_INDEX = len(PLATFORM_NAMES)
    (PLATFORM_UNKNOWN,
     PLATFORM_ILLUMINA_GA,
     PLATFORM_ILLUMINA_HISEQ,
     PLATFORM_ILLUMINA_MISEQ) = range(PLATFORM_MAX_INDEX)
    
    ###
    # Constructor
    ###
    def __init__(self, root, directory):
        #print("root is {root},dir is {directory}".format(root=root,directory=directory))
        self.root = root
        self.dir = directory

        self.reads = None
        self.cycle_list = None
        self.paired_end = None
        self.index_read = None

        self.lanes = None

        self.set_platform() #sets self.platform 
        self.seq_kit_version = None

        self.copy_proc = None
        self.copy_start_time = None
        self.copy_end_time = None

        self.start_date = None
        self.machine = None
        self.number = None
        self.flowcell = None
        
    def str(self):
        s = ""
        s += "<RUNDIR %s>\n" % (self.get_dir())
        s += "  Date: %s  Mach: %s  Num: %s  FC: %s\n" % (self.get_start_date(), self.get_machine(), self.get_number(), self.get_flowcell())
        s += "  Root: \t%s\n" % self.get_root()
        s += "  Status:\t%s\n" % self.get_status_string()
        s += "  Reads:\t%d\n" % self.get_number_of_reads()
        s += "  Cycles:\t%s\n" % " ".join(map(lambda d: str(d), self.get_cycle_list()))
        s += "\n"
        s += "  Extracted Cycles:\t%s/%s\n" % (self.get_extracted_cycle(), self.get_total_cycles())
        s += "  Called Cycles:\t%s/%s\n" % (self.get_called_cycle(),self.get_total_cycles())
        s += "  Scored Cycles:\t%s/%s\n" % (self.get_scored_cycle(),self.get_total_cycles())
        s += "\n"
        s += "  Platform:\t%s\n" % RunDir.PLATFORM_NAMES[self.get_platform()]
        s += "  SW Version:\t%s\n" % self.get_control_software_version()
        s += "  Lanes:\t%s\n" % self.get_lanes()
        return s

    ###
    # Accessors
    ###
    def get_root(self):
        return self.root
    def get_dir(self):
        return self.dir
    def get_path(self):
        return os.path.join(self.root,self.dir)
    def get_data_volume(self):
        m = re.search(r'(IlluminaRuns[0-9]+)', self.get_root())
        if not m:
            return None
        else:
            return m.groups()[0]

    def get_start_date(self):

        # Determine the start date from the run dir, if it hasn't been done yet.
        if not self.start_date:
            platform = self.get_platform()
            if platform == self.PLATFORM_ILLUMINA_GA:
                # Parse directory name to get start date.
                name_parse = self.get_dir().split("_",4)
                if len(name_parse) > 0 and re.match("\d{6}",name_parse[0]):
                    self.start_date = name_parse[0]
                else:
                    print >> sys.stderr, "RunDir.get_start_date(): RunDir %s: Start Date %s is not 6 digits." % (self.get_dir(),name_parse[0])
                    self.start_date = None

            elif platform == self.PLATFORM_ILLUMINA_HISEQ :

                # Get start date from runParameters.xml.
                # XML Path: <RunParameters><Setup><RunStartDate>
                run_params_file = os.path.join(self.get_path(), "runParameters.xml")
                if os.path.exists(run_params_file):
                    run_params_doc = xml.dom.minidom.parse(run_params_file)

                    # Get <RunParameters><Setup><RunStartDate>.
                    runparams_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    setup_node = runparams_node.getElementsByTagName("Setup")[0]
                    runstartdate_node = setup_node.getElementsByTagName("RunStartDate")[0]

                    self.start_date = runstartdate_node.firstChild.nodeValue

            elif platform == self.PLATFORM_ILLUMINA_MISEQ:

                # Get start date from runParameters.xml.
                # XML Path: <RunParameters><RunStartDate>
                run_params_file = os.path.join(self.get_path(), "runParameters.xml")
                if os.path.exists(run_params_file):
                    run_params_doc = xml.dom.minidom.parse(run_params_file)

                    # Get <RunParameters><Setup><RunStartDate>.
                    runparams_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    runstartdate_node = runparams_node.getElementsByTagName("RunStartDate")[0]

                    self.start_date = runstartdate_node.firstChild.nodeValue

            else:
                print >> sys.stderr, "RunDir.get_start_date(): Platform unknown."
                return None

        return self.start_date
    
    def get_machine(self):

        # Determine the machine from the run dir, if it hasn't been done yet.
        if self.machine is None:

            if self.get_platform() == RunDir.PLATFORM_ILLUMINA_MISEQ:

                machine = self.get_machine_from_dataintensitiesconfigxml()
                if machine is not None:
                    self.machine = machine

            elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:

                machine = self.get_machine_from_runinfoxml()
                if machine is not None:
                    self.machine = machine

            elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:

                machine = self.get_machine_from_dataintensitiesconfigxml()
                if machine is not None:
                    self.machine = machine
                else:
                    machine = self.get_machine_from_runinfoxml()
                    if machine is not None:
                        self.machine = machine

            else:
                print >> sys.stderr, "RunDir.get_machine(): Platform unknown."
                return None

        return self.machine

    def get_number(self):

        # Determine the run number from the run dir, if it hasn't been done yet.
        if self.number is None:

            # Get run number from RunInfo.xml.
            # XML Path: <RunInfo><Run Number="">
            run_info_file = os.path.join(self.get_path(), "RunInfo.xml")
            if os.path.exists(run_info_file):
                run_info_doc = xml.dom.minidom.parse(run_info_file)

                # Get <RunInfo><Run><Instrument>.
                runinfo_node = run_info_doc.getElementsByTagName("RunInfo")[0]
                run_node = runinfo_node.getElementsByTagName("Run")[0]

                # Find Number attribute in Run node.
                run_attrs = run_node.attributes
                for attr_idx in range(run_attrs.length):
                    attr_name = run_attrs.item(attr_idx).name
                    if attr_name == "Number":
                        self.number = run_attrs.item(attr_idx).value
                        break

        return self.number

    
    def get_flowcell(self):

        # Determine the flowcell from the run dir, if it hasn't been done yet.
        if self.flowcell is None:

            platform = self.get_platform()
            if platform == self.PLATFORM_ILLUMINA_GA:

                # Parse directory name to get flowcell.
                name_parse = self.get_dir().split("_",4)
                if len(name_parse) == 4:
                    flowcell_name = name_parse[3].split('_',1)[0]
                    # Remove leading "FC", if any.
                    fc_regexp = re.match("^(FC)?(.{5})", flowcell_name)
                    if fc_regexp:
                        self.flowcell = fc_regexp.group(2)
                elif len(name_parse) == 3:
                    flowcell_name = name_parse[2].split('_',1)[0]
                    # Remove leading "FC", if any.
                    fc_regexp = re.match("^(FC)?(.{5})", flowcell_name)
                    if fc_regexp:
                        self.flowcell = fc_regexp.group(2)
                else:
                    print >> sys.stderr, "RunDir.get_flowcell(): RunDir %s: Flowcell not found." % (self.get_dir())
                    self.flowcell = None

            elif platform == self.PLATFORM_ILLUMINA_HISEQ:

                # Get flowcell from runParameters.xml.
                # XML Path: <RunParameters><Setup><Barcode> (Alternative: From RunInfo.xml <RunInfo><Run><Flowcell>)
                run_params_file = os.path.join(self.get_path(), "runParameters.xml")
                if os.path.exists(run_params_file):
                    run_params_doc = xml.dom.minidom.parse(run_params_file)

                    # Get <RunParameters><Setup><RunStartDate>.
                    runparams_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    setup_node = runparams_node.getElementsByTagName("Setup")[0]
                    barcode_node = setup_node.getElementsByTagName("Barcode")[0]

                    flowcell_name = barcode_node.firstChild.nodeValue

                    # Remove tail (e.g., "ACXX") from flowcell name.
                    self.flowcell = flowcell_name[:5]

            elif platform == self.PLATFORM_ILLUMINA_MISEQ:

                # Get flowcell from runParameters.xml.
                # XML Path: <RunParameters><Barcode> (Alternative: From RunInfo.xml <RunInfo><Run><Flowcell>)
                run_params_file = os.path.join(self.get_path(), "runParameters.xml")
                if os.path.exists(run_params_file):
                    run_params_doc = xml.dom.minidom.parse(run_params_file)

                    # Get <RunParameters><Setup><RunStartDate>.
                    runparams_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    barcode_node = runparams_node.getElementsByTagName("Barcode")[0]

                    flowcell_name = barcode_node.firstChild.nodeValue

                    # Remove "000000000-" from flowcell name.
                    self.flowcell = flowcell_name[-5:]
            else:
                print >> sys.stderr, "RunDir.get_flowcell(): Platform unknown."
                return None

        return self.flowcell

    def get_reads(self):
        return self.get_number_of_reads()

    def get_number_of_reads(self):
        if self.reads is None:
            self.find_reads_cycles()
        if self.reads is None:
            return 0
        else:
            return self.reads

    def get_cycle_list(self):
        if self.cycle_list is None:
            self.find_reads_cycles()
        if self.cycle_list is None:
            return []
        else:
            return self.cycle_list

    def get_total_cycles(self):
        return sum(self.get_cycle_list())

    def get_read1_cycles(self):
        cycle_list = self.get_cycle_list()
        if len(cycle_list) < 1:
            return 0
        else:
            return cycle_list[0]

    def get_read2_cycles(self):
        cycle_list = self.get_cycle_list()
        if self.is_paired_end():
            # Don't know how to tell if this is dual index or not,
            # so just return the last element of the array
            if len(cycle_list) < 3:
                raise Exception ("Cycle list should be at least length 3 for a paired end run.")
            if len(cycle_list) > 5:
                raise Exception ("Cycle list should be no more than length 4 for a paired end run.")
            return cycle_list[-1]
        elif len(cycle_list) == 2:
            return cycle_list[1]
        elif len(cycle_list) == 1:
            # There is no read2 cycle
            return None
        else:
            raise Exception("Cycle list should be no more than length 2 for a non-paired end run.")

    def is_paired_end(self):
        if self.paired_end is None:
            self.find_reads_cycles()
        return self.paired_end

    def has_index_read(self):
        if self.index_read is None:
            self.find_reads_cycles()
        return self.index_read

    def get_extracted_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"ImgCycle")
    def get_called_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"CallCycle")
    def get_scored_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"ScoreCycle")

    def get_lanes(self):
        if self.lanes is None:
            platform = self.get_platform()
            if platform == RunDir.PLATFORM_ILLUMINA_GA:
                self.lanes = 8
            elif platform == RunDir.PLATFORM_ILLUMINA_HISEQ:
                # Advent of HiSeq 2500: put check for run mode here.
                self.lanes = 8
            elif platform == RunDir.PLATFORM_ILLUMINA_MISEQ:
                self.lanes = 1
            else:
                print >> sys.stderr, "RunDir.get_lanes(): unknown platform"

        return self.lanes

    #
    # SEQUENCING STATUS METHODS
    #
    def get_status(self):
        # Find the highest numbered status (latest in workflow) that
        #  is represented by a file in the run dir.
        for status in range(RunDir.STATUS_MAX_INDEX - 1, RunDir.STATUS_INITIALIZED, -1):
            if os.path.exists(os.path.join(self.get_path(), RunDir.STATUS_FILES[status])):
                break
        else:
            status = RunDir.STATUS_INITIALIZED
        return status

    def get_seq_status(self):
        return self.get_status()

    def get_status_string(self):
        return RunDir.STATUS_STRS[self.get_status()]

    def get_seq_status_string(self):
        return self.get_status_string()

    def reset_to_copy_not_started(self):
        self.copy_proc = None
        self.copy_start_time = None
        self.copy_end_time = None

    def kill_copy_process(self):
        self.copy_proc.kill()
        self.reset_to_copy_not_started()

    def seconds_since_copy_started(self):
        if self.copy_start_time == None:
            return None
        else:
            timedelta = datetime.datetime.now() - self.copy_start_time
        return timedelta.seconds

    def is_copying(self):
        if self.copy_proc:
            return True
        else:
            return False

    def set_copy_proc_and_start_time(self, copy_proc):
        self.copy_proc = copy_proc
        self.copy_start_time = datetime.datetime.now()
        self.copy_end_time = None

    def unset_copy_proc_and_set_stop_time(self):
        self.copy_proc = None
        self.copy_end_time = datetime.datetime.now()

    def is_finished(self):

        status = self.get_status()

        if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
            if self.get_reads() == 1:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_SINGLEREAD
            else:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
            
        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:
            sw_version = self.get_control_software_version(integer=True)
            if sw_version == 1137:  # "1.1.37"
                if self.get_reads() == 1:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_SINGLEREAD
                else:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
            elif sw_version >= 1308: # "1.3.8", "1.4.5", "1.4.8", "1.5.15"
                if self.get_reads() == 1:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_READ1
                elif self.get_reads() == 2:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
                elif self.get_reads() == 3:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_READ3
                elif self.get_reads() == 4:
                    return status == RunDir.STATUS_BASECALLING_COMPLETE_READ4
                else:
                    print >> sys.stderr, "RunDir.is_finished(): %s: Unexpected number of reads: %d" % (self.get_dir(), self.get_reads())
                    return False
            else:
                print >> sys.stderr, "RunDir.is_finished(): %s: HiSeq SW version %s unknown" % (self.get_dir(), sw_version)
                return False

        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_MISEQ:
            if self.get_reads() == 1:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_READ1
            elif self.get_reads() == 2:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
            elif self.get_reads() == 3:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_READ3
            elif self.get_reads() == 4:
                return status == RunDir.STATUS_BASECALLING_COMPLETE_READ4
            else:
                print >> sys.stderr, "RunDir.is_finished(): %s: Unexpected number of reads: %d" % (self.get_dir(), self.get_reads())
                return False

        else:
            print >> sys.stderr, "RunDir.is_finished(): %s: Platform unknown" % (self.get_dir())
            return False
    def is_seq_finished(self):
        return self.is_finished()

    #
    # ANALYSIS STATUS METHODS
    #
    def get_analysis_status(self, lane=None):

        # If no analysis status directory, status is NONE.
        analysis_status_path = os.path.join(self.get_path(), RunDir.ANALYSIS_STATUS_PATH)
        if not os.path.exists(analysis_status_path):
            analysis_status = self.ANALYSIS_STATUS_NONE

        # If no lane given, look at all lanes for status.
        elif lane is None:
            # Read all lanes of status.  Result across them is the earliest of all.
            analysis_status = self.ANALYSIS_STATUS_MAX_INDEX
            for lane in range(1,self.get_lanes()+1):
                lane_status = self.get_analysis_status(lane)
                if lane_status < analysis_status:
                    analysis_status = lane_status

        # Lane-specific status request.
        else:
            analysis_status = None
            lane_str = "Lane%d" % lane
            for status in range(self.ANALYSIS_STATUS_STARTED, self.ANALYSIS_STATUS_MAX_INDEX):
                status_file = re.sub("LANE",lane_str,
                                     self.ANALYSIS_STATUS_ARRAY[status][self.ANALYSIS_STATUS_IDX_FILENAME])
                status_path = os.path.join(analysis_status_path, status_file)
                if os.path.exists(status_path):
                    analysis_status = status

        return analysis_status

    def get_analysis_status_string(self, lane=None, human_readable=True):

        analysis_status = self.get_analysis_status(lane)

        if human_readable:
            analysis_status_str = self.ANALYSIS_STATUS_ARRAY[analysis_status][self.ANALYSIS_STATUS_IDX_HUMAN_STRING]
        else:
            analysis_status_str = self.ANALYSIS_STATUS_ARRAY[analysis_status][self.ANALYSIS_STATUS_IDX_COMP_STRING]

        if lane is not None:
            analysis_status_str = re.sub("LANE","Lane %d" % lane, analysis_status_str)
        else:
            analysis_status_str = re.sub("LANE ","", analysis_status_str)

        return analysis_status_str

    def set_analysis_status(self, lane, new_status):

        # This method does not enforce the hierarchy: i.e., it won't check for or put
        # BCL and Mapping status files down if the request is for a new Publish status.

        # If no analysis status directory, make one.
        analysis_status_path = os.path.join(self.get_path(), RunDir.ANALYSIS_STATUS_PATH)
        if not os.path.exists(analysis_status_path):
            os.makedirs(analysis_status_path)

        status_file = RunDir.ANALYSIS_STATUS_ARRAY[new_status][RunDir.ANALYSIS_STATUS_IDX_FILENAME]
        if status_file is not None:
            if lane is not None:
                status_file = re.sub("LANE", "Lane%d" % lane, status_file)
            fp = open(os.path.join(analysis_status_path, status_file), "w")
            fp.close()


    def unset_analysis_status(self, lane, new_status):

        # This method does not enforce the hierarchy: i.e., it won't check for or remove
        # BCL and Mapping status files if the request is for a new Publish status.

        # If no analysis status directory, do nothing.
        analysis_status_path = os.path.join(self.get_path(), RunDir.ANALYSIS_STATUS_PATH)
        if not os.path.exists(analysis_status_path):
            return

        status_file = RunDir.ANALYSIS_STATUS_ARRAY[new_status][RunDir.ANALYSIS_STATUS_IDX_FILENAME]
        if status_file is not None:
            if lane is not None:
                status_file = re.sub("LANE", "Lane%d" % lane, status_file)
            status_path = os.path.join(analysis_status_path, status_file)
            if os.path.exists(status_path):
                os.remove(status_path)

    def get_disk_usage(self):
        #
        # Run 'du' on the run directory.
        #
        du_cmd_list = ['du', '-s']

        # Set block size switches for various platforms.
        #  The desired unit of measure is gigabytes.
        platfm = platform.system()
        if platfm == "Linux":
            du_cmd_list.append("--block-size=1G")
        elif platfm == "Darwin":
            du_cmd_list.append("-g")
        else:
            print >> sys.stderr, "get_disk_usage(): Unknown OS platform %s" % (platfm)
            disk_usage = None
            return self.disk_usage

        # Add the run directory path to the command.
        du_cmd_list.append(self.get_path())

        # Run the disk usage command and get the output.
        du_cmd_pipe = subprocess.Popen(du_cmd_list, stdout=subprocess.PIPE)
        (du_stdout, du_stderr) = du_cmd_pipe.communicate()

        # Parse the output.
        (disk_usage, dir) = du_stdout.split()
        return int(disk_usage)

    ###
    # Other methods
    ###
    def find_reads_cycles(self):

        (reads, cycle_list, pairedend_run, indexed_reads) = self.get_reads_cycles_from_runparameters()
        if reads is None:
            (reads, cycle_list, pairedend_run, indexed_reads) = self.get_reads_cycles_from_status()
        if reads is None:
            (reads, cycle_list, pairedend_run, indexed_reads) = self.get_reads_cycles_from_recipe()

        self.reads = reads
        self.cycle_list = cycle_list
        self.paired_end = pairedend_run
        self.index_read = indexed_reads

    def get_reads_cycles_from_runparameters(self):
        """
        Funciton :
        Returns  : tuple of 4 items being:
                 1) int. number of read types (i.e. for forward read, index read, index2 read, reverse read),
                 2) list where each element is an int indicating the number of cycles for the corresponding read,
                 3) bool indicating whether the run is PE, and
                 4) bool indicating whether the run has at least one index read.
        """

        reads = 0
        pairedend_run = False
        indexed_reads = False

        #
        # Open the runParameters.xml file as an XML document.
        #
        run_dir = self.get_path()
        run_parameters_path = os.path.join(run_dir, "runParameters.xml")

        if os.path.exists(run_parameters_path):
            run_parameters_doc = xml.dom.minidom.parse(run_parameters_path)

            cycle_hash = dict()

            if self.platform == RunDir.PLATFORM_ILLUMINA_HISEQ:
                # Get <RunParameters><Setup><Reads>
                run_parameters_node = run_parameters_doc.getElementsByTagName("RunParameters")[0]
                setup_node = run_parameters_node.getElementsByTagName("Setup")[0]
                reads_node = setup_node.getElementsByTagName("Reads")[0]

                # Get <Read> subnodes
                read_nodes = reads_node.getElementsByTagName("Read")

            elif self.platform == RunDir.PLATFORM_ILLUMINA_MISEQ:

                # Get <RunParameters><Reads>
                run_parameters_node = run_parameters_doc.getElementsByTagName("RunParameters")[0]
                reads_node = run_parameters_node.getElementsByTagName("Reads")[0]

                # Get <RunInfoReads> subnodes
                read_nodes = reads_node.getElementsByTagName("RunInfoRead")

            else:
                # Platform has runParameters.xml, but isn't HiSeq/MiSeq?!
                return (None, None, None, None)

            # Decode platforms HiSeq and MiSeq
            for read_node in read_nodes:
                # Get "Number" attribute to search for number of reads.
                read_number = int(read_node.getAttribute("Number"))
                if read_number > reads: reads = read_number

                # Get "NumCycles" attribute for cycle list.
                cycles = int(read_node.getAttribute("NumCycles"))
                cycle_hash[read_number] = cycles

                # Check for "IsIndexedRead" attribute to determine if this is an indexed run.
                #  Also, if a non-indexed read is read number > 1, this is a paired-end run.
                indexed_read = read_node.getAttribute("IsIndexedRead")
                if indexed_read == 'Y':
                    indexed_reads = True
                elif indexed_read == "N" and read_number > 1:
                    pairedend_run = True

            # Create cycle_list from the accumulated cycle numbers.
            cycle_list = range(reads)
            for read_num in cycle_hash.keys():
                cycle_list[read_num-1] = cycle_hash[read_num]

        else:
            reads = None
            cycle_list = None
            pairedend_run = None
            indexed_reads = None

        return (reads, cycle_list, pairedend_run, indexed_reads)


    def get_reads_cycles_from_status(self):
            
        #
        # Open the Status.xml file as an XML document.
        #
        run_dir = self.get_path()
        run_status_path = os.path.join(run_dir, RunDir.DATA_STATUS_PATH)
        if os.path.exists(run_status_path):
            status_doc = xml.dom.minidom.parse(run_status_path)

            # Get <Configuration><NumberOfReads>.
            config_node = status_doc.getElementsByTagName("Configuration")[0]
            num_reads_node = config_node.getElementsByTagName("NumberOfReads")[0]
            reads = int(num_reads_node.firstChild.nodeValue)

            # Get <Configuration><IsPairedEndRun>.
            is_pairedend_run_node = config_node.getElementsByTagName("IsPairedEndRun")[0]
            pairedend_run = (is_pairedend_run_node.firstChild.nodeValue == 'True')

            # Get <NumberOfCycles>.
            num_cycles_node = status_doc.getElementsByTagName("NumCycles")[0]
            cycles = int(num_cycles_node.firstChild.nodeValue)
            
            if reads == 3:
                cycles -= RunDir.ILLUMINA_BARCODE_LENGTH
                cycles_per_read = cycles/(reads-1)
                cycle_list = [cycles_per_read, RunDir.ILLUMINA_BARCODE_LENGTH, cycles_per_read]
                indexed_reads = True
            elif reads == 2 and not pairedend_run:
                cycles -= RunDir.ILLUMINA_BARCODE_LENGTH
                cycle_list = [cycles, RunDir.ILLUMINA_BARCODE_LENGTH]
                indexed_reads = True
            else: # reads == 1 or reads == 2 and pairedend_run
                cycles_per_read = cycles/reads
                cycle_list = [cycles_per_read for _ in range(reads)]
                indexed_reads = False
        else:
            reads = None
            cycle_list = None
            pairedend_run = None
            indexed_reads = None
    
        return (reads, cycle_list, pairedend_run, indexed_reads)

    def get_reads_cycles_from_recipe(self):
    
        # Look in run directory for a file starting "Recipe_*".
        #
        # If single read: "Recipe_GA2_<cycles>Cycle_SR_v8.3.xml", or
        # If paired end: "Recipe_GA2_2x<cycles>Cycle_v8.3.xml"
        #
        run_dir = self.get_path()
        for entry in os.listdir(run_dir):
            if (entry.startswith("Recipe_GA2_") and entry.endswith(".xml")):
                # Is it single read?
                cycle_matches = re.search("^Recipe_GA2_(2x)?(\d+)Cycle",entry)
                if (cycle_matches is not None):
                    cycles = int(cycle_matches.group(2))
                    # If the (2x) group matched, then we have paired end, o/w, single read.
                    if (cycle_matches.group(1) is not None):
                        reads = 2
                        cycle_list = [cycles, cycles]
                        pairedend_run = True
                    else:
                        reads = 1
                        cycle_list = [cycles]
                        pairedend_run = False
                    break
        else:
            # The default answer if nothing found.
            reads = None
            cycle_list = None
            pairedend_run = None
                
        return (reads, cycle_list, pairedend_run, False)

    #
    # Get the cycle count for various stages from DATA_STATUS_PATH.
    #
    # Possible values for tag are:
    #  ImgCycle
    #  ScoreCycle
    #  CallCycle
    #
    # NOTE: the entire Data/reports directory is absent for MiSeq.
    @classmethod
    def get_cycle_tag_from_statusupdate(cls, run_dir, tag):

        cycles = None
        #
        # Open the Status.xml file as an XML document.
        #
        run_status_path = os.path.join(run_dir, RunDir.DATA_STATUSUPDATE_PATH)
        if os.path.exists(run_status_path):
            status_doc = xml.dom.minidom.parse(run_status_path)

            # Get <"tag">.
            cycles_node = status_doc.getElementsByTagName(tag)[0]
            if cycles_node:
                cycles = int(cycles_node.firstChild.nodeValue)

        return cycles


    def get_machine_from_dataintensitiesconfigxml(self):

        # Get machine name from Data/Intensities/config.xml (this works for MiSeq and GAIIx).
        # XML Path: <ImageAnalysis><RunParameters><Instrument>
        run_info_file = os.path.join(self.get_path(), "Data", "Intensities", "config.xml")
        if os.path.exists(run_info_file):
            run_info_doc = xml.dom.minidom.parse(run_info_file)

            # Get <RunInfo><Run><Instrument>.
            img_node = run_info_doc.getElementsByTagName("ImageAnalysis")[0]
            run_node = img_node.getElementsByTagName("RunParameters")[0]
            instrum_node = run_node.getElementsByTagName("Instrument")[0]

            return instrum_node.firstChild.nodeValue
        else:
            return None

    def get_machine_from_runinfoxml(self):

        # Get machine name from RunInfo.xml (this works for HiSeq and GAIIx).
        # XML Path: <RunInfo><Run><Instrument>
        run_info_file = os.path.join(self.get_path(), "RunInfo.xml")
        if os.path.exists(run_info_file):
            run_info_doc = xml.dom.minidom.parse(run_info_file)

            # Get <RunInfo><Run><Instrument>.
            runinfo_node = run_info_doc.getElementsByTagName("RunInfo")[0]
            run_node = runinfo_node.getElementsByTagName("Run")[0]
            instrum_node = run_node.getElementsByTagName("Instrument")[0]

            return instrum_node.firstChild.nodeValue
        else:
            return None


    # This function returns the run platform type for this run dir,
    # calculating it if necessary.


    def get_platform(self):
        return self.platform:

    def set_platform(self):
        platform = None
        # Platform is HiSeq if file "runParameters.xml" has an
        # entry <RunParameters><Setup><ApplicationName> which
        # includes "HiSeq" (Also MiSeq if "MiSeq").
        run_params_path = os.path.join(self.get_path(),"runParameters.xml")
        if os.path.exists(run_params_path):
            run_params_doc = xml.dom.minidom.parse(run_params_path)

            # Get <Configuration><NumberOfReads>.
            run_params_node = run_params_doc.getElementsByTagName("RunParameters")[0]
            setup_node = run_params_node.getElementsByTagName("Setup")[0]
            appname_node = setup_node.getElementsByTagName("ApplicationName")[0]

            hiseq_match = re.search("^HiSeq", appname_node.firstChild.nodeValue)
            if hiseq_match:
                platform = RunDir.PLATFORM_ILLUMINA_HISEQ
            else:
                miseq_match = re.search("^MiSeq", appname_node.firstChild.nodeValue)
                if miseq_match:
                    platform = RunDir.PLATFORM_ILLUMINA_MISEQ
                else:
                    platform = RunDir.PLATFORM_UNKNOWN

        # Platform is GA if there exists a "EventScripts" directory.
        else:
            event_scripts_path = os.path.join(self.get_path(), "EventScripts")

            if (os.path.exists(event_scripts_path) and
                os.path.isdir(event_scripts_path)):
                platform = RunDir.PLATFORM_ILLUMINA_GA
            else:
                # Otherwise, platform is unknown.
                platform = RunDir.PLATFORM_UNKNOWN
        if not platform:
            raise Exception("Cannot get platform for run {run}! Exiting ...".format(run=self.dir))
        self.platform = platform


    # This function returns the control software version for this run dir,
    # loading it if necessary.
    def get_control_software_version(self, integer=False):

        control_software_type = "unknown" # overwrite if found.
        control_software_version = "unknown" # overwrite if found.

        if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
            control_software_type = 'SCS'
        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:
            control_software_type = 'HCS'
        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_MISEQ:
            control_software_type = 'MCS'

        if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
                
            # Control software version is in file "RunLog-*.xml" (can pick any one).
            # entry <Software>
            run_log_files = glob.glob(os.path.join(self.get_path(),"RunLog_*"))
            if len(run_log_files):
                run_log_doc = xml.dom.pulldom.parse(run_log_files[0])

                # Get <Software>[version attribute]
                for (event,node) in run_log_doc:
                    if event=="START_ELEMENT" and node.tagName=="Software":
                        control_software_version = node.getAttribute("version")
                        break
                    
        elif (self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ or
              self.get_platform() == RunDir.PLATFORM_ILLUMINA_MISEQ):

            # Control software version is in file "runParameters.xml",
            # entry <RunParameters><Setup><ApplicationVersion> .
            run_params_path = os.path.join(self.get_path(),"runParameters.xml")
            if os.path.exists(run_params_path):
                run_params_doc = xml.dom.minidom.parse(run_params_path)
                
                # Get <RunParameters><Setup><ApplicationVersion> .
                run_params_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                setup_node = run_params_node.getElementsByTagName("Setup")[0]
                appvers_node = setup_node.getElementsByTagName("ApplicationVersion")[0]

                control_software_version =  appvers_node.firstChild.nodeValue

        else:
            print >> sys.stderr, "RunDir.get_control_software_version(): Platform unknown"
            return None

        return "%s %s" % (control_software_type, control_software_version)

    def get_seq_kit_version(self):

        if not self.seq_kit_version:

            if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:

                self.seq_kit_version = "version5"  # Don't know how to find this from files.

            elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:

                # Control software version is in file "runParameters.xml",
                # entry <RunParameters><Setup><Flowcell> .
                run_params_path = os.path.join(self.get_path(),"runParameters.xml")
                if os.path.exists(run_params_path):
                    run_params_doc = xml.dom.minidom.parse(run_params_path)

                    # Get <RunParameters><Setup><Flowcell>.
                    run_params_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    setup_node = run_params_node.getElementsByTagName("Setup")[0]
                    flowcell_node = setup_node.getElementsByTagName("Flowcell")[0]

                    if flowcell_node.firstChild.nodeValue.endswith('v3'):
                        self.seq_kit_version = 'hiseq_v3'
                    else:
                        self.seq_kit_version = 'hiseq_v1'

            elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_MISEQ:

                cycles = self.get_cycle_list()
                if cycles is not None and cycles[0] == 150:
                    self.seq_kit_version = "miseq_v1"
                else:
                    self.seq_kit_version = "miseq_v2"  # Don't know how to find this from files.

            else:
                 print >> sys.stderr, "RunDir.get_seq_kit_version(): Platform unknown"
                 return None

        return self.seq_kit_version


    def get_tile_list(self):
        # Make tile lists per platform.
        # Output: list of integers.
        tile_list = []
        platform = self.get_platform()
        if platform == RunDir.PLATFORM_ILLUMINA_GA:
            tile_list.extend(range(1,121)) # 1..120
            
        elif platform == RunDir.PLATFORM_ILLUMINA_HISEQ:
            tiles    = range(1,9)  # 8 tiles per swath per surface

            #
            # Tile list breakdown:
            #   HCS 1.1.37 uses SeqKit v1, and has two digit tile numbers.
            #   Otherwise, SeqKit v1 uses four digit tile numbers and has 2 swaths.
            #              SeqKit v3 uses four digit tile numbers and has 3 swaths.
            #
            sw_version = self.get_control_software_version()
            seq_kit_version = self.get_seq_kit_version()
            if sw_version.startswith("1.1.37"):
                # Tiles 1..8, 21..28, 41..48, 61..68
                swaths = ['',2,4,6]
                tile_list = [int(str(s)+str(t)) for s in swaths for t in tiles]

            elif seq_kit_version.endswith("v1"):
                # Tiles 1101..1108, 1201..1208, 2101..2108, 2201..2208
                surfaces = [1,2]  # 1 for top, 2 for bottom
                swaths   = [1,2]
                tile_list = [int(str(s)+str(w)+"%02d"%t) for s in surfaces for w in swaths for t in tiles]

            elif seq_kit_version.endswith("v3"):
                # Tiles 1101..1108, 1201..1208, 1301..1301, 2101..2108, 2201..2208, 2301..2308
                surfaces = [1,2]  # 1 for top, 2 for bottom
                swaths   = [1,2,3]
                tile_list = [int(str(s)+str(w)+"%02d"%t) for s in surfaces for w in swaths for t in tiles]

            else:
                print >> sys.stderr, "RunDir.get_tile_list(): %s: HiSeq SW version %s/seq kit version %s combo unknown" % (self.get_dir(), sw_version, seq_kit_version)
                tile_list = None

        elif platform == RunDir.PLATFORM_ILLUMINA_MISEQ:
            # Tiles 1101..1112
            tile_list = range(1101,1113)

        else:  # platform is UNKNOWN
            tile_list = None

        return tile_list

    def get_lane_list(self):

        platform = self.get_platform()
        if platform == self.PLATFORM_ILLUMINA_GA or platform == self.PLATFORM_ILLUMINA_HISEQ:
            # 8 lanes for GA and HiSeq
            lane_list = range(1,9)
        elif platform == self.PLATFORM_ILLUMINA_MISEQ:
            # 1 lane for MiSeq
            lane_list = [1]
        else:
            # Platform is unknown -- no lane list.
            print >> sys.stderr, "get_lane_list(): %s: Platform unknown" % self.get_dir()
            lane_list = None

        return lane_list

###
#
# Test code: displays the fields of the run directories given on the command line.
#
# Switches:
#   --validate:  Validates run directories
#   --diskUsage: Displays disk usage in Gb for run directories.
#
###
if __name__ == "__main__":
    from optparse import OptionParser
    import sys
    
    usage = "%prog [options] run_dir+"
    parser = OptionParser(usage=usage)

    parser.add_option("-v", "--validate", dest="validate", action="store_true",
                      default=False,
                      help='Run validation on run directory [default = false]')
    parser.add_option("-c", "--cif", dest="cif", action="store_true",
                      default=False,
                      help='When validating, include .cif files [default = False]')
    parser.add_option("-d", "--diskUsage", dest="disk_usage", action="store_true",
                      default=False,
                      help='Display the disk usage for the run directory [default = false]')

    (opts, args) = parser.parse_args()

    if not len(args):
        print >> sys.stderr, os.path.basename(__file__), ": No run directories given"
        sys.exit(1)

    for arg in args:
        (root, dir) = os.path.split(os.path.abspath(arg))
            
        rundir = RunDir(root,dir)
        print rundir.str(),

        if opts.disk_usage:
            disk_usage = rundir.get_disk_usage()

            print "  Disk Usage: %d Gb" % disk_usage

        if opts.validate:
            print
            if rundir.get_status() != RunDir.STATUS_RUN_ABORTED:
                if rundir_utils.validate(rundir,cif=opts.cif,verbose=True):
                    print "%s validated" % dir
                else:
                    print "%s has problems" % dir
            else:
                print "%s was aborted" % dir
