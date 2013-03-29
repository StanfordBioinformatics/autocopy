#!/usr/bin/env python

import glob
import os
import os.path
import platform
import re
import subprocess
import sys
import xml.dom.minidom
import xml.dom.pulldom

import lims
import rundir_utils

#
# The RunDir object encapsulates all the functionality associated with an Illumina run directory.
#
# Files read from run directory to support the methods below:
#  Data/reports/Status.xml
#  Data/reports/StatusUpdate.xml
#  Recipe_GA2_*Cycle_v8.3.xml [for GA runs]
#  runParameters.xml [for HiSeq runs]
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
        "Base Calling Complete (SingleRead)",
        "Base Calling Complete (Read 1)",
        "Base Calling Complete (Read 2)",
        "Base Calling Complete (Read 3)",
        "Copy Started",
        "Copy Complete",
        "Copy Failed",
        "Run Aborted"
        ]

    # Status constants
    STATUS_MAX_INDEX = len(STATUS_STRS)
    (STATUS_INITIALIZED,
     STATUS_STARTED,
     STATUS_IMAGEANALYSIS_COMPLETE_SINGLEREAD,
     STATUS_IMAGEANALYSIS_COMPLETE_READ1,
     STATUS_IMAGEANALYSIS_COMPLETE_READ2,
     STATUS_IMAGEANALYSIS_COMPLETE_READ3,
     STATUS_BASECALLING_COMPLETE_SINGLEREAD,
     STATUS_BASECALLING_COMPLETE_READ1,
     STATUS_BASECALLING_COMPLETE_READ2,
     STATUS_BASECALLING_COMPLETE_READ3,
     STATUS_COPY_STARTED,
     STATUS_COPY_COMPLETE,
     STATUS_COPY_FAILED,
     STATUS_RUN_ABORTED) = range(STATUS_MAX_INDEX)

    # Status files 
    STATUS_FILES= [
        None,
        "First_Base_Report.txt",
        "ImageAnalysis_Netcopy_complete_SINGLEREAD.txt",
        "ImageAnalysis_Netcopy_complete_READ1.txt",
        "ImageAnalysis_Netcopy_complete_READ2.txt",
        "ImageAnalysis_Netcopy_complete_READ3.txt",
        "Basecalling_Netcopy_complete_SINGLEREAD.txt",
        "Basecalling_Netcopy_complete_READ1.txt",
        "Basecalling_Netcopy_complete_READ2.txt",
        "Basecalling_Netcopy_complete_READ3.txt",
        "Autocopy_started.txt",
        "Autocopy_complete.txt",
        "Autocopy_failed.txt",
        "Run_aborted.txt"
        ]
    
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
        "Illumina HiSeq"
    ]
    PLATFORM_MAX_INDEX = len(PLATFORM_NAMES)
    (PLATFORM_UNKNOWN,
     PLATFORM_ILLUMINA_GA,
     PLATFORM_ILLUMINA_HISEQ) = range(PLATFORM_MAX_INDEX)
    
    ###
    # Constructor
    ###
    def __init__(self, root, directory):
        self.root = root
        self.dir = directory

        self.reads = None
        self.cycle_list = None
        self.platform = None
        self.control_software_version = None
        self.seq_kit_version = None

        self.status = RunDir.STATUS_INITIALIZED
        self.update_status()

        self.copy_proc = None
        self.copy_start_time = None
        self.copy_end_time = None

        self.disk_usage = None

        # Parse directory name to get start date, machine, and flowcell.
        # TODO: Get start date, machine, flowcell from files, not from run name.
        name_parse = directory.split("_",4)
        if len(name_parse) == 4:
            self.start_date = name_parse[0]
            self.machine = name_parse[1]
            self.number = name_parse[2]
            self.flowcell = name_parse[3].split('_',1)[0]
        elif len(name_parse) == 3:
            self.start_date = name_parse[0]
            self.machine = name_parse[1]
            self.number = None
            self.flowcell = name_parse[2].split('_',1)[0]
        else:
            self.start_date = None
            self.machine = None
            self.number = None
            self.flowcell = None


    def str(self):
        s = ""
        s += "<RUNDIR %s>\n" % self.get_dir()
        s += "  Root: %s\n" % self.get_root()
        s += "  Status: %s\n" % self.get_status_string()
        s += "  Reads: %d\n" % self.get_reads()
        s += "  Cycles: %s\n" % " ".join(map(lambda d: str(d), self.get_cycle_list()))
        s += "\n"
        s += "  Extracted Cycles: %s/%s\n" % (self.get_extracted_cycle(), self.get_total_cycles())
        s += "  Called Cycles: %s/%s\n" % (self.get_called_cycle(),self.get_total_cycles())
        s += "  Scored Cycles: %s/%s\n" % (self.get_scored_cycle(),self.get_total_cycles())
        s += "\n"
        s += "  Platform: %s\n" % RunDir.PLATFORM_NAMES[self.get_platform()]
        s += "  SW Version: %s\n" % self.get_control_software_version()
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

    def get_start_date(self):
        return self.start_date
    def get_machine(self):
        return self.machine
    def get_number(self):
        return self.number
    def get_flowcell(self):
        return self.flowcell

    def get_reads(self):
        if (self.reads is None):
            self.find_reads_cycles()
        if (self.reads is None):
            return 0
        else:
            return self.reads
    def get_cycle_list(self):
        if (self.cycle_list is None):
            self.find_reads_cycles()
        if (self.cycle_list is None):
            return []
        else:
            return self.cycle_list
    def get_total_cycles(self):
        return sum(self.get_cycle_list())

    def get_extracted_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"ImgCycle")
    def get_called_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"CallCycle")
    def get_scored_cycle(self):
        return RunDir.get_cycle_tag_from_statusupdate(self.get_path(),"ScoreCycle")

    def get_status(self):
        return self.status
    def get_status_string(self):
        return RunDir.STATUS_STRS[self.status]
    
    def update_status(self):
        # Find the highest numbered status (latest in workflow) that
        #  is represented by a file in the run dir.
        for status in range(RunDir.STATUS_MAX_INDEX - 1, RunDir.STATUS_INITIALIZED, -1):
            if os.path.exists(os.path.join(self.get_path(), RunDir.STATUS_FILES[status])):
                self.status = status
                break
        else:
            self.status = RunDir.STATUS_INITIALIZED

        return self.status

    def drop_status_file(self):
        if (RunDir.STATUS_FILES[self.status] is not None):
            fp = open(os.path.join(self.get_path(), RunDir.STATUS_FILES[self.status]),"w")
            fp.close()

    def undrop_status_file(self):
        if (RunDir.STATUS_FILES[self.status] is not None and
            self.status > RunDir.STATUS_BASECALLING_COMPLETE_READ3):
            os.remove(os.path.join(self.get_path(), RunDir.STATUS_FILES[self.status]))

    def is_finished(self):
        if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
            if self.get_reads() == 1:
                return self.status == RunDir.STATUS_BASECALLING_COMPLETE_SINGLEREAD
            else:
                return self.status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
            
        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:
            sw_version = self.get_control_software_version(integer=True)
            if sw_version == 1137:  # "1.1.37"
                if self.get_reads() == 1:
                    return self.status == RunDir.STATUS_BASECALLING_COMPLETE_SINGLEREAD
                else:
                    return self.status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
            elif sw_version >= 1308: # "1.3.8", "1.4.5", "1.4.8"
                if self.get_reads() == 1:
                    return self.status == RunDir.STATUS_BASECALLING_COMPLETE_READ1
                elif self.get_reads() == 2:
                    return self.status == RunDir.STATUS_BASECALLING_COMPLETE_READ2
                elif self.get_reads() == 3:
                    return self.status == RunDir.STATUS_BASECALLING_COMPLETE_READ3
                else:
                    print >> sys.stderr, "RunDir.is_finished(): %s: Unexpected number of reads: %d" % (self.get_dir(), self.get_reads())
                    return False
            else:
                print >> sys.stderr, "RunDir.is_finished(): %s: HiSeq SW version %s unknown" % (self.get_dir(), sw_version)
                return False
        else:
            print >> sys.stderr, "RunDir.is_finished(): %s: Platform unknown" % (self.get_dir())
            return False


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
            print >> sys.stderr, "get_disk_usage(): Unknown platform %s" % (platfm)
            self.disk_usage = None
            return self.disk_usage

        # Add the run directory path to the command.
        du_cmd_list.append(self.get_path())

        # Run the disk usage command and get the output.
        du_cmd_pipe = subprocess.Popen(du_cmd_list, stdout=subprocess.PIPE)
        (du_stdout, du_stderr) = du_cmd_pipe.communicate()

        # Parse the output.
        (disk_usage, dir) = du_stdout.split()
        self.disk_usage = int(disk_usage)

        return self.disk_usage


    ###
    # Other methods
    ###
    def find_reads_cycles(self):
        
        (reads, cycle_list) = RunDir.get_reads_cycles_from_status(self.get_path())
        if (reads is None):
            (reads, cycle_list) = RunDir.get_reads_cycles_from_recipe(self.get_path())
        self.reads = reads
        self.cycle_list = cycle_list

    @classmethod
    def get_reads_cycles_from_status(cls, run_dir):
            
        #
        # Open the Status.xml file as an XML document.
        #
        run_status_path = os.path.join(run_dir, RunDir.DATA_STATUS_PATH)
        if os.path.exists(run_status_path):
            status_doc = xml.dom.minidom.parse(run_status_path)

            # Get <Configuration><NumberOfReads>.
            config_node = status_doc.getElementsByTagName("Configuration")[0]
            num_reads_node = config_node.getElementsByTagName("NumberOfReads")[0]
            reads = int(num_reads_node.firstChild.nodeValue)

            # Get <NumberOfCycles>.
            num_cycles_node = status_doc.getElementsByTagName("NumCycles")[0]
            cycles = int(num_cycles_node.firstChild.nodeValue)
            
            if (reads == 3):
                cycles -= RunDir.ILLUMINA_BARCODE_LENGTH
                cycles_per_read = cycles/(reads-1)
                cycle_list = [cycles_per_read, RunDir.ILLUMINA_BARCODE_LENGTH, cycles_per_read]
            else:
                cycles_per_read = cycles/reads
                cycle_list = [cycles_per_read for _ in range(reads)]
        else:
            reads = None
            cycle_list = None
    
        return (reads, cycle_list)

    @classmethod
    def get_reads_cycles_from_recipe(cls, run_dir):
    
        # Look in run directory for a file starting "Recipe_*".
        #
        # If single read: "Recipe_GA2_<cycles>Cycle_SR_v8.3.xml", or
        # If paired end: "Recipe_GA2_2x<cycles>Cycle_v8.3.xml"
        #
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
                    else:
                        reads = 1
                        cycle_list = [cycles]
                    break
        else:
            # The default answer if nothing found.
            reads = None
            cycle_list = None        
                
        return (reads, cycle_list)

    #
    # Get the cycle count for various stages from DATA_STATUS_PATH.
    #
    # Possible values for tag are:
    #  ImgCycle
    #  ScoreCycle
    #  CallCycle
    #
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

    # This function returns the run platform type for this run dir,
    # calculating it if necessary.
    def get_platform(self):

        if not self.platform:

            # Platform is HiSeq if file "runParameters.xml" has an
            # entry <RunParameters><Setup><ApplicationName> which
            # includes "HiSeq".
            run_params_path = os.path.join(self.get_path(),"runParameters.xml")
            if os.path.exists(run_params_path):
                run_params_doc = xml.dom.minidom.parse(run_params_path)

                # Get <Configuration><NumberOfReads>.
                run_params_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                setup_node = run_params_node.getElementsByTagName("Setup")[0]
                appname_node = setup_node.getElementsByTagName("ApplicationName")[0]

                hiseq_match = re.search("^HiSeq", appname_node.firstChild.nodeValue)
                if (hiseq_match):
                    self.platform = RunDir.PLATFORM_ILLUMINA_HISEQ
                else:
                    self.platform = RunDir.PLATFORM_UNKNOWN

            # Platform is GA if there exists a "EventScripts" directory.
            else:
                event_scripts_path = os.path.join(self.get_path(), "EventScripts")

                if (os.path.exists(event_scripts_path) and
                    os.path.isdir(event_scripts_path)):
                    self.platform = RunDir.PLATFORM_ILLUMINA_GA
                else:
                    # Otherwise, platform is unknown.
                    self.platform = RunDir.PLATFORM_UNKNOWN

        return self.platform

    # This function returns the control software version for this run dir,
    # loading it if necessary.
    def get_control_software_version(self, integer=False):

        if not self.control_software_version:

            if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
                
                # Control software version is in file "RunLog-*.xml" (can pick any one).
                # entry <Software>
                run_log_files = glob.glob(os.path.join(self.get_path(),"RunLog_*"))
                if len(run_log_files):
                    run_log_doc = xml.dom.pulldom.parse(run_log_files[0])

                    # Get <Software>[version attribute]
                    for (event,node) in run_log_doc:
                        if event=="START_ELEMENT" and node.tagName=="Software":
                            self.control_software_version = node.getAttribute("version")
                            break
                    
            elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:

                # Control software version is in file "runParameters.xml",
                # entry <RunParameters><Setup><ApplicationVersion> .
                run_params_path = os.path.join(self.get_path(),"runParameters.xml")
                if os.path.exists(run_params_path):
                    run_params_doc = xml.dom.minidom.parse(run_params_path)

                    # Get <RunParameters><Setup><ApplicationVersion> .
                    run_params_node = run_params_doc.getElementsByTagName("RunParameters")[0]
                    setup_node = run_params_node.getElementsByTagName("Setup")[0]
                    appvers_node = setup_node.getElementsByTagName("ApplicationVersion")[0]

                    self.control_software_version =  appvers_node.firstChild.nodeValue

            else:
                print >> sys.syserr, "RunDir.get_control_software_version(): Platform unknown"
                return None

        # If user asked for integer output, convert the SW version to an integer.
        if integer and self.control_software_version:
            digits = self.control_software_version.split('.')

            if len(digits) >= 3:
                version_int = int(digits[0])*1000 + int(digits[1])*100 + int(digits[2])
                return version_int
            else:
                print >> sys.stderr, "RunDir.get_control_software_version(): %s is not at least 3 digits" % (self.control_software_version)
                return 0
        else:
            return self.control_software_version


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

            else:
                 print >> sys.syserr, "RunDir.get_seq_kit_version(): Platform unknown"
                 return None

        return self.seq_kit_version


    def get_tile_list(self):
        # Make tile lists per platform.
        # Output: list of integers.
        tile_list = []
        if self.get_platform() == RunDir.PLATFORM_ILLUMINA_GA:
            tile_list.extend(range(1,121)) # 1..120
            
        elif self.get_platform() == RunDir.PLATFORM_ILLUMINA_HISEQ:
            tiles    = range(1,9) # 8 tiles per swath per surface

            # Each version of the HCS seem to have a new way to number tiles.
            sw_version = self.get_control_software_version()
            if sw_version.startswith("1.1.37"):
                # Tiles 1..8, 21..28, 41..48, 61..68
                swaths = ['',2,4,6]
                tile_list = [int(str(s)+str(t)) for s in swaths for t in tiles]

            elif sw_version.startswith("1.3.8") or sw_version.startswith("1.4.5"):
                # Tiles 1101..1108, 1201..1208, 2101..2108, 2201..2208
                surfaces = [1,2]  # 1 for top, 2 for bottom
                swaths   = [1,2]
                tile_list = [int(str(s)+str(w)+"%02d"%t) for s in surfaces for w in swaths for t in tiles]

            elif sw_version.startswith("1.4.8"):
                # Tiles 1101..1108, 1201..1208, 1301..1301, 2101..2108, 2201..2208, 2301..2308
                surfaces = [1,2]  # 1 for top, 2 for bottom
                swaths   = [1,2,3]
                tile_list = [int(str(s)+str(w)+"%02d"%t) for s in surfaces for w in swaths for t in tiles]

            else:
                print >> sys.stderr, "RunDir.get_tile_list(): %s: HiSeq SW version %s unknown" % (self.get_dir(), sw_version)
                tile_list = None

        else:  # platform is UNKNOWN
            tile_list = None

        return tile_list


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
    parser.add_option("-c", "--no_cif", dest="no_cif", action="store_true",
                      default=False,
                      help='When validating, ignore .cif files [default = false]')
    parser.add_option("-d", "--diskUsage", dest="disk_usage", action="store_true",
                      default=False,
                      help='Display the disk usage for the run directory [default = false]')
    parser.add_option("-l", "--addToLIMS", dest="add_to_lims", action="store_true",
                      default=False,
                      help='Add this run directory to the LIMS [default = false]')

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
                if rundir_utils.validate(rundir,no_cif=opts.no_cif,verbose=True):
                    print "%s validated" % dir
                else:
                    print "%s has problems" % dir
            else:
                print "%s was aborted" % dir

        if opts.add_to_lims:
            if lims.lims_run_create(rundir):
                print "Run dir record created in LIMS."
            else:
                print "Run dir record COULD NOT be created in LIMS."
