#!/usr/local/bin/python2.7

''' Descripton: Copy of DNAnexusUpload class that is designed to be integrated
    into autocopy. This standalone version is only intested for testing and
    debugging.
'''


import os
import re
import dxpy
import time
import fnmatch
import argparse
import subprocess

from distutils.version import StrictVersion

from bin.rundir import RunDir

class DNAnexusUpload:

    def __init__(self, rundir, LOG_FILE, rta_version=None, lims_url, lims_token):
        self.rundir = rundir            # RunDir object
        self.rta_version = rta_version
        self.LOG_FILE = LOG_FILE
        self.lims_url = lims_url
        self.lims_token = lims_token

        self.interop_tar = None
        self.metadata_tar = None
        self.lane_tar_files = None
        self.file_dxids = {}

    def run(self):
        if not self.rta_version:
            self.rta_version = self.get_rta_version()

        # Tar interop and metadata files
        self.interop_tar = self.tar_interop_dir()
        self.metadata_tar = self.tar_metadata()

        # Tar lane files
        print 'RTA versions is: %s' % self.rta_version
        if StrictVersion(self.rta_version) < StrictVersion('2.0.0'):
        print 'Tarring files according to RTA v1 pattern'
        self.metadata_tar = self.tar_rta_v1_metadata(self.rundir)
            self.lane_tar_files = self.tar_rta_v1_rundir(self.rundir)
        elif StrictVersion(self.rta_version) >= StrictVersion('2.0.0'):
        print 'Tarring files according to RTA v2 pattern'
        self.metadata_tar = self.tar_rta_v2_metadata(self.rundir)
            self.lane_tar_files = self.tar_rta_v2_rundir(self.rundir)

        # Upload files
        for lane_index in self.lane_tar_files:
            lane_tar = self.lane_tar_files[lane_index]
            
            project_dxid = self.make_dnanexus_project(lane_index, contains_phi=False)
            record_dxid = self.make_dnanexus_record(lane_index, project_dxid)
            dxids = self.upload_lane(project_dxid = project_dxid, 
                                     lane_tar = lane_tar
                                    )
            self.file_dxids[lane_index] = dxids

            # Update record properties to indicate that upload is complete
            property_params = {
                               'project': dashboard_project_dxid, 
                               'properties': {'upload_stats': 'complete'}
                              }
            dxpy.api.record_set_properties(object_id = record_dxid, 
                                           input_params = property_params
                                          )
        self.call_workflow_manager()


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
        
        tar_name = '%s.InterOp.tar.gz' % self.rundir.get_dir()
        #interop_tar = os.path.join(self.rundir.get_path(), tar_name)
        interop_tar = tar_name  

        if os.path.isfile(interop_tar):
            return interop_tar

            interop_tar_list = ['tar', '-C', self.rundir.get_path(), 
                                    '-czvf', interop_tar, 'InterOp'
                                ]
            interop_tar_proc = subprocess.Popen(interop_tar_list, stdout=self.LOG_FILE, 
                                                stderr=self.LOG_FILE)
            # Check that InterOp tar file exists
            return interop_tar

    def tar_rta_v2_metadata(self):
        ''' DEV: Change this to mirror formatting of tar_rta_v1_metadata method and specify files/dirs
            Description: 
        '''

        tar_name = '%s.metadata.tar.gz' % self.rundir.get_dir() # get_dir() = basename/run name
        #metadata_tar = os.path.join(self.rundir.get_path(), tar_name) 
        metadata_tar = tar_name 

        if os.path.isfile(metadata_tar):
            return metadata_tar
        
            meta_tar_list = ['tar', '-C', '%s' % self.rundir.get_path(), 
                             '--exclude', './Data/Intensities/BaseCalls',
                             '--exclude', './Data/Intensities/L*', 
                             '--exclude', './Images', 
                             '--exclude', './Logs', 
                             '-czvf', '%s' % metadata_tar, 
                             '.'
                            ]
                
            meta_tar_proc = subprocess.Popen(meta_tar_list, stdout=self.LOG_FILE, 
                                             stderr=self.LOG_FILE)
            return metadata_tar
    
    def tar_rta_v1_metadata(self):
        ''' Description:
        '''

        tar_name = '%s.metadata.tar.gz' % self.rundir.get_dir() # get_dir() = basename/run name
        #metadata_tar = os.path.join(self.rundir.get_path(), tar_name) 
        metadata_tar = tar_name 

        if os.path.isfile(metadata_tar):
            return metadata_tar
        
        mata_tar_list = ['tar', '-C', '%s' % self.rundir.get_path(),
                 '-czvf', '%s' % metadata_tar,
                 'runParameters.xml', 'RunInfo.xml',
                 'Data/RTALogs', 'Data/Intensitites/config.xml', 'Data/Intensities/BaseCalls/config.xml',
                 'RTAComplete.txt', 
                 'Data/Intensities/RTAConfiguration.xml', 'Data/Intensitites/config.xml',
                 'Data/Intensities/Offsets',
                 'Basecalling_Netcopy_complete_*', 'ImageAnalysis_Netcopy_complete_*',
                 'Recipe', 'Config']
                
            meta_tar_proc = subprocess.Popen(meta_tar_list, stdout=self.LOG_FILE, 
                                             stderr=self.LOG_FILE)
            return metadata_tar
    
    def tar_rta_v1_rundir(self, rundir):
        ''' Description: Need to tar the lane directory in Data/Intensities/L00N as well as in
            Data/Intensities/BaseCalls/L00N.
        '''
        
        lane_tar_files = {}
        basecalls_dir = os.path.join(rundir.get_path(), 'Data', 'Intensities', 'BaseCalls')

        for filename in os.listdir(basecalls_dir):
                if fnmatch.fnmatch(filename, 'L0*'):
                    lane_name = filename
                    lane_index = int(filename[-1:])
                    tar_name = '%s.L%d.tar' % (rundir.get_dir(), lane_index)
            # lane_tar = os.path.join(self.rundir.get_path(), tar_name)
            lane_tar = tar_name
            # Check if tarball already exists. If not, create it.
            if os.path.isfile(lane_tar):
                lane_tar_files[lane_index] = lane_tar
            else:
                        intens_rel_path = os.path.join('Data', 'Intensities', lane_name)
                        basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
                        lane_tar_list = ['tar', '-C', rundir.get_path(),
                                         '-cf', lane_tar, intens_rel_path, basecall_rel_path
                                        ]
                        lane_tar_proc = subprocess.Popen(lane_tar_list, stdout=self.LOG_FILE, 
                                                         stderr=self.LOG_FILE)
                        lane_tar_files[lane_index] = lane_tar
            return lane_tar_files
    
    def tar_rta_v2_rundir(self, rundir):
        ''' Description: With v2 Real Time Analysis (RTA) software, all of the lane files
            have been aggregated into a single directory in Data/Intensities/BaseCalls/L00N
        '''

        lane_tar_files = {}
        basecalls_dir = os.path.join(rundir.get_path(), 'Data', 'Intensities', 'BaseCalls')

        for filename in os.listdir(basecalls_dir):
            if fnmatch.fnmatch(filename, 'L0*'):
                lane_name = filename
                lane_index = int(filename[-1:])
                tar_name = '%s.L%d.tar' % (rundir.get_dir(), lane_index)
        #lane_tar = os.path.join(self.rundir.get_path(), tar_name)
        lane_tar = tar_name
        # Check if tarball already exists. If not, create it.
        if os.path.isfile(lane_tar):
            lane_tar_files[lane_index] = lane_tar
        else :
                    basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
                    lane_tar_list = ['tar', '-C', rundir.get_path(),
                                     '-cf', lane_tar, basecall_rel_path
                                        ]
                    lane_tar_proc = subprocess.Popen(lane_tar_list, stdout=self.LOG_FILE, 
                                                     stderr=self.LOG_FILE)
                    lane_tar_files[lane_index] = lane_tar
        return lane_tar_files

    def upload_lane(self, project_dxid, lane_index, lane_tar):
        #project_dxid = self.make_dnanexus_project(rundir, lane_index)

        interop_dxfile = dxpy.upload_local_file(filename=self.interop_tar, project=project_dxid, folder='/')
        metadata_dxfile = dxpy.upload_local_file(filename=self.metadata_tar, project=project_dxid, folder='/')
        lane_dxfile = dxpy.upload_local_file(filename=lane_tar, project=project_dxid, folder='/')
        return [interop_dxfile.get_id(), metadata_dxfile.get_id(), lane_dxfile.get_id()]

    def get_lane_info(run_name, lane_index, lims_url, lims_token):
        """
        Function : Gets the runinfo dict provided by the scgpm_utils package for a given run. Returns the entire runinfo dict, unless 'lane' is
                 provided, in which case the sub-lane dict is returned.
        Args     : runName - str. Name of sequencing run.
                             lane - int or str. Number of the lane (i.e. 1,2,3,4,5,6,7,8).
        Returns  : dict.
        """
        
        #lims_url,lims_token = get_lims_credentials()
        conn = Connection(lims_url=lims_url, lims_token=lims_token)
        run_info = conn.getruninfo(run=runName)['run_info']
        lane_info =  run_info['lanes'][str(lane_index)]
        return lane_info

    def get_record_details(self, run_name, lane_index, lims_url, lims_token):
        ''' Inputs = email : client email address ("erin.mitsunaga@gmail.com")
                     lane : lane index ("1")
                     laneProject : lane project dxid
                     library : library name
                     mapping Reference : reference genome name ("Human Male (hg19")
                     run : run name
                     runProject : -- no longer relevant
                     uploadDate : UTC time in milliseconds ("1448905921940")
                     user : client name ("Erin Mitsunaga")
        '''

        conn = Connection(lims_url=lims_url, lims_token=lims_token)
        run_info = conn.getruninfo(run=runName)['run_info']
        lane_info =  run_info['lanes'][str(lane_index)]

        # Get mapping reference name
        if 'mapping_requests' in lane_info:
            reference = lane_info['mapping_requests'][0]['reference_sequence_name']
        else:
            reference = ""

        # Get UTC time in milliseconds
        utc_time_milli = int(round(time.time() * 1000))

        record_details = {
                          'email': lane_info['submitter_email'].strip(),
                          'lane': lane_index,
                          'laneProject': lane_project_dxid,
                          'library': lane_info['sample_name'].strip(),
                          'mapping Reference': reference,
                          'run': run_name,
                         #'runProject': "",
                          'uploadDate': utc_time_milli,
                          'user': lane_info['submitter'].strip()
                         }
        return record_details()

    def make_dnanexus_project(self, lane_index, contains_phi=False):
        project_name = '%s_L%d' % (self.rundir.get_dir(), lane_index)

        # Check whether project already exists
        project_generator = dxpy.find_projects(name=project_name, name_mode='exact')
        projects = list(project_generator)
        if len(projects) == 1:
        print 'Found existing project matching name %s' % project_name
            project = projects[0]
            project_dxid = project['id']
        elif len(projects) == 0:
            print 'Creating new DNAnexus project named %s' % project_name
            input_params = {'name': project_name,
                            'containsPHI': contains_phi
                           }
            project_dxid = dxpy.api.project_new(input_params=input_params)['id']
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
        #print project_dxid
        return project_dxid

    def make_dnanexus_record(self, lane_index, lane_project_dxid, dashboard_project_dxid):
        ''' DEV: dashboard_project_dxid will need to be specified in config file 
        '''

        run_name = self.rundir.get_dir()
        record_name = '%s_L%d' % (run_name, lane_index)

        record_generator = dxpy.find_data_objects(classname = 'record', 
                                                  name = record_name, 
                                                  name_mode = 'exact'
                                                 )
        records = list(record_generator)
        if len(records) == 1:
        print 'Found existing record matching name %s' % record_name
            record = records[0]
            record_dxid = record['id']
        
        elif len(records) == 0:
            print 'Creating new DNAnexus record named %s' % record_name

            # Create DXRecord object
            record_params = {
                             'project': dashboard_project_dxid,
                             'name': record_name,
                             'types': ['SCGPMRun']
                            }
            record_dxid = dxpy.api.record_new(input_params=record_params)['id']

            # Add record details
            record_details = self.get_record_details(run_name, 
                                                     self.lane_index, 
                                                     self.lims_url,
                                                     self.lims_token
                                                    )
            dxpy.api.record_set_details(object_id=record_dxid,input_params=record_details)

            # Change dashboard status to 'uploading'
            property_params = {
                               'project': dashboard_project_dxid, 
                               'properties': {
                                              'status': 'uploading',
                                              'upload_status': 'incomplete',
                                              'workflow_name': '',
                                              'workflow_dxid': ''
                                             }
                              }
            dxpy.api.record_set_properties(object_id=record_dxid, 
                                           input_params=property_params
                                          )
            dxpy.api.record_close(record_dxid)

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
        #print project_dxid
        return project_dxid

    def call_workflow_manager(self):
        manager_applet = dxpy.api.find_one_applet(name='manager_applet')
        manager_applet.run()

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--run_path', 
                        help='Full path of sequencing run directory')
    parser.add_argument('-l', '--log_file', help='Name of log file')
    args = parser.parse_args()

    path_elements = os.path.split(args.run_path)
    dirname = path_elements[0]
    basename = path_elements[1]

    rundir = RunDir(root=dirname, directory=basename)
    LOG_FILE = open(args.log_file, 'w')
    
    upload = DNAnexusUpload(rundir, LOG_FILE)
    upload.run()


if __name__ == "__main__":
    main()
