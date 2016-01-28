''' Descripton: Copy of DNAnexusUpload class that is designed to be integrated
    into autocopy. This standalone version is only intested for testing and
    debugging.
'''


import os
import re
import dxpy
import argparse
import subprocess

from bin.rundir import RunDir

class DNAnexusUpload:

    def __init__(self, rundir, LOG_FILE, rta_version=None):
        self.rundir = rundir            # RunDir object
        self.rta_version = rta_version
        self.LOG_FILE = LOG_FILE
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
        if self.rta_version < 2:
            self.lane_tar_files = tar_rta_v1_rundir()
        elif self.rta_versoin >= 2:
            self.lane_tar_files = tar_rta_v2_rundir()

        # Upload files
        for lane_index in lane_tar_files:
            lane_tar = lane_tar_files[lane_index]
            dxids = self.upload_lane(rundir=self.rundir, lane_index=lane_index, 
                                     lane_tar=lane_tar, interop=self.interop_tar, 
                                     metadata=self.metadata_tar)
            self.file_dxids[lane_index] = ids

    def get_rta_version(self):
        params_file = os.path.join(rundir.get_path(), 'runParameters.xml')
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
        interop_tar = os.path.join(self.rundir.get_path(), 'InterOp.tar.gz')
        interop_tar_list = ['tar', '-C', self.rundir.get_path(), 
                                '-czvf', interop_tar, 'InterOp'
                            ]
        interop_tar_proc = subprocess.Popen(interop_tar_list, stdout=self.LOG_FILE, 
                                            stderr=self.LOG_FILE
        # Check that InterOp tar file exists
        return interop_tar

    def tar_metadata(self):
        ''' Description:
        '''

        metadata_tar = '%s.metadata.tar' % rundir.get_dir() # get_dir() = basename/run name

        meta_tar_list = ['tar', '-C', '%s' % rundir.get_path(), 
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
    
    def tar_rta_v1_rundir(self):
        ''' Description: Need to tar the lane directory in Data/Intensities/L00N as well as in
            Data/Intensities/BaseCalls/L00N.
        '''
        
        lane_tar_files = []
        for filename in os.listdir(self.basecalls_dir):
            if fnmatch.fnmatch(filename, 'L0*'):
                lane_name = filename
                lane_index = int(filename[-1:])
                lane_tar = '%s.L%d.tar' % (rundir.get_dir(), lane_index)
                intens_rel_path = os.path.join('Data', 'Intensities', lane_name)
                basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
                # Create tarball for lane data
                lane_tar_list = ['tar', '-C', rundir.get_path(),
                                 '-cf', lane_tar, intens_rel_path, basecall_rel_path
                                ]
                lane_tar_proc = subprocess.Popen(lane_tar_list, stdout=self.LOG_FILE, 
                                                 stderr=self.LOG_FILE)
                lane_tar_files.append(lane_tar)
        return lane_tar_files
    
    def tar_rta_v2_rundir(self, rundir):
        ''' Description: With v2 Real Time Analysis (RTA) software, all of the lane files
            have been aggregated into a single directory in Data/Intensities/BaseCalls/L00N
        '''

        lane_tar_files = []
        basecalls_dir = os.path.join(rundir.get_path(), 'Data', 'Intensities', 'BaseCalls')

        for filename in os.listdir(basecalls_dir):
            if fnmatch.fnmatch(filename, 'L0*'):
                lane_name = filename
                lane_index = int(filename[-1:])
                lane_tar = '%s.L%d.tar' % (rundir.get_dir(), lane_index)
                basecall_rel_path = os.path.join('Data', 'Intensities', 'BaseCalls', lane_name)
                # Create tarball for lane data
                lane_tar_list = ['tar', '-C', rundir.get_path(),
                                 '-cf', lane_tar, basecall_rel_path
                                    ]
                lane_tar_proc = subprocess.Popen(lane_tar_list, stdout=self.LOG_FILE, 
                                                 stderr=self.LOG_FILE)
                lane_tar_files[lane_index] = lane_tar
        return lane_tar_files

    def upload_lane(self, rundir, lane_index, lane_tar, interop, metadata):
        project_dxid = self.make_dnanexus_project(rundir, lane_index)

        interop_dxfile = dxpy.upload_local_file(filename=interop, project=project_dxid, folder='/')
        metadata_dxfile = dxpy.upload_local_file(filename=metadata, project=project_dxid, folder='/')
        lane_dxfile = dxpy.upload_local_file(filename=lane_tar, project=project_dxid, folder='/')
        return [interop_dxfile.get_id(), metadata_dxfile.get_id(), lane_dxfile.get_id()]

    @classmethod
    def make_dnanexus_project(self, rundir, lane_index, contains_phi=False):
        project_name = '%s_L%d' % (rundir, lane_index)

        # Check whether project already exists
        project_generator = dxpy.find_projects(name=project_name, name_mode='exact')
        projects = list(project_generator)
        if len(projects) == 1:
            project = projects[0]
            project_dxid = project['id']
        elif len(projects) == 0:
            # Create new DNAnexus project
            input_params = {'name': project_name,
                              'containsPHI': contains_phi,
                           }
            project_dxid = dxpy.api.project_new(input_params=input_params)
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

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--run_path', 
                        help='Full path of sequencing run directory')
    parser.add_argument('-l', '--log_file', help='Name of log file')
    args = parser.parse_args()

    path_elements = os.path.split(args.run_path)
    dirname = path_elements[0]
    directory = path_elements[1]

    rundir = RunDir(root=dirname, directory=basename)

    upload = DNAnexusUpload(rundir, args.log_file)
    upload.run()


if __name__ == "__main__":
    main()