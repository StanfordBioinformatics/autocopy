import os
import os.path
import platform
import shutil
import subprocess
import sys
import tarfile

##########################################################################
#
# rundir_utils.py - Utilities which act on RunDirs
#
# These routines were too large to store in the RunDir object itself.
#
##########################################################################

#
# validate() confirms that a set of files necessary to analyze
#  an Illumina run directory exist and have non-zero size.
#
def validate(rundir, no_cif=False, verbose=False):

    # Confirms non-zero-size existence of:
    #  Data/
    #  Data/Intensities
    #  Data/Intensities/s_<lane>_00<tile>_pos.txt
    #  Data/Intensities/L00<lane>
    #  Data/Intensities/L00<lane>/C<cyc>.1
    #  Data/Intensities/L00<lane>/C<cyc>.1/s_<lane>_<tile>.cif
    #  Data/Intensities/BaseCalls
    #  Data/Intensities/BaseCalls/config.xml
    #  Data/Intensities/BaseCalls/s_<lane>_<tile>.filter
    #  Data/Intensities/BaseCalls/L00<lane>
    #  Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1
    #  Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/s_<lane>_<tile>.bcl
    #  Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/s_<lane>_<tile>.stats
    #

    # This constant denotes how many items to print in verbose
    # mode before saying "and a bunch more..."
    MAX_VERBOSE_COUNT = 20

    tile_list = rundir.get_tile_list()

    if not tile_list:
        # Platform is unknown -- what do we do?
        print >> sys.stderr, "validate(): %s: Platform unknown" % rundir.get_dir()
        return False

    lane_list = range(1,9)

    total_cycles = sum(rundir.get_cycle_list())

    exit_status = True

    # Confirm that the "Data/" directory exists.
    data_path = os.path.join(rundir.get_path(), "Data")
    if not os.path.exists(data_path) or not os.path.isdir(data_path):
        print >> sys.stderr, "validate(): %s: No Data directory" % rundir.get_dir()
        return False

    # Confirm that the "Data/Intensities/" directory exists.
    intensities_path = os.path.join(data_path, "Intensities")
    if not os.path.exists(intensities_path) or not os.path.isdir(intensities_path):
        print >> sys.stderr, "validate(): %s: No Intensities directory" % rundir.get_dir()
        return False

    missing_position_files = []
    missing_lane_dirs = []
    for lane in lane_list:
        # GA, HCS 1.1.37: Confirm that the 's_<lane>_00<tile>_pos.txt' files exist in Data/Intensities/.
        if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA or
            (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) <= 1137)): # "1.1.37"
            for tile in tile_list:
                pos_file = "s_%d_%04d_pos.txt" % (lane, tile)
                pos_path = os.path.join(intensities_path, pos_file)
                if not os.path.exists(pos_path) or os.path.getsize(pos_path) == 0:
                    missing_position_files.append(pos_file)

        # Confirm that the Data/Intensities/L00<lane>/ directory exists.
        intensities_lane_path = os.path.join(intensities_path, "L%03d" % lane)
        if not os.path.exists(intensities_lane_path) or not os.path.isdir(intensities_lane_path):
            missing_lane_dirs.append("L%03d" % lane)
            exit_status = False
            continue

        if verbose:
            print >> sys.stderr, "validate(): Examining Data/Intensities/L%03d" % lane

        # As of HCS 1.3.8: Confirm that the 's_<lane>_<tile>.clocs' files exist in Data/Intensities/L00<lane>.
        if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) >= 1308):
            for tile in tile_list:
                 pos_file = "s_%d_%04d.clocs" % (lane, tile)
                 pos_path = os.path.join(intensities_lane_path, pos_file)
                 if not os.path.exists(pos_path) or os.path.getsize(pos_path) == 0:
                     missing_position_files.append(pos_file)

        if not no_cif:
            missing_cycle_dirs = []
            found_one_cif_file = False
            for cyc in range(1, total_cycles+1):

                # Confirm that the Data/Intensities/L00<lane>/C<cyc>.1/ directory exists.
                intensities_lane_cycle_path = os.path.join(intensities_lane_path, "C%d.1" % cyc)
                if not os.path.exists(intensities_lane_cycle_path) or not os.path.isdir(intensities_lane_cycle_path):
                    missing_cycle_dirs.append("L%03d/C%d.1" % (lane, cyc))
                    exit_status = False
                    continue

                if verbose:
                    sys.stderr.write(".")

                # Confirm that the Data/Intensities/L00<lane>/C<cyc>.1/s_<lane>_<tile>.cif files exist.
                missing_cif_files = []
                for tile in tile_list:
                    cif_file = "s_%d_%d.cif" % (lane, tile)
                    cif_path = os.path.join(intensities_lane_cycle_path, cif_file)
                    if not os.path.exists(cif_path) or os.path.getsize(cif_path) == 0:
                        missing_cif_files.append("L%03d/C%d.1/%s" % (lane, cyc, cif_file))
                        exit_status = False
                    else:
                        found_one_cif_file = True

                if len(missing_cif_files) > 0 and found_one_cif_file:
                    print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/L%03d/C%d.1 .cif files" % (rundir.get_dir(),len(missing_cif_files),lane, cyc)
                    if verbose:
                        for f in missing_cif_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
                        if len(missing_cif_files) > MAX_VERBOSE_COUNT:
                            print >> sys.stderr, "[...%d more items]" % (len(missing_cif_files) - MAX_VERBOSE_COUNT)

            if verbose:
                print >> sys.stderr

            if not found_one_cif_file:
                print >> sys.stderr, "validate(): %s: No .cif files in lane L%03d" % (rundir.get_dir(), lane)

            if len(missing_cycle_dirs) > 0:
                print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/L%03d cycle dirs" % (rundir.get_dir(),len(missing_cycle_dirs),lane)
                if verbose:
                    for d in missing_cycle_dirs[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, d
                    if len(missing_cycle_dirs) > MAX_VERBOSE_COUNT:
                        print >> sys.stderr, "[...%d more items]" % (len(missing_cycle_dirs) - MAX_VERBOSE_COUNT)

    if len(missing_position_files) > 0:
        if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA or
            (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) <= 1137)): # "1.1.37"
            position_file_ext = "_pos.txt"
        elif (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) >= 1308):
            position_file_ext = ".clocs"
        else:
            position_file_ext = "POSITION"
        print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities %s files" % (rundir.get_dir(),len(missing_position_files),position_file_ext)
        if verbose:
            for f in missing_position_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
            if len(missing_position_files) > MAX_VERBOSE_COUNT:
                print >> sys.stderr, "[...%d more items]" % (len(missing_position_files) - MAX_VERBOSE_COUNT)

    if len(missing_lane_dirs) > 0:
        print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities lane dirs"  % (rundir.get_dir(),len(missing_lane_dirs))
        if verbose:
            for d in missing_lane_dirs[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, d
            if len(missing_lane_dirs) > MAX_VERBOSE_COUNT:
                print >> sys.stderr, "[...%d more items]" % (len(missing_lane_dirs) - MAX_VERBOSE_COUNT)


    # Confirm that the "Data/Intensities/BaseCalls/" directory exists.
    basecalls_path = os.path.join(intensities_path, "BaseCalls")
    if not os.path.exists(basecalls_path) or not os.path.isdir(basecalls_path):
        print >> sys.stderr, "validate(): %s: No BaseCalls directory" % rundir.get_dir()
        return False

    if verbose:
        print >> sys.stderr, "validate(): Examining Data/Intensities/BaseCalls"

    # Confirm that the "Data/Intensities/BaseCalls/config.xml" file exists.
    basecalls_config_path = os.path.join(basecalls_path, "config.xml")
    if not os.path.exists(basecalls_config_path):
        print >> sys.stderr, "validate(): %s: No BaseCalls/config.xml" % rundir.get_dir()
        exit_status = False

    # GA, HCS v 1.1.37.8: Confirm that .filter files exist in Data/Intensities/BaseCalls.
    if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA or
        (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) <= 1137)): # "1.1.37"
        missing_filter_files = []
        for lane in lane_list:
            for tile in tile_list:
                tile_prefix = "s_%d_%04d" % (lane, tile)

                filter_file = tile_prefix + ".filter"
                filter_path = os.path.join(basecalls_path, filter_file)
                if (not os.path.exists(filter_path) or os.path.getsize(filter_path) == 0):
                    missing_filter_files.append(filter_file)
                    exit_status = False

        if len(missing_filter_files) > 0:
            print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls/ .filter files" % (rundir.get_dir(),len(missing_filter_files))
            if verbose:
                for f in missing_filter_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
                if len(missing_filter_files) > MAX_VERBOSE_COUNT:
                    print >> sys.stderr, "[...%d more items]" % (len(missing_filter_files) - MAX_VERBOSE_COUNT)

    missing_lane_dirs = []
    for lane in lane_list:
        # Confirm that the Data/Intensities/BaseCalls/L00<lane>/ directory exists.
        basecalls_lane_path = os.path.join(basecalls_path, "L%03d" % lane)
        if not os.path.exists(basecalls_lane_path) or not os.path.isdir(basecalls_lane_path):
            missing_lane_dirs.append("L%03d" % lane)
            exit_status = False
            continue

        if verbose:
            print >> sys.stderr, "validate(): Examining Data/Intensities/BaseCalls/L%03d" % lane

        # As of HCS v 1.3.8: Confirm that .filter files exist in Data/Intensities/BaseCalls/L00<lane>.
        if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) >= 1308):
            missing_filter_files = []
            for tile in tile_list:
                tile_prefix = "s_%d_%04d" % (lane, tile)

                filter_file = tile_prefix + ".filter"
                filter_path = os.path.join(basecalls_lane_path, filter_file)
                if not os.path.exists(filter_path) or os.path.getsize(filter_path) == 0:
                    missing_filter_files.append(filter_file)
                    exit_status = False

        missing_cycle_dirs = []
        for cyc in range(1, total_cycles+1):
            # Confirm that the Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/ directory exists.
            basecalls_lane_cycle_path = os.path.join(basecalls_lane_path, "C%d.1" % cyc)
            if not os.path.exists(basecalls_lane_cycle_path) or not os.path.isdir(basecalls_lane_cycle_path):
                missing_cycle_dirs.append("L%03d/C%d.1" % (lane, cyc))
                exit_status = False
                continue

            if verbose:
                sys.stderr.write(".")

            missing_bcl_files = []
            missing_stats_files = []
            for tile in tile_list:
                tile_prefix = "s_%d_%d" % (lane, tile)

                # Confirm that '.bcl' files exist in Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/
                bcl_file = tile_prefix + ".bcl"
                bcl_path = os.path.join(basecalls_lane_cycle_path, bcl_file)
                if not os.path.exists(bcl_path) or os.path.getsize(bcl_path) == 0:
                    missing_bcl_files.append("L%03d/C%d.1/%s" % (lane, cyc, bcl_file))
                    exit_status = False

                # Confirm that '.stats' files exist in Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/
                stats_file = tile_prefix + ".stats"
                stats_path = os.path.join(basecalls_lane_cycle_path, stats_file)
                if not os.path.exists(stats_path) or os.path.getsize(stats_path) == 0:
                    missing_stats_files.append("L%03d/C%d.1/%s" % (lane, cyc, stats_file))
                    exit_status = False

            if len(missing_bcl_files) > 0:
                print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls/L%03d/C%d.1 .bcl files" % (rundir.get_dir(),len(missing_bcl_files),lane,cyc)
                if verbose:
                    for f in missing_bcl_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
                    if len(missing_bcl_files) > MAX_VERBOSE_COUNT:
                        print >> sys.stderr, "[...%d more items]" % (len(missing_bcl_files) - MAX_VERBOSE_COUNT)

            if len(missing_stats_files) > 0:
                print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls/L%03d/C%d.1 .stats files" % (rundir.get_dir(),len(missing_stats_files),lane,cyc)
                if verbose:
                    for f in missing_stats_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
                    if len(missing_stats_files) > MAX_VERBOSE_COUNT:
                        print >> sys.stderr, "[...%d more items]" % (len(missing_stats_files) - MAX_VERBOSE_COUNT)

        if verbose:
            print >> sys.stderr

        if len(missing_cycle_dirs) > 0:
            print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls/L%03d cycle dirs" % (rundir.get_dir(),len(missing_cycle_dirs),lane)
            if verbose:
                for d in missing_cycle_dirs[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, d
                if len(missing_cycle_dirs) > MAX_VERBOSE_COUNT:
                    print >> sys.stderr, "[...%d more items]" % (len(missing_cycle_dirs) - MAX_VERBOSE_COUNT)

        if ((rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ and rundir.get_control_software_version(integer=True) >= 1308) and
            len(missing_filter_files) > 0):
             print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls/L%03d .filter files" % (rundir.get_dir(),len(missing_filter_files),lane)
             if verbose:
                 for f in missing_filter_files[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, f
                 if len(missing_filter_files) > MAX_VERBOSE_COUNT:
                     print >> sys.stderr, "[...%d more items]" % (len(missing_filter_files) - MAX_VERBOSE_COUNT)


    if len(missing_lane_dirs) > 0:
        print >> sys.stderr, "validate(): %s: Missing %d Data/Intensities/BaseCalls lane dirs"  % (rundir.get_dir(),len(missing_lane_dirs))
        if verbose:
            for d in missing_lane_dirs[0:MAX_VERBOSE_COUNT]: print >> sys.stderr, d
            if len(missing_lane_dirs) > MAX_VERBOSE_COUNT:
                print >> sys.stderr, "[...%d more items]" % (len(missing_lane_dirs) - MAX_VERBOSE_COUNT)

    rundir.validated = exit_status

    return exit_status

#
# With HCS 1.3.8, this function may be obsolete, since the BCL->qseq conversion can now ignore
#  missing BCL and stats files.
#
def fix_missing_stats_files(rundir, verbose=False):

    tile_list = rundir.get_tile_list()

    if not tile_list:
        # Platform is unknown -- what do we do?
        print >> sys.stderr, "fix_missing_stats_files(): %s: Platform unknown" % rundir.get_dir()
        return False

    lane_list = range(1,9)

    total_cycles = sum(rundir.get_cycle_list())

    exit_status = True

    # Confirm that the "Data/" directory exists.
    data_path = os.path.join(rundir.get_path(), "Data")
    if (not os.path.exists(data_path) or
        not os.path.isdir(data_path)):
        print >> sys.stderr, "fix_missing_stats_files(): %s: No Data directory" % rundir.get_dir()
        return False

    # Confirm that the "Data/Intensities/" directory exists.
    intensities_path = os.path.join(data_path, "Intensities")
    if (not os.path.exists(intensities_path) or
        not os.path.isdir(intensities_path)):
        print >> sys.stderr, "fix_missing_stats_files(): %s: No Intensities directory" % rundir.get_dir()
        return False

    # Confirm that the "Data/Intensities/BaseCalls/" directory exists.
    basecalls_path = os.path.join(intensities_path, "BaseCalls")
    if (not os.path.exists(basecalls_path) or
        not os.path.isdir(basecalls_path)):
        print >> sys.stderr, "fix_missing_stats_files(): %s: No BaseCalls directory" % rundir.get_dir()
        return False

    for lane in lane_list:

        # Confirm that the Data/Intensities/BaseCalls/L00<lane>/ directory exists.
        basecalls_lane_path = os.path.join(basecalls_path, "L%03d" % lane)
        if (not os.path.exists(basecalls_lane_path) or
            not os.path.isdir(basecalls_lane_path)):
            print >> sys.stderr, "fix_missing_stats_files(): %s: Missing Data/Intensities/BaseCalls/L%03d dir"  % (rundir.get_dir(), lane)
            continue

        for cyc in range(1, total_cycles+1):

            # Confirm that the Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/ directory exists.
            basecalls_lane_cycle_path = os.path.join(basecalls_lane_path, "C%d.1" % cyc)
            if (not os.path.exists(basecalls_lane_cycle_path) or
                not os.path.isdir(basecalls_lane_cycle_path)):
                print >> sys.stderr, "fix_missing_stats_files(): %s: Missing Data/Intensities/BaseCalls/L%03d/C%d.1 dir"  % (rundir.get_dir(), lane, cyc)
                continue

            for tile in tile_list:
                tile_prefix = "s_%d_%d" % (lane, tile)

                # Confirm that '.stats' files exist in Data/Intensities/BaseCalls/L00<lane>/C<cyc>.1/
                stats_file = tile_prefix + ".stats"
                stats_path = os.path.join(basecalls_lane_cycle_path, stats_file)
                if (not os.path.exists(stats_path) or os.path.getsize(stats_path) == 0):
                    #
                    # Missing .stats file!
                    #
                    # Try getting the same tile from a previous cycle.
                    for prevcyc in range(cyc-1, 0, -1):
                        prev_basecalls_lane_cycle_path = os.path.join(basecalls_lane_path, "C%d.1" % prevcyc)
                        if (not os.path.exists(prev_basecalls_lane_cycle_path) or
                            not os.path.isdir(prev_basecalls_lane_cycle_path)):
                            continue

                        prev_stats_path = os.path.join(prev_basecalls_lane_cycle_path, stats_file)
                        if (os.path.exists(prev_stats_path) and
                            os.path.getsize(prev_stats_path) != 0):
                            prev_stats_rel_path = os.path.join("..", "C%d.1" % prevcyc, stats_file)
                            if verbose:
                                print >> sys.stderr, "Linking %s to L%03d/C%d.1" % (prev_stats_rel_path, lane, cyc)
                            os.symlink(prev_stats_rel_path, stats_path)
                            break
                    else:
                        # Try getting the same tile from a subsequent cycle.
                        for nextcyc in range(cyc+1, total_cycles+1):
                            next_basecalls_lane_cycle_path = os.path.join(basecalls_lane_path, "C%d.1" % nextcyc)
                            if (not os.path.exists(next_basecalls_lane_cycle_path) or
                                not os.path.isdir(next_basecalls_lane_cycle_path)):
                                continue

                            next_stats_path = os.path.join(next_basecalls_lane_cycle_path, stats_file)
                            if (os.path.exists(next_stats_path) and
                                os.path.getsize(next_stats_path) != 0):
                                next_stats_rel_path = os.path.join("..", "C%d.1" % nextcyc, stats_file)
                                if verbose:
                                    print >> sys.stderr, "Linking %s to L%03d/C%d.1" % (next_stats_rel_path, lane, cyc)
                                os.symlink(next_stats_rel_path, stats_path)
                                break
                        else:
                            print >> sys.stderr, "No other cycle to copy into missing L%03d/C%d.1/%s" % (lane, cyc, stats_file)

    return exit_status


def make_thumbnail_subset_tar(rundir, overwrite=False, verbose=False):

    tar_filename     = "Thumbnail_subset.tgz"
    tar_filename_tmp = tar_filename + ".tmp"
    tar_path         = os.path.join(rundir.get_path(), tar_filename)

    # Check to see if tar file already exists.
    if not overwrite and os.path.exists(tar_path):
        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: Thumbnail subset tar already exists" % rundir.get_dir()
        return False

    # Get the list of lanes to keep.
    lane_list = range(1,9)

    if verbose:
        print >> sys.stderr, "Lane list: %s" % lane_list

    # Get the subset of tiles to keep.
    if (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_GA):
        # For GAIIx, use this subset of tiles.
        tile_subset = [1,20,40,60,61,80,100,120]
    elif (rundir.get_platform() == rundir.PLATFORM_ILLUMINA_HISEQ):
        # For HiSeq, use all tiles.
        tile_subset = rundir.get_tile_list()
    else:
        # Platform is unknown -- what do we do?
        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: Platform unknown" % rundir.get_dir()
        return False

    if verbose:
        print >> sys.stderr, "Tile subset: %s" % tile_subset

    # Calculate the subset of cycles to keep.
    cycle_list = rundir.get_cycle_list()
    if not cycle_list:
        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: No cycle list" % rundir.get_dir()
        return False

    read_starts = []
    cur_cyc = 0
    for cyc in cycle_list:
        read_starts.append(cur_cyc)
        cur_cyc += cyc

    cycle_subset_per_read = []
    for cyc in cycle_list:
        if (cyc >= 10):
            cycle_subset_per_read.append([1, 10, cyc-10, cyc])
        else:
            cycle_subset_per_read.append([1, cyc])

    cycle_subset_read_list = map(lambda start, cyc_list: map(lambda cyc: cyc+start, cyc_list),
                                 read_starts, cycle_subset_per_read)

    cycle_subset = [cyc for subset in cycle_subset_read_list for cyc in subset]

    if verbose:
        print >> sys.stderr, "Cycle_subset: %s" % cycle_subset

    bases = "actg"

    #
    # Hierarchy of Thumbnail_Images directory:
    #  Thumbnail_Images
    #   L00<lane>
    #    C<cycle>.1
    #     s_<lane>_<tile>_[ACTG].jpg
    #

    # Confirm that the "Thumbnail_Images/" directory exists.
    thumbnail_path = "Thumbnail_Images"
    if (not os.path.isdir(os.path.join(rundir.get_path(), thumbnail_path))):
        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: No Thumbnail_Images directory" % rundir.get_dir()
        return False

    # The list of Thumbnail images to be tarred.
    file_subset = []

    for lane in lane_list:

        # Confirm that the Thumbnail_Images/L00<lane>/ directory exists.
        thumbnail_lane_path = os.path.join(thumbnail_path, "L%03d" % lane)
        if (not os.path.isdir(os.path.join(rundir.get_path(), thumbnail_lane_path))):
            print >> sys.stderr, "make_thumbnail_subset_tar(): %s: Missing Thumbnail_Images/L%03d dir" % (rundir.get_dir(), lane)
            continue

        for cyc in cycle_subset:

            # Confirm that the Thumbnail_Images/L00<lane>/C<cyc>.1/ directory exists.
            thumbnail_lane_cycle_path = os.path.join(thumbnail_lane_path, "C%d.1" % cyc)
            if (not os.path.isdir(os.path.join(rundir.get_path(), thumbnail_lane_cycle_path))):
                print >> sys.stderr, "make_thumbnail_subset_tar(): %s: Missing Thumbnail_Images/L%03d/C%d.1 dir" % (rundir.get_dir(), lane, cyc)
                continue

            for tile in tile_subset:
                for base in bases:
                    # Make path of thumbnail image using lowercase base
                    lane_tile_base_lc_filename = "s_%d_%d_%s.jpg" % (lane, tile, base)
                    image_lc_file = os.path.join(thumbnail_lane_cycle_path, lane_tile_base_lc_filename)
                    image_lc_path = os.path.join(rundir.get_path(), image_lc_file)
                    # Make path of thumbnail image using uppercase base
                    lane_tile_base_uc_filename = "s_%d_%d_%s.jpg" % (lane, tile, base.upper())
                    image_uc_file = os.path.join(thumbnail_lane_cycle_path, lane_tile_base_uc_filename)
                    image_uc_path = os.path.join(rundir.get_path(), image_uc_file)

                    if verbose:
                        print >> sys.stderr, image_uc_file

                    if os.path.exists(image_lc_path):
                        file_subset.append(image_lc_file)
                    elif os.path.exists(image_uc_path):
                        file_subset.append(image_uc_file)
                    else:
                        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: Missing Thumbnail_Images/L%03d/C%d.1/%s" % (rundir.get_dir(), lane, cyc, lane_tile_base_uc_filename)


    if len(file_subset) > 0:

        if verbose:
            print >> sys.stderr, "Creating %s..." % tar_filename_tmp

        # Save the current directory.
        saved_curdir = os.getcwd()

        # Change current directory to rundir.
        os.chdir(rundir.get_path())

        # Open tar file object.
        tar_file = tarfile.open(tar_filename_tmp, "w:gz")
        
        # Add all the files from the file_subset list.
        for f in file_subset:
            tar_file.add(f)

        # Close the tar.
        tar_file.close()

        # Move the temporary tar file into its final place.
        if verbose:
            print >> sys.stderr, "Moving %s to %s..." % (tar_filename_tmp,tar_filename)
        os.rename(tar_filename_tmp, tar_filename)

        # Restore the saved current directory
        os.chdir(saved_curdir)
        
        return True
    else:
        print >> sys.stderr, "make_thumbnail_subset_tar(): %s: No images chosen; No tar file created." % rundir.get_dir()
        return False


def make_archive_tar(rundir, destDir=None, fileCheck=True, deleteAfter=False, verbose=False):

    #
    # These files will be checked before and after making the tar to see that they exist/got in.
    #  All paths are relative to the root run directory.
    #
    THUMBNAIL_SUBSET_TAR = "Thumbnail_subset.tgz"
    SPOT_CHECK_FILES = [
            "Data/reports/Status.xml",
            "Data/Intensities/Offsets/offsets.txt",
            ["Data/Intensities/BaseCalls/L001/C1.1/s_1_68.bcl",
             "Data/Intensities/BaseCalls/L001/C1.1/s_1_1108.bcl"], # Sublists are OR'ed together.
            "InterOp/CorrectedIntMetricsOut.bin",
            "InterOp/ExtractionMetricsOut.bin",
            "InterOp/QMetricsOut.bin",
            "InterOp/TileMetricsOut.bin",
            "RunInfo.xml"
            ]

    # Put resulting tar file in the root of the run dir, if not provided.
    if not destDir:
        destDir = rundir.get_root()

    # Create paths for the files we'll be creating.
    compressed_tar_path = os.path.join(destDir, rundir.get_dir() + ".tgz")
    compressed_tar_path_tmp = compressed_tar_path + ".tmp"
    list_tar_path = compressed_tar_path + ".list"
    list_tar_path_tmp = list_tar_path + ".tmp"
    compressed_list_tar_path = list_tar_path + ".gz"
    compressed_list_tar_path_tmp = compressed_list_tar_path + ".tmp"
    md5_path = compressed_tar_path + ".md5"
    md5_path_tmp = md5_path + ".tmp"
    interop_tar_path = os.path.join(destDir, rundir.get_dir() + ".InterOp.tar")
    interop_tar_path_tmp = interop_tar_path + ".tmp"

    #
    # Do precheck to make sure spot-check files are in the run directory.
    #
    if fileCheck:

        # Check for thumbnail subset tar.  If not there, make it.
        if not os.path.exists(os.path.join(rundir.get_path(), THUMBNAIL_SUBSET_TAR)):
            if not make_thumbnail_subset_tar(rundir, verbose=verbose):
                print >> sys.stderr, "make_archive_tar(): Couldn't make missing thumbnail subset tar...exiting..."
                return False

        # Check for all the other spot check files.
        oneFileMissing = False
        for file in SPOT_CHECK_FILES:
            if isinstance(file,list):
                exists = any(map(lambda f: os.path.exists(os.path.join(rundir.get_path(), f)), file))
            else:
                exists = os.path.exists(os.path.join(rundir.get_path(), file))
            if not exists:
                print >> sys.stderr, "make_archive_tar(): Run dir missing %s" % file
                oneFileMissing = True
        if oneFileMissing:
            print >> sys.stderr, "make_archive_tar(): Run dir is missing spot-check files...exiting..."
            return False

    #
    # Make compressed tar file.
    #
    if not os.path.isfile(compressed_tar_path) or os.path.getsize(compressed_tar_path) == 0:

        # Compress and tar the directory into a temporary file.
        tar_cmd_list = ["tar", "-C", rundir.get_root(),
                        "--exclude", "Images", "--exclude", "Thumbnail_Images",
                        "-c", "-z", "-f", compressed_tar_path_tmp, rundir.get_dir()]
        if verbose:
            print >> sys.stderr, "make_archive_tar(): creating tar file for %s" % rundir.get_dir()

        retcode = subprocess.call(tar_cmd_list)
        if retcode:
            print >> sys.stderr, "make_archive_tar(): Error creating tar file %s (ret = %d)" % (compressed_tar_path_tmp, retcode)
            return False

        # Rename the temporary file to the final compressed tar file name.
        os.rename(compressed_tar_path_tmp, compressed_tar_path)
    else:
        print >> sys.stderr, "make_archive_tar(): Compressed tar file %s already exists...skipping creation..." % (compressed_tar_path)

    #
    # Make list of files in tar.
    # (Rationale: touch all the blocks in the tar to see if they are valid.)
    #
    if os.path.isfile(list_tar_path):
        if verbose:
            print >> sys.stderr, "make_archive_tar(): Tar file list %s already exists...overwriting..." % (list_tar_path)

    list_tar_file_out = open(list_tar_path_tmp, "w")

    list_tar_cmd_list = ["tar", "-tvz", "-f", compressed_tar_path]

    if verbose:
        print >> sys.stderr, "make_archive_tar(): creating tar file listing for %s" % rundir.get_dir()
        
    retcode = subprocess.call(list_tar_cmd_list, stdout=list_tar_file_out)

    list_tar_file_out.close()

    if retcode:
        print >> sys.stderr, "make_archive_tar(): Error creating tar file list %s (ret = %d)" % (list_tar_path_tmp, retcode)
        return False
    
    #
    # Spot-check the tar file list for some files.
    # (Rationale: confirm that at least some interesting files made it in.)
    #
    if fileCheck:
        for file in SPOT_CHECK_FILES:

            if isinstance(file,list):
                files_to_check = file
            else:
                files_to_check = [file]

            found_file = False
            for f in files_to_check:
                fgrep_cmd_list = ["fgrep", "-q", rundir.get_dir() + "/" + f, list_tar_path_tmp]

                if verbose:
                    print >> sys.stderr, "make_archive_tar(): looking in tar file list for %s" % f,

                retcode = subprocess.call(fgrep_cmd_list)

                if not retcode:
                    found_file = True
                    if verbose: print >> sys.stderr, "found"
                else:
                    if verbose: print >> sys.stderr, "not found"

            if not found_file:
                print >> sys.stderr, "make_archive_tar(): Tar file list is missing %s" % (file)
                return False
    else:
        if verbose:
            print >> sys.stderr, "make_archive_tar(): skipping file check for %s" % rundir.get_dir()

    #
    # File check above confirms proper creation of .tgz.list_tmp file: compress it.
    #

    # Compress the tar list file into another file.
    compressed_list_tar_file = open(compressed_list_tar_path_tmp, "w")

    compress_list_cmd_list = ["gzip", "-c", list_tar_path_tmp]

    if verbose:
        print >> sys.stderr, "make_archive_tar(): compressing tar list file for %s" % rundir.get_dir()

    retcode = subprocess.call(compress_list_cmd_list, stdout=compressed_list_tar_file)

    compressed_list_tar_file.close()

    if retcode:
        print >> sys.stderr, "make_archive_tar(): Error compressing tar list file %s (ret = %d)" % (list_tar_path, retcode)
        return False

    # Remove the original tar list file.
    if verbose:
        print >> sys.stderr, "make_archive_tar(): removing uncompressed tar list file for %s" % rundir.get_dir()
    os.remove(list_tar_path_tmp)

    # Rename the temporary compressed tar list file to the final tar list file name.
    os.rename(compressed_list_tar_path_tmp, compressed_list_tar_path)

    #
    # Make MD5 checksum of the compressed tar file.
    #
    if os.path.isfile(md5_path):
        if verbose:
            print >> sys.stderr, "make_archive_tar(): MD5 file %s already exists...overwriting..." % (md5_path)

    md5_file = open(md5_path_tmp, "w")

    if platform.system() == "Linux":
        md5_cmd_list = ["md5sum"]
    elif platform.system() == "Darwin":
        md5_cmd_list = ["md5", "-r"]
    else:
        md5_cmd_list = ["md5sum"]
        
    md5_cmd_list.append(compressed_tar_path)

    if verbose:
        print >> sys.stderr, "make_archive_tar(): creating MD5 checksum file listing for %s compressed tar" % rundir.get_dir()

    retcode = subprocess.call(md5_cmd_list, stdout=md5_file)

    md5_file.close()

    if retcode:
        print >> sys.stderr, "make_archive_tar(): Error creating MD5 file %s (ret = %d)" % (md5_path, retcode)
        return False

    # Rename the temporary MD5 file to the final MD5 file name.
    os.rename(md5_path_tmp, md5_path)

    #
    # Make an archive of the InterOp directory and RunInfo.xml file (and, if available, runParameters.xml).
    #
    if not os.path.isfile(interop_tar_path) or os.path.getsize(interop_tar_path) == 0:

        # Compress and tar the InterOp directory et al. into a temporary file.
        interop_tar_cmd_list = ["tar", "-C", rundir.get_root(), "-c", "-f", interop_tar_path_tmp,
                                os.path.join(rundir.get_dir(),"InterOp"),
                                os.path.join(rundir.get_dir(),"RunInfo.xml") ]

        # Add the runParameters.xml file to tar command, if it exists (only for HiSeq runs).
        if os.path.exists(os.path.join(rundir.get_path(),"runParameters.xml")):
            interop_tar_cmd_list.append(os.path.join(rundir.get_dir(),"runParameters.xml"))

        if verbose:
            print >> sys.stderr, "make_archive_tar(): creating tar file for InterOp dir of %s" % rundir.get_dir()

        retcode = subprocess.call(interop_tar_cmd_list)
        if retcode:
            print >> sys.stderr, "make_archive_tar(): Error creating tar file %s (ret = %d)" % (interop_tar_path_tmp, retcode)
            return False

    else:
        print >> sys.stderr, "make_archive_tar(): InterOp tar file %s already exists...skipping..." % (interop_tar_path)

    # Rename the temporary InterOp tar file to the final tar file name.
    os.rename(interop_tar_path_tmp, interop_tar_path)

    # Remove the run directory, if requested.
    if deleteAfter:
        if verbose:
            print >> sys.stderr, "make_archive_tar(): removing %s" % rundir.get_dir()

        # Rename the directory to one that looks like it is getting removed.
        os.rename(rundir.get_path(), rundir.get_path() + ".removing")

        # Remove the run directory.
        shutil.rmtree(rundir.get_path() + ".removing")

    return True
