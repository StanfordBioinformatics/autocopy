###
#AUTHOR: Nathaniel Watson
#DATE  : April 29, 2014
###

from optparse import OptionParser
import os


description = "Calculcates the time it took for autocopy to copy one or more runs to the cluster, by looking at the timestamps of the Autocopy_started.txt and Autocopy_complete.txt files in a run directory. Any number of run directories can be specified as arguments. The output fomat is one line per run in the form 'run name: hours', where hours is represented as a float. Runs that don't have both autocopy sentinal files will be skipped."
usage = "usage: %prog [options] dir1 dir2 dir3 ..."
parser = OptionParser(description=description,usage=usage)
parser.add_option('--ignore-skipped',action="store_true",help="(Optional) Presence of this option indicates that skipped runs (which don't have both autocopy sentinal files) will not be listed in stdout.")
opts,args = parser.parse_args()

dirs = args

for d in dirs:
  acs = os.path.join(d,"Autocopy_started.txt")
  acc = os.path.join(d,"Autocopy_complete.txt")
  if not os.path.exists(acc) or not os.path.exists(acs):
    if not opts.ignore_skipped:
      print ("Skipping {d}".format(d=d))
    continue  
  mtime_acc = os.path.getmtime(acc)
  mtime_acs = os.path.getmtime(acs)
  diff = mtime_acc - mtime_acs
  minutes = diff/60.0
  hours = "%.2f" % (minutes/60.0)

  print os.path.basename(d) + ": " + str(hours)
