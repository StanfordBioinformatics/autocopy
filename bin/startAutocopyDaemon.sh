#!/bin/bash

launchctl load AutocopyDaemon.plist
launchctl start AutocopyDaemon

for dir in /Volumes/IlluminaRuns?
do 
chmod 770 $dir;
chown illumina:staff $dir;
chown -R illumina:staff $dir/Runs;
done
