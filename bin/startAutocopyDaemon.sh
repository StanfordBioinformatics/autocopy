#!/bin/bash


for dir in /Volumes/IlluminaRuns?
do 
chmod 770 $dir;
chown illumina:staff $dir;
chown illumina:staff $dir/Runs;
done

launchctl load AutocopyDaemon.plist
launchctl start AutocopyDaemon
