#!/bin/bash
# Version 1.0
# Collect "show support details" output from the list of ASR5k nodes
# Set username/password below
USER="username"
PASS="password"
DEST=.
USAGE="$0 -l <nodelist w/spaces> -w <workdir default .> [-r (remove SSD from the node flash)]"


while getopts l:w:rh option
do
        case "${option}"
        in
                l) LST=${OPTARG};;
                w) DEST=${OPTARG};;
                r) RMSRC="-E";;
                h) echo $USAGE && exit 0;;
        esac
done

TS=`date +%Y-%m-%d--%H-%M`
echo "Will download SSD files from $LST to $DEST with command(s)"
for i in $LST
do
 echo get ${RMSRC} sftp://${USER}:${PASS}@${i}/sftp/SSD/SSD_${i}_${TS}.tar.gz -o ${DEST}/SSD_${i}_${TS}.tar.gz
done

#exit 0

./ssd.exp "$LST" $TS

#Collect SSDs
for i in $LST
do
 /usr/bin/lftp -c "get $RMSRC sftp://$USER:$PASS@$i/sftp/SSD/SSD_${i}_${TS}.tar.gz -o $DEST/SSD_${i}_${TS}.tar.gz"
done
