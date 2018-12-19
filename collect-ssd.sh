#!/bin/bash
# Don't forget to set user/password below
USER="user"
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

/usr/bin/expect <(cat << 'EOD'
set nodes [lindex $argv 0];
set time [lindex $argv 1];
set user [lindex $argv 2];
set pass [lindex $argv 3];
set timeout 5;
array set spawn2node {};
log_user 1;

foreach connection $nodes {
  spawn ssh -q $user@$connection
  lappend spawn_id_list $spawn_id
  set spawn2node($spawn_id) $connection
  expect "assword:"
  send "$pass\r";
  expect "#"
}

set timeout 1200;
foreach id $spawn_id_list {
  set spawn_id $id
  send "show support details to file /flash/sftp/SSD/SSD\_$spawn2node($spawn_id)\_$time compress -noconfirm\r";
  expect "#"
  send "exit\r";
  expect eof;
}
EOD
) "$LST" $TS $USER $PASS


#Collect SSDs
for i in $LST
do
 /usr/bin/lftp -c "get $RMSRC sftp://$USER:$PASS@$i/sftp/SSD/SSD_${i}_${TS}.tar.gz -o $DEST/SSD_${i}_${TS}.tar.gz"
done
