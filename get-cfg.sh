#!/bin/bash
# Set user/password below, edit node list and enjoy

USER="user"
PASS="password"
LST="node1 node2 node3 ... etc"

HOME="/home/user"
REPO="$HOME/configRepo"

if [ $1 ]; then
	REPO=$1
fi
MESSAGE=`date +updated-%H:%M:%S-%d/%m/%Y`

echo "Will download configuration files to $REPO"

cd $HOME
cvs checkout configRepo

/usr/bin/expect <(cat << 'EOD'
set nodes [lindex $argv 0];
set user [lindex $argv 1];
set pass [lindex $argv 2];
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

foreach id $spawn_id_list {
  set spawn_id $id
  send "save config /flash/sftp/$spawn2node($spawn_id).cfg -r -n\r";
  expect "#"
#  send "save config /flash/$spawn2node($spawn_id).cfg -r -n\r";
#  expect "#"
  send "exit\r";
  expect eof;
}
EOD
) "$LST" $USER $PASS

for i in $LST
do
 /usr/bin/lftp -c "mirror -n -r -I $i.cfg sftp://$USER:$PASS@$i/sftp $REPO"
done
cd $REPO
cvs commit -m $MESSAGE 
