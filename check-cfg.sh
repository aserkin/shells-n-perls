#!/bin/bash
# Don't forget to change user/password below to something meaningful

USER="user"
PASS="password"
NODE=$1

cd /tmp
expect <(cat << 'EOD'
set nodes [lindex $argv 0];
set user [lindex $argv 1];
set pass [lindex $argv 2];
set timeout 5;
array set spawn2node {};
log_user 0;
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
  send "copy /flash/$spawn2node($spawn_id).cfg /flash/sftp/$spawn2node($spawn_id)-orig.cfg -n\r";
  expect "#"
  send "exit\r";
  expect eof;
}
EOD
) $NODE $USER $PASS
/usr/bin/lftp -c "set xfer:clobber on && get sftp://$USER:$PASS@$NODE/sftp/$NODE.cfg"
/usr/bin/lftp -c "set xfer:clobber on && get sftp://$USER:$PASS@$NODE/sftp/$NODE-orig.cfg"
cat $NODE.cfg|/bin/egrep -v "encrypted|ssh key|\+[A-Z]" >running.cfg
cat $NODE-orig.cfg|/bin/egrep -v "encrypted|ssh key|\+[A-Z]" >saved.cfg
ORIGMD5=`/bin/cat saved.cfg|/usr/bin/md5sum`
CURRMD5=`/bin/cat running.cfg|/usr/bin/md5sum`
echo "Saved configuration checksum:   $ORIGMD5"
echo "Running configuration checksum: $CURRMD5"
if [ "$ORIGMD5" != "$CURRMD5" ]; then
   echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
   echo "!Warning! configurations differ!"
   echo "!   Enjoy differences below:   !"
   echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
   echo "diff -c saved.cfg running.cfg"
   diff -c saved.cfg running.cfg
   exit 0
fi

rm -f $NODE.cfg $NODE-orig.cfg running.cfg saved.cfg
