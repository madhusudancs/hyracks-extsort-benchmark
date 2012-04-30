#!/bin/bash

export JAVA_HOME=/usr/local/java/vms/java

LOGSDIR=/mnt/data/sdc/space/madhusudancs/extsort/logs
HYRACKS_HOME=/home/madhusudancs/hyracks-mycopy

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep asterix | grep "$IPADDR " | awk '{print $2}'`

rm -rf /mnt/data/sdc/space/madhusudancs/extsort/tmp/*

chmod -R 755 $HYRACKS_HOME
export JAVA_OPTS="-Xmx10G"

cd $LOGSDIR
echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 128.195.14.4 -cc-port 3099 -data-ip-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host 10.1.0.1 -cc-port 1099 -cluster-net-ip-address $IPADDR -data-ip-address $IPADDR -node-id $NODEID -iodevices "/mnt/data/sdc/space/madhusudancs/extsort/tmp/" -frame-size 65536&> $LOGSDIR/$NODEID.log &
