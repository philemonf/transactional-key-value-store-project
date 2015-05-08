#!/bin/sh

RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

if [ "$1" = "local" ];
then
    logs_dir=`find "$HADOOP_HOME" -name userlogs`
    app=`cat .last_app_id`
    cat `find "$logs_dir/$app/" -type f`
else
    echo ${CYAN}* List of TKVS logs:${NC}
    hadoop fs -ls /tmp/tkvs/logs

    echo ${CYAN}* Enter log filename: ${NC}
    read log
    hadoop fs -cat /tmp/tkvs/logs/$log
fi
