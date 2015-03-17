#!/bin/sh

mkdir classes
mkdir -p classes/

javac -cp `hadoop classpath` -d classes/ `find src -name *.java`

jar cvf AppMaster.jar -C classes/ .

hadoop jar `find $HADOOP_PREFIX -name *unmanaged-am-launcher*.jar|head -n 1` -appname 'transactional kv store' -cmd 'java com.epfl.tkvs.AppMaster' -classpath AppMaster.jar
