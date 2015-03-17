#!/bin/sh

HDFS_PATH=/tkvs/

mkdir -p classes/am classes/tm

javac -cp `hadoop classpath` -d classes/am `find src -name AppMaster.java`
javac -cp `hadoop classpath` -d classes/tm `find src -name TransactionManagerDeamon.java`

jar cvf AppMaster.jar -C classes/am .
jar cvf TransactionManagerDeamon.jar -C classes/tm .

hdfs dfs -mkdir -p $HDFS_PATH
hdfs dfs -put TransactionManagerDeamon.jar $HDFS_PATH

hadoop jar `find $HADOOP_PREFIX -name *unmanaged-am-launcher*.jar|head -n 1` -appname 'transactional kv store' -cmd 'java com.epfl.tkvs.AppMaster' -classpath AppMaster.jar
