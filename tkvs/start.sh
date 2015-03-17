#!/bin/sh

HDFS_PATH=/tkvs/

mkdir -p classes/

HADOOP_CP=`hadoop classpath`
if [[ -z "$HADOOP_CP" ]]
then
    HADOOP_CP="$HADOOP_CLASSPATH"
fi


javac -cp "$HADOOP_CP" -d classes/ `find src -name *.java`

jar cvfe AppMaster.jar ch.epfl.tkvs.AppMaster -C classes/ .
jar cvfe TransactionManagerDeamon.jar ch.epfl.tkvs.TransactionManagerDeamon -C classes/ .

hdfs dfs -mkdir -p $HDFS_PATH
hdfs dfs -rm -f "$HDFS_PATH/TransactionManagerDeamon.jar"
hdfs dfs -put TransactionManagerDeamon.jar "$HDFS_PATH"

if [[ -z "$HADOOP_HOME" ]]
then
	hadoop jar `find $HADOOP_PREFIX -name *unmanaged-am-launcher*.jar|head -n 1` -appname 'transactional kv store' -cmd 'java com.epfl.tkvs.AppMaster' -classpath AppMaster.jar
else
	hadoop jar "$HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-*.jar" -appname 'transactional kv store' -cmd 'java ch.epfl.tkvs.AppMaster' -classpath AppMaster.jar
fi
