#!/bin/sh

HADOOP_CP=`hadoop classpath`
if [[ -z "$HADOOP_CP" ]]
then
	HADOOP_CP="$HADOOP_CLASSPATH"
fi


rm -f *.jar
rm -r -f build

mkdir build
javac -cp "$HADOOP_CP" -d build src/ch/epfl/tkvs/*/*.java
jar cf TKVS.jar -C build .
rm -r -f build

hadoop fs -rm -r -f /apps/tkvs
hadoop fs -mkdir -p /apps/tkvs
hadoop fs -copyFromLocal TKVS.jar /apps/tkvs/TKVS.jar

hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client