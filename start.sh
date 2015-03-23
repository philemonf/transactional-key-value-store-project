#!/bin/sh

# Sets the hadoop classpath.
HADOOP_CP=`hadoop classpath`
if [ -z "$HADOOP_CP" ]
then
	HADOOP_CP="$HADOOP_CLASSPATH"
fi

# Cleans old build.
rm -f *.jar
rm -r -f build

# Builds the jar.
mkdir build
find . -name "*.java" > .sources
javac -cp "lib:$HADOOP_CP" -d build @.sources
jar cf TKVS.jar -C build .
rm -f .sources
#rm -r -f build

# Puts the jar in HDFS under /projects/transaction-manager/.
hadoop fs -rm -r -f "/projects/transaction-manager/*"
hadoop fs -copyFromLocal TKVS.jar /projects/transaction-manager/TKVS.jar
hadoop fs -copyFromLocal config /projects/transaction-manager/

# Executes the Client.
hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client


