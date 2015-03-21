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
find -name "*.java" > sources.txt
javac -cp "$HADOOP_CP" -d build @sources.txt
rm -f sources.txt
jar cf TKVS.jar -C build .
rm -r -f build

# Puts the jar in HDFS under /projects/transaction-manager/.
hadoop fs -mkdir -p /projects/transaction-manager/
hadoop fs -rm -r -f "/projects/transaction-manager/*"
hadoop fs -copyFromLocal TKVS.jar /projects/transaction-manager/TKVS.jar

# Executes the Client.
hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client


# Outputs the whole log of the last app.
last=`ls -1t $HADOOP_HOME/logs/userlogs/ | head -1`
cat $HADOOP_HOME/logs/userlogs/"$last"/*/*