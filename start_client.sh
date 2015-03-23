#!/bin/sh

# Sets the hadoop classpath.
HADOOP_CP=`hadoop classpath`
if [ -z "$HADOOP_CP" ]
then
	HADOOP_CP="$HADOOP_CLASSPATH"
fi

java -cp "build:"`hadoop classpath`  ch.epfl.tkvs.userclient.UserClient
