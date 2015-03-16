#!/bin/sh

mkdir classes
mkdir -p classes/client classes/am

javac -cp `hadoop classpath` -d classes/client `find . -name Client.java`
javac -cp `hadoop classpath` -d classes/am `find . -name AppMaster.java`

jar cvf Client.jar -C classes/client .
jar cvf AppMaster.jar -C classes/am .

hdfs dfs -rm -f "$1"
hdfs dfs -put AppMaster.jar "$1"

hadoop jar Client.jar com.epfl.tkvs.Client "$1"
