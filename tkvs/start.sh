#!/bin/sh

rm -f AppMaster.jar
javac -cp $HADOOP_CLASSPATH -d . `find src -name *.java`
jar cfe AppMaster.jar ch.epfl.tkvs.AppMaster ch/epfl/tkvs/*.class
rm -r ch

hadoop jar $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-*.jar -appname 'transactional kv store' -cmd 'java ch.epfl.tkvs.AppMaster' -classpath AppMaster.jar
