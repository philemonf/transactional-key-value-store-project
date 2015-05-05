#!/bin/sh

#create slaves if local mode is set as a parameter
if [ "$1" = "local" ];
then
HOSTNAME=$(hostname)
mkdir -p config
echo "$HOSTNAME" > config/slaves
fi

# Hide stderr
#exec 3>&2 2>/dev/null

# Sets the hadoop classpath.
HADOOP_CP=`hadoop classpath`
if [ -z "$HADOOP_CP" ]
then
	HADOOP_CP="$HADOOP_CLASSPATH"
fi

# Cleans old build.
rm -f *.jar
rm -r -f build

echo "Building TKVS.jar..."
# Builds the jar.
mkdir build
find . -name "*.java" > .sources
javac -cp "lib:$HADOOP_CP" -d build @.sources
jar cf TKVS.jar -C build .
rm -f .sources
rm -r -f build


# Puts the jar in HDFS under /projects/transaction-manager/.
hadoop fs -rm -r -f "/projects/transaction-manager/*"

echo "Putting TKVS.jar and config in HDFS..."
PROJECT_HOME='/projects/transaction-manager/'
JAR_PATH="$PROJECT_HOME/TKVS.jar"
CONFIG_PATH="$PROJECT_HOME/config"

hadoop fs -mkdir -p "$PROJECT_HOME"
hadoop fs -copyFromLocal TKVS.jar "$JAR_PATH"
hadoop fs -chmod -R 777 "$JAR_PATH"
hadoop fs -copyFromLocal config "$PROJECT_HOME"
hadoop fs -chmod -R 777 "$CONFIG_PATH"

# Reset stderr
#exec 2>&3

# Executes the Client.
echo "Executing YARN Client...\n"
hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client

hadoop fs -copyToLocal /projects/transaction-manager/results.bm results.bm 
