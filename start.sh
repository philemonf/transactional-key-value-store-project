#!/bin/sh

RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

#create slaves if local mode is set as a parameter
if [ "$1" = "local" ];
then
	HOSTNAME=$(hostname)
	mkdir -p config
	echo "$HOSTNAME" > config/slaves
fi

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
echo ${CYAN}* Building TKVS.jar...${BLUE}
mkdir build
find . -name "*.java" > .sources
javac -cp "lib:$HADOOP_CP" -d build @.sources || { echo ${RED}ERROR: javac! Exiting.${NC} ; exit 1; }
jar cf TKVS.jar -C build . || { echo ${RED}ERROR: jar! Exiting.${NC} ; exit 1; }
rm -f .sources
rm -r -f build

# Puts the jar in HDFS under /projects/transaction-manager/.
echo ${CYAN}* Putting TKVS.jar and config in HDFS...${BLUE}

PROJECT_HOME='/projects/transaction-manager'

hadoop fs -rm -r -f "$PROJECT_HOME/*" || { echo ${RED}ERROR: hadoop fs -rm! Exiting.${NC} ; exit 1; }
JAR_PATH="$PROJECT_HOME/TKVS.jar"
CONFIG_PATH="$PROJECT_HOME/config"

hadoop fs -mkdir -p "$PROJECT_HOME" || { echo ${RED}ERROR: hadoop -mkdir! Exiting.${NC} ; exit 1; }
hadoop fs -copyFromLocal TKVS.jar "$JAR_PATH" || { echo ${RED}ERROR: hadoop -copyFromLocal! Exiting.${NC} ; exit 1; }
hadoop fs -chmod -R 777 "$JAR_PATH" || { echo ${RED}ERROR: hadoop -chmod! Exiting.${NC} ; exit 1; }
hadoop fs -copyFromLocal config "$PROJECT_HOME" || { echo ${RED}ERROR: hadoop -copyFromLocal! Exiting.${NC} ; exit 1; }
hadoop fs -chmod -R 777 "$CONFIG_PATH" || { echo ${RED}ERROR: hadoop -chmod! Exiting.${NC} ; exit 1; }

RESULT_DIR="./benchmarks/results"
if [ ! -d "$RESULT_DIR" ]; then
	mkdir $RESULT_DIR
fi



# Executes the Client.
echo ${CYAN}* Executing YARN Client...${NC}
if [ $# -gt 1 ];
then
	hadoop fs -copyFromLocal $2 /projects/transaction-manager/
	hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client < "$2" > "$RESULT_DIR"/results.csv || { echo ${RED}ERROR: hadoop jar! Exiting.${NC} ; exit 1; }
else
	hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client > ./benchmarks/results/results.csv || { echo ${RED}ERROR: hadoop jar! Exiting.${NC} ; exit 1; }
fi
