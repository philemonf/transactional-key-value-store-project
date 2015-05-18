#!/bin/sh

RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

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

algorithmName=`head -1 ./config/algorithm`
echo $algorithmName
if [ "$algorithmName" = "simple_2pl" ];
then
	algorithmName="2PL"
elif [ "$algorithmName" = "mvcc2pl" ];
then
	algorithmName="MVCC2PL"
else
	algorithmName="MVTO"
fi

RESULT_FILE="$RESULT_DIR/$algorithmName_results.csv"

# Executes the Client.
echo ${CYAN}* Executing YARN Client...${NC}
if [ $# -gt 1 ];
then
	hadoop fs -copyFromLocal $2 /projects/transaction-manager/
	hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client < "$2" > "$RESULT_FILE" || { echo ${RED}ERROR: hadoop jar! Exiting.${NC} ; exit 1; }
else
	hadoop jar TKVS.jar ch.epfl.tkvs.yarn.Client || { echo ${RED}ERROR: hadoop jar! Exiting.${NC} ; exit 1; }
fi
