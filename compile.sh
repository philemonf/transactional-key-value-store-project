#!/bin/sh

RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

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


