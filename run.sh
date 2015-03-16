#!/bin/sh

mkdir class_am
mkdir class_client
javac -classpath `hadoop classpath` -d class_am/ src/ApplicationMaster.java
javac -classpath `hadoop classpath` -d class_client/ src/Client.java
jar cvf ApplicationMaster.jar -C class_am/ .
jar cvf Client.jar -C class_client/ .
rm -r class_am class_client
yarn jar Client.jar Client ApplicationMaster.jar
