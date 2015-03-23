#!/bin/sh
# Outputs the whole log of the last app.
last=`ls -1t $HADOOP_HOME/logs/userlogs/ | head -1`
cat $HADOOP_HOME/logs/userlogs/"$last"/*/*
