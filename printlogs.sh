#!/bin/sh

# Outputs the whole log of the last app.
app=`cat .last_app_id`
cat $HADOOP_HOME/logs/userlogs/"$app"/*/*