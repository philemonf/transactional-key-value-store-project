#!/bin/sh

logs_dir=`find "$HADOOP_HOME" -name userlogs`

# Outputs the whole log of the last app.
app=`cat .last_app_id`
cat `find "$logs_dir/$app/" -type f`
