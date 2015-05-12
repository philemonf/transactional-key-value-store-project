#!/bin/bash

if [ $# -gt 3 ];
then
	start=$1;
	end=$2;
	step=$3;
	ratio=$4;
	fileName=$5;
else
	echo "Using default";
	start=1;
	end=500;
	step=20;
	ratio=2;
	fileName="benchmarkCommands.cm";
fi


if [ -f $fileName ] ; then
	rm -f $fileName
fi


# Generate command for the client
for (( t=$start; t<=$end; t+=$step )) ; do 
#	for (( r=$start; r<=$end; r+=$step )) ; do 
#		for (( k=$start; k<=$end; k+=$step )) ; do 
#	echo ":benchmark t" $t "r" $r "k" $k "ratio" $ratio "l 0" >> $fileName;	
	echo ":benchmark t" $t "r 20 k 20 ratio 15 l 0 4" >> $fileName;	
#		done
#	done
done
