#!/bin/sh

if [ $# -lt 1 ];
then
	echo "Specify the script"
	exit 1
else
	
	case "$1" in
	  MVTO*) 	prefix="MVTO" ;;
	  2PL*) 	prefix="2PL" ;;
	  *)		prefix="MVCC2PL" ;;
	esac

	parsedFile="parsed.bm"
	benchmarkResults=../results/"$prefix"_results.csv
	
	if [ -f "$benchmarkResults" ];
	then
		cat "$benchmarkResults" | grep -i "#BM-" | sed -e 's/#BM- //' > $parsedFile
		echo $parsedFile | gnuplot $1
		rm -f $parsedFile
	else
		echo "Could not find $benchmarkResults"	
	fi


fi

