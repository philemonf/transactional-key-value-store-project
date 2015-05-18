#!/bin/sh

if [ $# -lt 1 ];
then
	echo "Specify the script"
	exit 1
else
	merge="0"
	graphDirectory="../results/graphs/"
	if [ ! -d "$DIRECTORY" ]; then
		  mkdir -p $graphDirectory
	else
		rm -f $graphDirectory/*
	fi
	
	merge="0"

	case "$1" in
		MVTO*) 	prefix="MVTO" ;;
		2PL*) 	prefix="2PL" ;;
		MVCC2PL*) prefix="MVCC2PL";;	
		*)	prefix="MVTO 2PL MVCC2PL"
			merge="1"				
		;;
	esac

	if [ $# -eq 2 ];
	then
		scp "$2"@icdataportal2:~/transactional-key-value-store-project/benchmarks/results/* ../results
	fi

	for p in $prefix
	do
		benchmarkResults=../results/"$p"_results.csv
		parsedFile="$p""_parsed.bm"
		
		if [ -f "$benchmarkResults" ];
		then
			cat "$benchmarkResults" | grep -i "> #BM-" | sed -e 's/> #BM-\t//' | tr "#" "nb_" > $parsedFile
			echo $parsedFile | gnuplot "$p"_script.gp
		else
			echo "Could not find $benchmarkResults"	
		fi
	done

	if [ $merge -eq "1" ];
	then
	    	gnuplot MERGE_script.gp
	fi

	rm -f *.bm
fi
