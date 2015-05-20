#!/bin/bash

#################################################
#            Benchmark Configuration            #
#################################################

# Cluster's #nodes
nbNodes=10
if [ $# -eq 1 ];
then
	nbNodes=$1
fi

# Starting #xact
nbxact_start=250
# Ending #xact
nbxact_end=$nbxact_start
# Step when increasing #xact
nbxact_step=0

# Max #(reads/writes) done by a xact
nbrequests=20
# #keys = #xact * key_multiplier
key_multiplier=2
# read:write ratio
rw_ratio=50

# single test's #repetitions (to get average values)
repetitions=25

#locality percentage
locality=50

##################################################
#        End of Benchmark Configuration          #
##################################################

# Output benchmark.bm file name
filename="benchmark.bm"
configName="../config/benchmark"

maxKeys=$(( $nbxact_end * $nb$key_multiplier ))

echo "nodes=""$nbNodes" > $configName
echo "maxKey="$maxKeys >> $configName
echo "locality=""$locality" >> $configName

if [ -f "results/$filename" ]; then
	rm -f "results/$filename"
fi

mkdir -p results

for (( t=$nbxact_start; t<=$nbxact_end; t+=$nbxact_step )) do
	keys=$(( $t * $nb$key_multiplier ))
	echo ":benchmark t" $t "r" $nbrequests "k" $keys "ratio" $rw_ratio  $repetitions >> "results/$filename"
done

