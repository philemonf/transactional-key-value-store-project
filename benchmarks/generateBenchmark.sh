#!/bin/bash

#################################################
#            Benchmark Configuration            #
#################################################

# Cluster's #nodes
nbnodes=20

# Starting #xact
nbxact_start=100
# Ending #xact
nbxact_end=100
# Step when increasing #xact
nbxact_step=100

# Max #(reads/writes) done by a xact
nbrequests=10
# #keys = #xact * key_multiplier
key_multiplier=10
# read:write ratio
rw_ratio=30
# percentage of a xact's keys that are on the local TM
locality_percentage=30

# single test's #repetitions (to get average values)
repetitions=1

# Output benchmark.bm file name
filename="benchmark.bm"

#################################################
#################################################

if [ -f results/$filename ]; then
	rm -f results/$filename
fi

mkdir -p results

for (( t=$nbxact_start; t<=$nbxact_end; t+=$nbxact_step )) do
	keys=$(( $t * $nb$key_multiplier ))
	echo ":benchmark t" $t "r" $nbrequests "k" $keys "ratio" $rw_ratio "l" $locality_percentage "N" $nbnodes $repetitions >> results/$filename
done
