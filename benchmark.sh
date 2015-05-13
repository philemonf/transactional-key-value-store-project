#! /bin/sh

if [ $# -lt 1 ];
then
	echo "Specify number of nodes"
	exit 1
fi

# Number of nodes
N=$1

cd ./benchmarks/
generateBenchmark.sh
cd ..

configFile="./config/algorithm"
benchmarkFile="./benchmarks/results/benchmark.bm"
algorithmNames="simple2pl mvcc2pl mvto"

for i in $algorithmNames
do
	echo "Benchmark: Processing $i"
	echo "$i" > "$configFile"
	./configure.sh $N
	./start.sh cluster "$benchmarkFile"
done

