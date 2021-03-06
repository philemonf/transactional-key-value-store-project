#! /bin/sh

if [ $# -lt 1 ];
then
	echo "Specify number of nodes"
	exit 1
fi

# Number of nodes
N=$1

cd ./benchmarks/
./generateBenchmark.sh $1
cd ..

configFile="./config/algorithm"
benchmarkFile="./benchmarks/results/benchmark.bm"
algorithmNames="simple_2pl mvcc2pl mvto"

if [ $# -eq 2 ]; 
then
	algorithmNames=$2
fi


for i in $algorithmNames
do
	echo "Benchmark: Processing $i"
	echo "$i" > "$configFile"
	./configure.sh $N
	./start.sh cluster "$benchmarkFile"
done
