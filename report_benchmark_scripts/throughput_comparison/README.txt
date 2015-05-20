These benchmarks were run until none of the 2PLs algorithms encounter deadlocks.
This might take some trials. It can be made easier if the benchmark is run algorithm by algorithm (i.e. ./benchmark.sh <N> <algorithm>).

To disable the garbage collector in MVTO, one must comment the content of the `checkpoint` method in the MVTO.java file.