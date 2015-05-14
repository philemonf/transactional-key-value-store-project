How do I run a benchmark ?
==========================

# Generate a custom benchmark

    1) Modify parameters in generateBenchmark.sh script
        No need to run this script, it will be done during next step.
    
# Running the benchmark

    1) run `benchmark.sh <#Nodes> <Name of the algorithm>` on the cluster
        <Name of the algorithm> is optional. If this field is not set, all the algorithms will run the benchmark
        If you only want to run one algorithm, specify `simple_2pl`, `mvcc2pl` or `mvto`
        
    2) It will create a `.csv` file for each algorithm run in the `benchmarks/results/` folder.
