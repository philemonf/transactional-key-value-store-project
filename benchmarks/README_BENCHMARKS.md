How do I run a benchmark ?
==========================

# Prerequisites

    1) GnuPlot (http://www.gnuplot.info/)
    
    2) pdfCairo for GnuPlot (http://www.manpagez.com/info/gnuplot/gnuplot-4.4.0/gnuplot_387.php)

# Generate a custom benchmark

    1) Modify parameters in generateBenchmark.sh script
        No need to run this script, it will be done during next step.
    
# Running the benchmark

    1) run `benchmark.sh <#Nodes> <Name of the algorithm>` on the cluster
        <Name of the algorithm> is optional. If this field is not set, all the algorithms will run the benchmark
        If you only want to run one algorithm, specify `simple_2pl`, `mvcc2pl` or `mvto`
        
    2) It will create a `.csv` file for each algorithm run in the `../results/` folder.

# Plotting the result

    1) On your local computer, go into folder `results/scripts`
        The generated files will be downloaded using `scp` automatically in the next step.
        
    2) Run `./plot.sh <algorithm name> <Gaspar>`
        <algorithm name> can be `all` to generate all the graphs, `MVTO`, `2PL`, `MVCC2PL` to generate the plots individualy. Once the results have been fetched from the cluster, <Gaspar> is not needed.
        
    3) Go into folder ../results/graphs
