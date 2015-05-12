How do I run a benchmark ?
==========================

# Prerequisites

    1) Select the algorithm you want to use
    2) Configure the number of nodes

    For both steps, please refer to the main README.

# Generate a custom benchmark

    1) Modify parameters in generateBenchmark.sh script

        WARNING: The number of nodes in the script must be the same
        number you used to configure the system in Prerequisites

    2) Run the script, it will create a .bm file with all benchmark's tests in folder `results`

# Running the benchmark

    1) run `start.sh cluster <benchmarkFileName>`

    2) It will create a `results.csv` file in the `results` folder

# Plotting the result

    1) Go into folder `results`

    2) Use the corresponding scripts in ../scripts on results.csv