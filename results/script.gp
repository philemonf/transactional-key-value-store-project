set terminal png
set output "test.png"

# read shell input
# the echo prints the variable, which is then piped to gnuplot
fname = system("read filename; echo $filename")

#	users 	keys 	ratio 	nbReadTotal 	nbReadAbortsTotal 	nbWriteTotal 	nbWriteAbortsTotal 	nbCommitTotal 	nbAbortTotal 	latency	throughput 	abortRate localityPercentage
#	1	2	3	4		5			6		7			8		9		10	11		12		13
plot for[col=10:11] fname using 1:col title columnheader(col) with lines