set terminal png

# read shell input
# the echo prints the variable, which is then piped to gnuplot
fname = system("read filename; echo $filename")

#	users 	keys 	ratio 	nbReadTotal 	nbReadAbortsTotal 	nbWriteTotal 	nbWriteAbortsTotal 	nbCommitTotal 	nbAbortTotal 	latency	throughput 	abortRate localityPercentage
#	1	2	3	4		5			6		7			8		9		10	11		12		13

set output "thoughputLatencyAbortrate.png"
set title "Throughput, Latency and Abort Rate"
set xlabel "#Transaction"
#set ylabel ""
plot for[col=10:12] fname using 1:col title columnheader(col) with lines

########################################################################

set output "test1.png"
set title ""
set xlabel ""
#set ylabel ""
plot col=6 fname using 1:col title columnheader(col) with lines, col2=7 fname using 1:col2 title columnheader(col2) with lines

