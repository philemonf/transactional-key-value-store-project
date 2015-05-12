set terminal png

# read shell input
# the echo prints the variable, which is then piped to gnuplot
fname = system("read filename; echo $filename")

#	users 	keys 	ratio 	nbReadTotal 	nbReadAbortsTotal 	nbWriteTotal 	nbWriteAbortsTotal 	nbCommitTotal 	nbAbortTotal 	latency	throughput 	abortRate localityPercentage
#	1	2	3	4		5			6		7			8		9		10	11		12		13


set output "Latency.png"
set title "Latency"
set xlabel "#Transactions"
#set ylabel "seconds/transaction"
plot col=10 fname using 1:col title columnheader(col) with lines

########################################################################


set output "Thoughput.png"
set title "Throughput"
set xlabel "#Transactions"
#set ylabel "transactions/second"
plot col=11 fname using 1:col title columnheader(col) with lines

#, col2=7 fname using 1:col2 title columnheader(col2) with lines

########################################################################

set output "AbortRate.png"
set title "Abort Rate"
set xlabel "#Transactions"
#set ylabel "aborts/second"
plot col=12 fname using 1:col title columnheader(col) with lines