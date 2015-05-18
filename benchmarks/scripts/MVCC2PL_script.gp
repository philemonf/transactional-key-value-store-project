fname = system("read filename; echo $filename")
set terminal pdfcairo font "Gill Sans,9" linewidth 4 rounded fontscale 1.0

set style line 80 lt rgb "#808080"

set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80
set xtics nomirror
set ytics nomirror

set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2

set output "../results/graphs/MVCC2PL_Latency.pdf"
set title "MVCC2PL Latency"
set xlabel "#Transactions"
set ylabel "ms/transaction"
plot col=9 fname using 2:col title columnheader(col) with lines

########################################################################

set output "../results/graphs/MVCC2PL_Thoughput.pdf"
set title "MVCC2PL Throughput"
set xlabel "#Transactions"
set ylabel "transactions/s"
plot col=8 fname using 2:col title columnheader(col) with lines

########################################################################

set output "../results/graphs/MVCC2PL_AbortRate.pdf"
set title "MVCC2PL Abort Rate"
set xlabel "#Transactions"
set ylabel "aborts/s"
plot col=10 fname using 2:col title columnheader(col) with lines