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
set style line 4 lt rgb "#F25900" lw 2 pt 9

set output "../results/graphs/Merge_Latency.pdf"
set title "Latency"
set xlabel "#Transactions"
set ylabel "ms/transaction"
plot col=9 "MVTO_parsed.bm" using 2:col title "MVTO" with lines, col=9 "MVCC2PL_parsed.bm" using 2:col title "MVCC2PL" with lines, col=9 "2PL_parsed.bm" using 2:col title "2PL" with lines

########################################################################


set output "../results/graphs/Merge_Thoughput.pdf"
set title "Throughput"
set xlabel "#Transactions"
set ylabel "transactions/s"

plot col=8 "MVTO_parsed.bm" using 2:col title "MVTO" with lines, col=8 "MVCC2PL_parsed.bm" using 2:col title "MVCC2PL" with lines, col=8 "2PL_parsed.bm" using 2:col title "2PL" with lines

########################################################################

set output "../results/graphs/Merge_AbortRate.pdf"
set title "Abort Rate"
set xlabel "#Transactions"
set ylabel "aborts/s"
plot col=10 "MVTO_parsed.bm" using 2:col title "MVTO" with lines, col=10 "MVCC2PL_parsed.bm" using 2:col title "MVCC2PL" with lines, col=10 "2PL_parsed.bm" using 2:col title "2PL" with lines
