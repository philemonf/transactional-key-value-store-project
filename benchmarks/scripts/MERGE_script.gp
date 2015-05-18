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
set style line 4 lt rgb "#F25900" lw 2 pt 9

set output "../results/graphs/Merge_Latency.pdf"
set title "Latency"
set xlabel "#Transactions"
set ylabel "ms/transaction"
plot "MVTO_parsed.bm" using 2:9 title "MVTO" with lines, "MVCC2PL_parsed.bm" using 2:9 title "MVCC2PL" with lines, "2PL_parsed.bm" using 2:9 title "2PL" with lines

########################################################################


set output "../results/graphs/Merge_Thoughput.pdf"
set title "Throughput"
set xlabel "#Transactions"
set ylabel "transactions/s"

plot "MVTO_parsed.bm" using 2:8 title "MVTO" with lines, "MVCC2PL_parsed.bm" using 2:8 title "MVCC2PL" with lines, "2PL_parsed.bm" using 2:8 title "2PL" with lines

########################################################################

set output "../results/graphs/Merge_AbortRate.pdf"
set title "Abort Rate"
set xlabel "#Transactions"
set ylabel "aborts/s"
plot "MVTO_parsed.bm" using 2:10 title "MVTO" with lines, "MVCC2PL_parsed.bm" using 2:10 title "MVCC2PL" with lines, "2PL_parsed.bm" using 2:10 title "2PL" with lines
