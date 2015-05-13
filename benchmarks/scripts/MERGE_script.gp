#	users 	keys 	ratio 	nbReadTotal 	nbReadAbortsTotal 	nbWriteTotal 	nbWriteAbortsTotal 	nbCommitTotal 	nbAbortTotal 	latency	throughput 	abortRate localityPercentage
#	1	2	3	4		5			6		7			8		9		10	11		12		13
set terminal pdfcairo font "Gill Sans,9" linewidth 4 rounded fontscale 1.0

# Line style for axes
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

#set log x
#set mxtics 10    # Makes logscale look good.

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2
set style line 4 lt rgb "#F25900" lw 2 pt 9



set output "../results/graphs/Merge_Latency.pdf"
set title "Latency"
set xlabel "#Transactions"
set ylabel "ms/transaction"
plot col=10 "MVTO_parsed.bm" using 1:col title "MVTO" with lines, col=10 "MVCC2PL_parsed.bm" using 1:col title "MVCC2PL" with lines, col=10 "2PL_parsed.bm" using 1:col title "2PL" with lines

########################################################################


set output "../results/graphs/Merge_Thoughput.pdf"
set title "Throughput"
set xlabel "#Transactions"
set ylabel "transactions/s"

plot col=11 "MVTO_parsed.bm" using 1:col title "MVTO" with lines, col=11 "MVCC2PL_parsed.bm" using 1:col title "MVCC2PL" with lines, col=11 "2PL_parsed.bm" using 1:col title "2PL" with lines

########################################################################

set output "../results/graphs/Merge_AbortRate.pdf"
set title "Abort Rate"
set xlabel "#Transactions"
set ylabel "aborts/s"
plot col=12 "MVTO_parsed.bm" using 1:col title "MVTO" with lines, col=12 "MVCC2PL_parsed.bm" using 1:col title "MVCC2PL" with lines, col=12 "2PL_parsed.bm" using 1:col title "2PL" with lines
