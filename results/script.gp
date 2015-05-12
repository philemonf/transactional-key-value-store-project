set terminal png
set output "test.png"

# read shell input
# the echo prints the variable, which is then piped to gnuplot
fname = system("read filename; echo $filename")

plot for[col=11:12] fname using 1:col title columnheader(col) with lines 

