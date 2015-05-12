#!/bin/sh

cat ./results.bm | grep -i "#BM-" | sed -e 's/#BM- //' > parsed.bm

echo "parsed.bm" | gnuplot script.gp
