#!/usr/bin/env gnuplot
set title "Network Traffic of 432 disks\n{/*0.8 (7+1)(16+2) CORVAULT 54 disks 20TB repair rate 50MB/s}" font ",18"
set terminal postscript eps enhanced color 22 font ",15"
set output "eps/plot.eps"

set boxwidth 1
set style fill pattern border
set key top right horizontal outside

set ylabel "network traffic (TB)"

set yrange [0 : 2]
#set xtics 3
set xrange [9 : 16]
set xtics format ""
#set logscale y 10
#set format y "10^{%L}"
set bmargin 3

set xtics ('RAID'  11, \
           "DP"  14)

set ytic 0,0.5,2
           
set size 0.6,0.50
set origin 0.1,0.1


plot \
'dat/repair-all.dat' u 1:2 with boxes title "repair CORVAULT" fc rgb "blue" fs solid , \
'dat/disks-only.dat' u 1:2 with boxes title "repair disks only" fc rgb "green" fs solid , \