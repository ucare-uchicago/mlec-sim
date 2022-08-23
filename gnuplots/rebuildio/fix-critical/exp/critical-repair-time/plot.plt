#!/usr/bin/env gnuplot
set title "Critical repair time when 2 disks fail\n{/*0.8 50 disks, disk cap 20TB repair rate 50MB/s}" font ",18"
set terminal postscript eps enhanced color 22 font ",15"
set output "eps/plot.eps"

set boxwidth 1
set style fill pattern border
set key top right horizontal outside

set ylabel "Critical repair time (hours)"

set yrange [0 : 20]
#set xtics 3
set xrange [9 : 16]
set xtics format ""
#set logscale y 10
#set format y "10^{%L}"
set bmargin 3

set xtics ('8+2'  11, \
           "16+2"  14)

set ytic 0,5,20
           
set size 0.6,0.50
set origin 0.1,0.1

set arrow 1 from 11.25,4.6 to 11.25,5.8 lc rgb 'red' size screen 0.03,10 front
set label '11%' at 11.4,5.5 tc rgb 'red' font ",12"

set arrow 2 from 14.25,15.5 to 14.25,17 lc rgb 'red' size screen 0.03,10 front
set label '6%' at 14.4,16.1 tc rgb 'red' font ",12"

plot \
'dat/to-degraded.dat' u 1:2 with boxes title "To degraded" fc rgb "blue" fs solid , \
'dat/to-healthy.dat' u 1:2 with boxes title "To healthy" fc rgb "green" fs solid , \