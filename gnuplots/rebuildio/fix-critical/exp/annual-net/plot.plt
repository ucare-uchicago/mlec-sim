#!/usr/bin/env gnuplot
set title "Annual rebuild IO when disk AFR=1%\n{/*0.8 50 disks, disk cap 20TB repair rate 50MB/s}" font ",18"
set terminal postscript eps enhanced color 22 font ",15"
set output "eps/plot.eps"

set boxwidth 1
set style fill pattern border
set key top right horizontal outside

set ylabel "Annual rebuild IO (TB)"

set yrange [0 : 200]
#set xtics 3
set xrange [9 : 16]
set xtics format ""
#set logscale y 10
#set format y "10^{%L}"
set bmargin 3

set xtics ('8+2'  11, \
           "16+2"  14)

set ytic 0,50,200
           
set size 0.6,0.50
set origin 0.1,0.1

set arrow 1 from 11.15,110 to 11.15,93 lc rgb 'red' size screen 0.03,10
set label '0.07%' at 11.25,100 tc rgb 'red' font ",12"

set arrow 2 from 14.15,190 to 14.15,173 lc rgb 'red' size screen 0.03,10
set label '0.09%' at 14.25,180 tc rgb 'red' font ",12"


plot \
'dat/to-degraded.dat' u 1:2 with boxes title "To degraded" fc rgb "blue" fs solid , \
'dat/to-healthy.dat' u 1:2 with boxes title "To healthy" fc rgb "green" fs solid , \