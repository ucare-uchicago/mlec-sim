set term postscript eps color 25 font ",24"
set title "Durability of 100 disks (8+2)(8+2) MLEC (AFR 5%)"
set ylabel '# nines'
set xlabel 'disk capacity'
set yrange [0:16]
set xrange [0:110]

set size 1.2, 1.4
set ytic 0,2,16
set xtic 0,20,100
set key bottom left
set grid

set output 'cap.eps'

set label 'max diff 0.39' at 85,6 tc rgb 'red' font ",22"
set label 'max diff 0.17' at 43,13.2 tc rgb 'red' font ",22"

set arrow 1 from 102,6.5 to 102,6.895 heads lc rgb 'red' size screen 0.03,10
set arrow 2 from 42,12.732 to 42,12.9 heads lc rgb 'red' size screen 0.03,10 front
 
plot \
'./web-raid-mlec-8-2-8-2-cap-io50.txt'  u ($1):($2) title "Math | IO 50MB/s" w lp pt 7 lw 6 dt 2 lc rgb 'green', \
'./sim-raid-mlec-8-2-8-2-cap-io50-apx2.txt'  u ($1):($2) title "Sim | IO 50MB/s" w lp pt 5 lw 4 dt 2 lc rgb 'blue', \
'./web-raid-mlec-8-2-8-2-cap-io20.txt'  u ($1):($2) title "Math | IO 20MB/s" w lp pt 7 lw 6 lc rgb 'green', \
'./sim-raid-mlec-8-2-8-2-cap-io20-apx1.txt'  u ($1):($2) title "Sim | IO 20MB/s" w lp pt 5 lw 4 lc rgb 'blue', \
