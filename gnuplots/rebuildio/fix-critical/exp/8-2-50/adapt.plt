set term postscript eps color 25 font ",24"
set title "Durability of 50 disks 8+2 EC (disk cap 20TB IO 100MB/s)"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:7]
set xrange [0:12]

set size 1.2, 1.4
set ytic 0,1,7
set xtic (0,2,4,6,8,10)
set key bottom left
set grid

set output 'adapt.eps'

plot \
'./sim-dp-8-2.txt'  u ($1):($2):($3) title "Sim DP Method 1" w yerrorlines lw 4 lc rgb 'blue', \
'./sim-dp-8-2-adapt.txt'  u ($1):($2):($3) title "Sim DP Method 2" w yerrorlines lw 6 lc rgb 'green', \