set term postscript eps color 25 font ",24"
set title "Durability of 432 disks MLEC (7+1)(16+2)\n(disk cap 20TB IO 50MB/s)"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:12]
set xrange [0:12]

set size 1.2, 1.4
set ytic 0,2,12
set xtic (0,2,4,6,8,10)
set key bottom left
set grid

set output 'repair-disk-or-group.eps'

plot \
'./sim-dp-mlec-7-1-16-2-disk.txt'  u ($1):($2) title "Repair disks only | local DP   " w lp lw 6 lc rgb 'green', \
'./sim-dp-mlec-7-1-16-2-group.txt'  u ($1):($2) title "Repair CORVAULT | local DP   " w lp lw 4 lc rgb 'blue', \
'./sim-raid-mlec-7-1-16-2-disk.txt'  u ($1):($2) title "Repair disks only | local RAID" w lp lw 6 lc rgb 'red' dt 2, \
'./sim-raid-mlec-7-1-16-2-group.txt'  u ($1):($2) title "Repair CORVAULT | local RAID" w lp lw 4 lc rgb 'orange' dt 2, \