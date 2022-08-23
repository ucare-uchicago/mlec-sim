set term postscript eps color 25 font ",24"
set title "Durability of 45 disks 8+1 EC (disk cap 20TB IO 100MB/s)"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:4]
set xrange [0:12]

set size 1.2, 0.92
set ytic 0,1,4
set xtic (0,2,4,6,8,10)
set key bottom left
set grid

set output 'dp-raid.eps'

plot \
'./sim-dp-8-1.txt'  u ($1):($2):($3) title "DP" w yerrorlines lw 4 lc rgb 'orange', \
'./sim-raid-8-1.txt'  u ($1):($2):($3) title "RAID" w yerrorlines dt 2 lw 4 lc rgb 'red', \