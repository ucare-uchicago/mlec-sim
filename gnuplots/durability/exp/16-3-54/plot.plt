set term postscript eps color 25 font ",24"
set title "Durability of 57 disks 16+3 EC (disk cap 20TB IO 40MB/s)"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:7]
set xrange [0:14]

set size 1.2, 1.3
set ytic 0,1,7
set xtic (0,2,4,6,8,10,12,14)
set key bottom left
set grid

set output 'gpu-util.eps'

plot \
'./web-dp-16-3.txt'  u ($1):($2) title "Web DP 16+3" w l lw 6 lc rgb 'green', \
'./sim-dp-16-3.txt'  u ($1):($2):($3) title "Sim DP 16+3" w yerrorlines lw 4 lc rgb 'blue', \
'./web-raid-16-3.txt'  u ($1):($2) title "Web RAID 16+3" w l dt 2 lw 6 lc rgb 'green', \
'./sim-raid-16-3.txt'  u ($1):($2):($3) title "Sim RAID 16+3" w yerrorlines dt 2 lw 4 lc rgb 'blue', \