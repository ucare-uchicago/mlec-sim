set term postscript eps color 25 font ",24"
set title "Durability of first year of RAID 7+1 EC \n{/*0.8 disk 20TB IO 50MB/s AFR=1% Weibull distribution}" font ",28"
set ylabel '# nines'
set xlabel 'beta'
set yrange [0:7]
set xrange [0:2.3]

set size 1.2, 1.4
set ytic 0,1,7
set xtic (0,1,1.2,1.4,1.6,1.8, 2.0, 2.2)
set key bottom left
set grid

set output 'nines.eps'

plot \
'./web-raid-7-1-disk8.txt'  u ($1):($2) title "Web 8 disks" w l dt 2 lw 6 lc rgb 'green', \
'./sim-raid-7-1-disk8.txt'  u ($1):($2):($3) title "Sim 8 disks" w yerrorlines dt 2 lw 4 lc rgb 'blue', \
'./web-raid-7-1-disk80.txt'  u ($1):($2) title "Web 80 disks" w l lw 8 lc rgb 'green', \
'./sim-raid-7-1-disk80.txt'  u ($1):($2):($3) title "Sim 80 disks" w yerrorlines lw 4 lc rgb 'blue', \