set term postscript eps color 25 font ",24"
set title "Durability of first year of RAID 8+2 EC \n{/*0.8 disk 20TB IO 50MB/s AFR=2% Weibull distribution}" font ",28"
set ylabel '# nines'
set xlabel 'beta'
set yrange [0:8]
set xrange [0:2.3]

set size 1.2, 1.4
set ytic 0,1,8
set xtic (0,1,1.2,1.4,1.6,1.8, 2.0, 2.2)
set key bottom left
set grid

set output 'nines.eps'

plot \
'./web-raid-8-2-disk10.txt'  u ($1):($2) title "Web 10 disks" w l dt 2 lw 6 lc rgb 'green', \
'./sim-raid-8-2-disk10.txt'  u ($1):($2):($3) title "Sim 10 disks" w yerrorlines dt 2 lw 4 lc rgb 'blue', \
'./web-raid-8-2-disk100.txt'  u ($1):($2) title "Web 100 disks" w l lw 8 lc rgb 'green', \
'./sim-raid-8-2-disk100.txt'  u ($1):($2):($3) title "Sim 100 disks" w yerrorlines lw 4 lc rgb 'blue', \