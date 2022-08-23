set term postscript eps color 25 font ",24"
set title "Durability of 50 disks Declustered parity (disk cap 20TB IO 50MB/s)"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:7]
set xrange [0:12]

set size 1.2, 1.4
set ytic 0,1,7
set xtic (0,2,4,6,8,10)
set key bottom left
set grid

set output 'gpu-util.eps'

plot \
'./sim-adapt-8-2.txt'  u ($1):($2) title "Fix to degraded 8+2" w yerrorlines lw 6 lc rgb 'green', \
'./sim-dp-8-2.txt'  u ($1):($2):($3) title "Fix to healthy 8+2" w yerrorlines lw 4 lc rgb 'blue', \
'./sim-adapt-16-2.txt'  u ($1):($2) title "Fix to degraded 16+2" w yerrorlines dt 2 lw 6 lc rgb 'green', \
'./sim-dp-16-2.txt'  u ($1):($2):($3) title "Fix to healthy 16+2" w yerrorlines dt 2 lw 4 lc rgb 'blue', \