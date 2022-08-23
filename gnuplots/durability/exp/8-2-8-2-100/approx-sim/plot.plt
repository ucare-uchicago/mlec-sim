set term postscript eps color 25 font ",24"
set title "Durability of 100 disks (8+2)(8+2) MLEC (disk cap 100TB IO 10MB/s)"
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
'./web-raid-mlec-8-2-8-2.txt'  u ($1):($2) title "Web (8+2)(8+2)" w l lw 6 lc rgb 'green', \
'./sim-raid-mlec-8-2-8-2.txt'  u ($1):($2):($3) title "Sim (8+2)(8+2)" w yerrorlines lw 4 lc rgb 'blue', \
