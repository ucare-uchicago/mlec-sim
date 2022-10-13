set term postscript eps color 25 font ",24"
set title "Probability of data loss vs failed disks # for (2+1)(6+1) \n 2 affected racks"
set ylabel '# nines'
set xlabel 'AFR(%)'
set yrange [0:100]
set xrange [0:14]

set size 1.2, 1.4
set ytic 0,20,100
set xtic 0,1,14
set key bottom right
set grid

set output 'dl.eps'

plot \
'./simulator.txt'  u ($1):($2*100) title "simulator" w p lw 5 ps 2.5 pt 2 lc rgb 'blue', \
'./formula.txt'  u ($1):($2*100) title "formula" w p lw 5 ps 2 pt 7 lc rgb 'orange', \