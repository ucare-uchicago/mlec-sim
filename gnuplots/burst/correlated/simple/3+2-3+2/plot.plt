set term postscript eps color 25 font ",24"
set title "Probability of data loss vs failed disks # for (3+2)(3+2)"
set ylabel 'Probability of data loss (%)'
set xlabel 'failures #'
set yrange [0:100]
set xrange [0:20]

set size 1.2, 1.4
set ytic 0,20,100
set xtic 0,1,20
set key top left
set grid

set output '3+2-3+2.eps'

plot \
'./simulator-3.txt'  u ($1):($2*100) title "simulator 3 racks" w p lw 5 ps 2.5 pt 2 lc rgb 'blue', \
'./brute-3.txt'  u ($1):($2*100) title "brute 3 racks" w p lw 5 ps 2 pt 7 lc rgb 'blue', \
'./simulator-4.txt'  u ($1):($2*100) title "simulator 4 racks" w p lw 5 ps 2.5 pt 2 lc rgb 'green', \
'./brute-4.txt'  u ($1):($2*100) title "brute 4 racks" w p lw 5 ps 2 pt 7 lc rgb 'green', \
'./simulator-5.txt'  u ($1):($2*100) title "simulator 5 racks" w p lw 5 ps 2.5 pt 2 lc rgb 'orange', \
'./brute-5.txt'  u ($1):($2*100) title "brute 5 racks" w p lw 5 ps 2 pt 7 lc rgb 'orange', \
