set term postscript eps color 25 font ",24"
set title "Probability of data loss vs failed disks # for network-only SLEC 8+2\n40 racks, each rack contains 800 disks"
set ylabel 'Prob of Data Loss'
set xlabel 'disk failures #'
set yrange [0.000001:1]
set xrange [0:50]

set logscale y 10

set size 1.2, 1.4
set ytic ('0' 0.000001, '0.00001' 0.00001, '0.0001' 0.0001, '0.001' 0.001, '0.01' 0.01, '0.1' 0.1, '1' 1)
set xtic 0,10,100
set key bottom right
set grid

set output 'dl.eps'

plot \
'./theory-4rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "theory 4 racks" w lp lw 5 ps 0 pt 7 lc rgb 'blue', \
'./sim-4rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "simulator 4 racks" w p lw 5 ps 1.5 pt 7 lc rgb 'orange', \
'./theory-8rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "theory 8 racks" w lp lw 5 ps 0 pt 7 lc rgb 'green', \
'./sim-8rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "simulator 8 racks" w p lw 5 ps 1.5 pt 7 lc rgb 'purple', \