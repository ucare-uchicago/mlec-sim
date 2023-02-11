set term postscript eps color 25 font ",24"
set title "MLEC Declustered-Clustered (9+1)/(9+1))\n40 racks, each rack contains 800 disks"
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
'./theory-2rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "theory 2 racks" w lp lw 5 ps 0 pt 7 lc rgb 'blue', \
'./sim-2rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5):($6) title "simulator 2 racks" w yerrorbars lw 5 ps 1.5 pt 7 lc rgb 'orange', \
'./theory-4rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5) title "theory 4 racks" w lp lw 5 ps 0 pt 7 lc rgb 'green', \
'./sim-4rack.txt'  u ($3):($5 == 0 ? 0.000001 : $5):($6) title "simulator 4 racks" w yerrorbars lw 5 ps 1.5 pt 7 lc rgb 'purple', \