set term postscript eps color 25 font ",24"
set title "correlated failures in 2 racks in ORNL (40 racks x 8 enclores x 100 disks)"
set ylabel 'Probability of data loss (%)'
set xlabel 'failures #'
set yrange [0.0001:100]
set xrange [0:20]

set size 1.2, 1.4
set ytic (0.0001, 0.001, 0.01, 0.1, 1, 10, 100)
set xtic 0,1,20
set key top left
set logscale y 10

set output '2racks.eps'

plot \
'./local-13+2.txt'  u ($1):($2*100) title "local 13+2" w p lw 5 ps 2.5 pt 2 lc rgb 'blue', \
'./mlec-9+1-19+1.txt'  u ($1):($2*100) title "mlec (9+1)/(19+1)" w p lw 5 ps 2 pt 7 lc rgb 'green', \
'./network-13+2.txt'  u ($1):($2*100) title "network 13+2" w p lw 5 ps 2 pt 9 lc rgb 'orange', \
