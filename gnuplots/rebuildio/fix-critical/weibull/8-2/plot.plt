set term postscript eps color 25 font ",24"
set title "Per year rebuild IO when disk AFR=1% Declustered 8+2\n{/*0.8 50 disks, disk cap 20TB repair rate 50MB/s}" font ",26"
set ylabel "Per-year rebuild IO (TB)"
set xlabel 'Year'
set xrange [0:50]
set yrange [0:100]

set size 1.2, 1.4
set xtic 0,10,50
set ytic (0,20,40,60,80,100)
set key bottom right
set grid

set output 'rebuildio.eps'

plot \
'./to-healthy.txt'  u ($1):($2) title "Fix to healthy" w l lw 6 lc rgb 'green', \
'./to-degraded.txt'  u ($1):($2) title "Fix to degraded" w l lw 4 lc rgb 'blue', \
