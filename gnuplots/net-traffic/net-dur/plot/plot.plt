set terminal postscript eps enhanced color 22 font ",32"
set output "eps/eval-thru-dur.eps"


set title "Annual network traffic of 180 disk (disk cap 20TB IO 50MB/s)"

set size 1.4,1.4


set multiplot layout 1,1
set origin 0,0
set size 1.4,1.4
# first figur1 
set border 1+2
set datafile separator "\t"
set xlabel "Durability (nines)"
set ylabel "Annual Network Traffic (TB)"
set xrange [0:23]
set yrange [0:800]

set xtics nomirror (0,5,10,15,20)

set ytics nomirror (\
           "0" 0, \
           "200" 200, \
           "400" 400, \
           "600" 600, \
           "800" 800) 
#set ytics nomirror (0,5,10,15)

#set key at 13,21
set key top right
set key font ",32"
# unset key
#unset xlabel

set label "(3+1)" at 1.5,200 font ',25'
set label "(5+2)" at 4,280 font ',25'
set label "(7+3)" at 7,350 font ',25'
set label "(10+4)" at 9.5,450 font ',25'
set label "(12+5)" at 12,550 font ',25'


set label "(4+1)/(8+1)" at 5,50 font ',30'
set label "(8+2)/(8+1)" at 10.5,50 font ',30'
set label "(8+2)/(16+2)" at 17,50 font ',30'


set arrow from 44,3800 to 42,4600 lw 5

plot \
'dat/cap-30.dat' using ($6):($5) with points title "Net-SLEC" ps 3 pt 5 lc rgb "red", \
'dat/mlec.dat' using ($5):($6) with points title "MLEC" ps 3 pt 7 lc rgb "blue", \
#'dat/mlec.dat' using ($5):($6):(sprintf("(%d+%d)(%d+%d)", $1, $2, $3, $4)) with labels offset char -0.2,0.8 font ',35' notitle, \
#'dat/cap-30.dat' using ($6):($5):(sprintf("(%d+%d)", $2, $3)) with labels offset char 0,-0.7 font ',35' notitle, \
