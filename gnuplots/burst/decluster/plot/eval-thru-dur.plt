set terminal postscript eps enhanced color 22 font ",35"
set output "eps/eval-thru-dur.eps"

set size 1.2,1.2


set multiplot layout 1,1
set origin 0,0
set size 1.2,1.2
# first figur1 
set border 1+2
set title "ORNL burst durability vs encoding throughput"
set datafile separator "\t"
set xlabel "Durability (nines)"
set ylabel "Throughput (MB/s)"
set xrange [0:8]
set yrange [0:12000]

set xtics nomirror (0,2,4,6,8)

set ytics nomirror (\
           "0" 0, \
           "2K" 2000, \
           "4K" 4000, \
           "6K" 6000, \
           "8K" 8000, \
           "10K" 10000, \
           "12K" 12000) 
#set ytics nomirror (0,5,10,15)

#set key at 13,21
set key top right
set key font ",35"
# unset key
#unset xlabel

set label "N(8+2)" at 0.35,11200 font ',20'
set label "N(12+3)" at 0.7,7500 font ',20'
set label "N(16+4)" at 0.85,6200 font ',20'
set label "N(20+5)" at 1,5000 font ',20'
set label "N(24+6)" at 1.05,4200 font ',20'


set label "L(8+2)" at 2.5,12000 font ',20'
set label "L(12+3)" at 2.5,8300 font ',20'
set label "L(16+4)" at 3.0,6700 font ',20'
set label "L(20+5)" at 3.0,5500 font ',20'
set label "L(24+6)" at 2.6,3900 font ',20'


set label "(9+1)/(9+1)" at 4.1,8500 font ',20'
set label "(9+1)/(18+2)" at 4.25,5800 font ',20'
set label "(18+2)/(9+1)" at 6,3700 font ',20'

set object rectangle at 6.3,12000 size char 0.7, char 0.4 \
    fillcolor rgb 'orange' fillstyle solid

set object rectangle at 6.3,11000 size char 0.7, char 0.4 \
    fillcolor rgb 'red' fillstyle solid

set object circle at 6.3,10000 radius char 0.4 \
    fillcolor rgb 'blue' fillstyle solid

set label "Net-only Declus" at 6.5,12000 font ',20'
set label "Local-only Declus" at 6.5,11000 font ',20'
set label "MLEC Declus" at 6.5,10000 font ',20'



plot \
'dat/local.dat' using ($2):($3) with points notitle ps 2.5 pt 5 lc rgb "red", \
'dat/network.dat' using ($2):($3) with points notitle ps 2.5 pt 5 lc rgb "orange", \
'dat/mlec.dat' using ($2):($3) with points notitle ps 2.5 pt 7 lc rgb "blue", \
#'dat/mlec.dat' using ($5):($6):(sprintf("(%d+%d)(%d+%d)", $1, $2, $3, $4)) with labels offset char -0.2,0.8 font ',35' notitle, \
#'dat/cap-30.dat' using ($6):($5):(sprintf("(%d+%d)", $2, $3)) with labels offset char 0,-0.7 font ',35' notitle, \