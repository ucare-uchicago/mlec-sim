set terminal postscript eps enhanced color 22 font ",45"
set output "eps/eval-thru-dur.eps"

set size 1.2,1.2


set multiplot layout 1,1
set origin 0,0
set size 1.2,1.2
# first figur1 
set border 1+2
set notitle
set datafile separator "\t"
set xlabel "Burst Durability (nines)"
set ylabel "Throughput (MB/s)"
set yrange [0:8000]
set xrange [0:4]

set xtics nomirror (0,1,2,3,4)

 set ytics nomirror (\
           "0" 0, \
           "2K" 2000, \
           "4K" 4000, \
           "6K" 6000, \
           "8K" 8000) 
#set ytics nomirror (0,1,2,3,4)

#set key at 13,21
set key top center outside
set key font ",35"
# unset key
#unset xlabel

set label "(5+2)" at 1,5930 font ',35'
set label "(10+4)" at 1.2,3700 font ',35'
set label "(15+6)" at 1.4,2600 font ',35'
set label "(20+8)" at 1.6,1350 font ',35'
set label "(25+10)" at 2.5,1150 font ',35'
set label "(30+12)" at 2.6,400 font ',35'


set label "(8+1)/(4+1)" at 1.8,7450 font ',35'
set label "(16+2)/(8+2)" at 2.2,5600 font ',35'
set label "(24+3)/(12+3)" at 2.8,4150 font ',35'


set arrow from 44,3800 to 42,4600 lw 5
set label "better" at 40.7,3450 font ',40'

plot \
'dat/cap-30.dat' using ($7):($5) with points title "local SLEC DP" ps 3 pt 5 lc rgb "red", \
'dat/mlec.dat' using ($7):($6) with points title "MLEC DP" ps 3 pt 7 lc rgb "blue", \
#'dat/mlec.dat' using ($5):($6):(sprintf("(%d+%d)(%d+%d)", $1, $2, $3, $4)) with labels offset char -0.2,0.8 font ',35' notitle, \
#'dat/cap-30.dat' using ($6):($5):(sprintf("(%d+%d)", $2, $3)) with labels offset char 0,-0.7 font ',35' notitle, \