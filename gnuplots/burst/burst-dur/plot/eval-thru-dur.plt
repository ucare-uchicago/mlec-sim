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
set xlabel "1-year Durability (nines)"
set ylabel "Burst durability (nines)"
set xrange [0:50]
set yrange [0:4]

set xtics nomirror (0,10,20,30,40,50)

# set ytics nomirror (\
#           "0" 0, \
#           "2K" 2000, \
#           "4K" 4000, \
#           "6K" 6000, \
#           "8K" 8000) 
set ytics nomirror (0,1,2,3,4)

#set key at 13,21
set key bottom right
set key font ",35"
# unset key
#unset xlabel

set label "(5+2)" at 1.9,1.4 font ',35'
set label "(10+4)" at 9,1.7 font ',35'
set label "(15+6)" at 17,1.9 font ',35'
set label "(20+8)" at 24,2.1 font ',35'
set label "(25+10)" at 32.45,2.2 font ',35'
set label "(30+12)" at 31,2.8 font ',35'


set label "(8+1)/(4+1)" at 1,2.3 font ',35'
set label "(16+2)/(8+2)" at 14.7,3 font ',35'
set label "(24+3)/(12+3)" at 35.6,3.35 font ',35'


set arrow from 44,3800 to 42,4600 lw 5
set label "better" at 40.7,3450 font ',40'

plot \
'dat/cap-30.dat' using ($6):($7) with points title "local SLEC DP" ps 3 pt 5 lc rgb "red", \
'dat/mlec.dat' using ($5):($7) with points title "MLEC DP" ps 3 pt 7 lc rgb "blue", \
#'dat/mlec.dat' using ($5):($6):(sprintf("(%d+%d)(%d+%d)", $1, $2, $3, $4)) with labels offset char -0.2,0.8 font ',35' notitle, \
#'dat/cap-30.dat' using ($6):($5):(sprintf("(%d+%d)", $2, $3)) with labels offset char 0,-0.7 font ',35' notitle, \