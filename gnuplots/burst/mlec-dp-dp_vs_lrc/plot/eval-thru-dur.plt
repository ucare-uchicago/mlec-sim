set terminal postscript eps enhanced color 22 font ",36"
set output "eps/eval-thru-dur.eps"

set size 1.2,1.2

set print "-"

set origin 0,0
set size 1.15,1.2

lm = 0.25
bm = 0.3
tm = 1.1
gap = 0.03
size = 0.75
x1=0.0; x2=18; x3=996.; x4=1000.

set multiplot
set border 1+2
set notitle
set datafile separator "\t"
set xlabel "Prob to survive ORNL bursts (nines)"
set ylabel "Throughput (MB/s)"


set bmargin at screen bm
set tmargin at screen tm
set lmargin at screen lm
set rmargin at screen lm + size * (1.* abs(x2-x1) / (abs(x2-x1) + abs(x4-x3) ) )


set yrange [0:10000]
set xrange [x1:x2]

set xtics nomirror (0,5,10,15)
set ytics nomirror (\
           "0" 0, \
           "2K" 2000, \
           "4K" 4000, \
           "6K" 6000, \
           "8K" 8000, \
           "10K" 10000,\
           "12K" 12000,\
           "14K" 14000) 

set key top right outside
set key font ",35"
# unset key
#unset xlabel

mlec_label_color = 'blue'
lrc_label_color = 'red'

set label "(16,2,2)" at 2,6830 font ',30' tc rgb lrc_label_color
set label "(24,3,3)" at 3.8,5630 font ',30' tc rgb lrc_label_color
set label "(32,4,4)" at 4.2,2850 font ',30' tc rgb lrc_label_color
set label "(32,2,6)" at 11,2000 font ',30' tc rgb lrc_label_color


set label "(9+1)/(9+1)" at 0.3,9900 font ',30' tc rgb mlec_label_color
set label "(9+1)/(18+2)" at 0.8,8300 font ',30' tc rgb mlec_label_color
set label "(18+2)/(9+1)" at 0.6,1000 font ',30' tc rgb mlec_label_color
set label "(18+2)/(18+2)" at 7,4500 font ',30' tc rgb mlec_label_color
set label "(18+2)/(27+3)" at 18,4300 font ',30' tc rgb mlec_label_color


set arrow from 1.2,7800 to 0.2,6500 lc rgb mlec_label_color nohead
set arrow from 1.5,1500 to 0.9,4500 lc rgb mlec_label_color nohead

plot \
'dat/mlec.dat' using ($3):($4) with points title "MLEC DP-DP" ps 3 pt 7 lc rgb "blue", \
'dat/lrc.dat' using ($3):($4) with points title "LRC DP" ps 3 pt 7 lc rgb "red", \



unset ytics
unset ylabel
unset xlabel

set xtics nomirror (\
           "Inf" 1000)
set border 1
set lmargin at screen lm + size * (1. * abs(x2-x1) / (abs(x2-x1) + abs(x4-x3) ) ) + gap
set rmargin at screen lm + size + gap
set xrange [x3:x4]


set arrow from screen lm + size * (1. * abs(x2-x1) / \
(abs(x2-x1)+abs(x4-x3) ) ) - gap / 4.0 , bm - gap / 4.0 to screen \
lm + size * (1. * abs(x2-x1) / (abs(x2-x1) + abs(x4-x3) ) ) + \
gap / 4.0 , bm + gap / 4.0 nohead


set arrow from screen lm + gap + size * (1. * abs(x2-x1) / \
(abs(x2-x1)+abs(x4-x3) ) ) - gap / 4.0 , bm - gap / 4.0 to screen \
lm + gap + size * (1. * abs(x2-x1) / (abs(x2-x1) + abs(x4-x3) ) ) + \
gap / 4.0 , bm + gap / 4.0 nohead


plot \
'dat/mlec.dat' using ($3):($4) with points notitle ps 3 pt 7 lc rgb "blue", \
'dat/lrc.dat' using ($3):($4) with points notitle ps 3 pt 7 lc rgb "red", \
