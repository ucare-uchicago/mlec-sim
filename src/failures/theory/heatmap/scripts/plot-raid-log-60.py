import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib
import math
import pandas as pd
import sys
import numpy as np 

plt.rcParams["font.family"] = "Times New Roman"
label_font = {'fontname':'Times New Roman', 'fontsize':'20'}
title_font_size = 28
tick_font_size = 22

if len(sys.argv) - 1 < 6:
    print("python plot_xxx.py filepath [cc/cd/dc/dd] [k_n] [p_n] [k_l] [p_l]")
    exit(-1)

input_file_path = sys.argv[1]
placements = {'cc': 'CP-CP', 'cd': 'CP-DP', 'dc': 'DP-CP', 'dd': 'DP-DP'}
if sys.argv[2] not in placements:
    print('wrong chunk placement.')
    exit(-1)

placement = placements[sys.argv[2]]

k_n = sys.argv[3]
p_n = sys.argv[4]
k_l = sys.argv[5]
p_l = sys.argv[6]



occuranceDataLoss = pd.read_csv(input_file_path, sep=' ')
# prob[i,j] means the probability 
print(occuranceDataLoss)

import matplotlib.pyplot as plt
import matplotlib
import math
figure, axes = plt.subplots()

axes.set_aspect( 1 )
x_range = [0,42]
y_range = [0,68]
axes.set_xlim(x_range)
axes.set_ylim(y_range)


def cal_radius(count):
    # slope = 11.8
    slope = 25
    # intercept = 2.4
    #   intercept = 4
    if count == 0:
        return 5
    intercept = 10
    return slope * math.log10(count) + intercept

def coloring(x):
    if x == 0:
        return 'green'
    elif x <=0.0000001:
        return 'lightgreen'
    elif x <= 0.000001:
        return 'yellow'
    elif x <= 0.00001:
        return 'lightblue'
    elif x <= 0.0001:
        return 'blue'
    elif x <= 0.001:
        return 'orange'
    elif x <= 0.01:
        return 'orchid'
    elif x <= 0.1:
        return 'brown'
    elif x <= 0.99:
        return 'purple'
    else:
        return 'red'

survival_prob = 1
total_count = 0
single_burst_survival_prob = 0

for index, row in occuranceDataLoss.iterrows():
    disks = row['failed_disks']
    racks = row['affected_racks']
    dl = float(row['dl_prob'])
    dl_color = coloring(dl)
    count = 1
    radius = cal_radius(count)
    # if count == 0:
        # axes.plot(racks, disks,marker='o',ms=radius,mfc=dl_color,mec=dl_color, mew=0.1)
    axes.add_patch(patches.Rectangle((racks-0.5, disks-0.5), 1, 1, linewidth=0.15, edgecolor=(1,1,1, 1), facecolor=dl_color))




# plt.legend()

plt.xlabel('Number of racks affected', fontsize=title_font_size)
plt.ylabel('Number of drives affected', fontsize=title_font_size)
# plt.title('Frequency of failure bursts sorted by racks and drives affected')
plt.title('({}+{})/({}+{}) MLEC {}\n'.format(k_n, p_n, k_l, p_l, placement), fontsize=title_font_size)
# plt.title(r'$\mathdefault{(18+2)/(18+2)}$ MLEC CP-CP\n', fontsize=24)

plt.xticks([0,10,20,30,40],fontsize=tick_font_size)
plt.yticks([0,10,20,30,40,50,60],fontsize=tick_font_size)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())


label_y = 64.8
plt.text(0.8, label_y, '0', **label_font)
plt.text(4.5, label_y, r'$\mathdefault{10^{-7}}$', **label_font)
plt.text(10, label_y, r'$\mathdefault{10^{-6}}$', **label_font)
plt.text(15, label_y, r'$\mathdefault{10^{-5}}$', **label_font)
plt.text(20, label_y, r'$\mathdefault{10^{-4}}$', **label_font)
plt.text(25, label_y, r'$\mathdefault{10^{-3}}$', **label_font)
plt.text(30, label_y, r'$\mathdefault{10^{-2}}$', **label_font)
plt.text(35, label_y, r'$\mathdefault{10^{-1}}$', **label_font)
plt.text(40.2, label_y, r'$\mathdefault{1}$', **label_font)

import matplotlib as mpl
import matplotlib.colors as colors

dd = 10**(-100)  # a number that is very close to 0
hc = ['green', 'green', 'lightgreen', 'lightgreen','yellow', 'yellow', 'lightblue', 'lightblue', 'blue', 'blue', 'orange', 'orange', 'orchid',  'orchid', 'brown', 'brown', 'purple', 'purple', 'red', 'red']
th = [0,       0.01,    0.01+dd,   0.125,   0.125+dd,    0.25,         0.25+dd,      0.375,      0.375+dd,   0.5,  0.5+dd, 0.625,  0.625+dd,  0.75,     0.75+dd, 0.875,    0.875+dd, 0.99,      0.99+dd, 1]


mycolors=list(zip(th, hc))
cm = colors.LinearSegmentedColormap.from_list('test', mycolors)

# As for a more fancy example, you can also give an axes by hand:
c_map_ax = figure.add_axes([0.192, 0.8191, 0.64, 0.015])
c_map_ax.axes.get_xaxis().set_visible(False)
c_map_ax.axes.get_yaxis().set_visible(True)

# and create another colorbar with:
mpl.colorbar.ColorbarBase(c_map_ax, cmap=cm, orientation = 'horizontal')

figure.set_size_inches(8, 11)
figure.set_dpi(500)

plt.show()

plt.savefig(sys.argv[1] + '.60.png',  bbox_inches='tight')