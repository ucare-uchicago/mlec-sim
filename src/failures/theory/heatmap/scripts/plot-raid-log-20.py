import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib
import math
import pandas as pd
import sys
import numpy as np 

occuranceDataLoss = pd.read_csv(sys.argv[1], sep=' ')
# prob[i,j] means the probability 
print(occuranceDataLoss)

import matplotlib.pyplot as plt
import matplotlib
import math
figure, axes = plt.subplots()

axes.set_aspect( 1 )
x_range = [0,22]
y_range = [0,25]
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
        return 'white'
    elif x <=0.0000001:
        return 'yellow'
    elif x <=0.000001:
        return 'lightgreen'
    elif x <= 0.00001:
        return 'lightblue'
    elif x <= 0.0001:
        return 'green'
    elif x <= 0.001:
        return 'blue'
    elif x <= 0.01:
        return 'orange'
    elif x <= 0.1:
        return 'orchid'
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
    if disks <= 20:
        axes.add_patch(patches.Rectangle((racks-0.5, disks-0.5), 1, 1, linewidth=0.5, edgecolor='black', facecolor=dl_color))




# plt.legend()

plt.xlabel('Number of racks affected', fontsize=18)
plt.ylabel('Number of drives affected', fontsize=18)
# plt.title('Frequency of failure bursts sorted by racks and drives affected')
plt.title('(18+2)/(18+2) MLEC CP-CP\n', fontsize=18)

plt.xticks([0,5,10,15,20],fontsize=16)
plt.yticks([0,5,10,15,20],fontsize=16)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

plt.text(0.5, 22.5, '0.0', fontsize=14)
plt.text(2.5, 22.5, r'$10^{-7}$', fontsize=14)
plt.text(5, 22.5, r'$10^{-6}$', fontsize=14)
plt.text(7.5, 22.5, r'$10^{-5}$', fontsize=14)
plt.text(10, 22.5, r'$10^{-4}$', fontsize=14)
plt.text(12.5, 22.5, r'$10^{-3}$', fontsize=14)
plt.text(15, 22.5, r'$10^{-2}$', fontsize=14)
plt.text(18, 22.5, r'$10^{-1}$', fontsize=14)
plt.text(20.5, 22.5, r'$1$', fontsize=14)

import matplotlib as mpl
import matplotlib.colors as colors

dd = 10**(-100)  # a number that is very close to 0
hc = ['white', 'white', 'yellow', 'yellow','lightgreen', 'lightgreen', 'lightblue', 'lightblue', 'green', 'green', 'blue', 'blue', 'orange',  'orange', 'orchid', 'orchid', 'purple', 'purple', 'red', 'red']
th = [0,       0.01,    0.01+dd,   0.125,   0.125+dd,    0.25,         0.25+dd,      0.375,      0.375+dd,   0.5,  0.5+dd, 0.625,  0.625+dd,  0.75,     0.75+dd, 0.875,    0.875+dd, 0.99,      1-dd, 1]


mycolors=list(zip(th, hc))
cm = colors.LinearSegmentedColormap.from_list('test', mycolors)

# As for a more fancy example, you can also give an axes by hand:
c_map_ax = figure.add_axes([0.19, 0.78, 0.62, 0.02])
c_map_ax.axes.get_xaxis().set_visible(False)
c_map_ax.axes.get_yaxis().set_visible(True)

# and create another colorbar with:
mpl.colorbar.ColorbarBase(c_map_ax, cmap=cm, orientation = 'horizontal')

figure.set_size_inches(8, 8)
figure.set_dpi(500)
plt.show()

plt.savefig(sys.argv[1] + '.20.png', bbox_inches='tight')