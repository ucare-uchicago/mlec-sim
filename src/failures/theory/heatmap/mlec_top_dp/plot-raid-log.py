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
x_range = [0,30]
y_range = [0,30]
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
    elif x <=0.000001:
        return 'yellow'
    elif x <=0.00001:
        return 'lightgreen'
    elif x <= 0.0001:
        return 'lightblue'
    elif x <= 0.001:
        return 'green'
    elif x <= 0.01:
        return 'blue'
    elif x <= 0.1:
        return 'orange'
    elif x <= 0.9999:
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
    axes.add_patch(patches.Rectangle((racks-0.5, disks-0.5), 1, 1, linewidth=0.5, edgecolor='black', facecolor=dl_color))



plt.plot(x_range, y_range, linewidth=0.4, color='green')


plt.legend()
plt.xlabel('Number of racks affected', fontsize=14)
plt.ylabel('Number of drives affected', fontsize=14)
# plt.title('Frequency of failure bursts sorted by racks and drives affected')
plt.title(occuranceDataLoss.iloc[1]['config'] + ' Declustered-Clustered MLEC\n'+ r"$\bf{Theoretical}$", fontsize=16)

plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

plt.text(1, 28, 'PDL: 0.0')
plt.text(5.5, 28, r'$10^{-6}$')
plt.text(9, 28, r'$10^{-5}$')
plt.text(12.5, 28, r'$10^{-4}$')
plt.text(16, 28, r'$10^{-3}$')
plt.text(19, 28, r'$10^{-2}$')
plt.text(22.5, 28, r'$10^{-1}$')
plt.text(26, 28, r'$1$')

import matplotlib as mpl
import matplotlib.colors as colors

dd = 10**(-16)  # a number that is very close to 0
hc = ['white', 'white', 'yellow', 'yellow','lightgreen', 'lightgreen', 'lightblue', 'lightblue', 'green', 'green', 'blue', 'blue', 'orange',  'orange', 'purple', 'purple',  'red', 'red']
th = [0,       0.01,  0.01+dd, 0.15,  0.15+dd,  0.29, 0.29+dd,  0.43,      0.43+dd,   0.57,      0.57+dd,   0.71, 0.71+dd, 0.85, 0.85+dd, 0.99, 0.99+dd, 1]


mycolors=list(zip(th, hc))
cm = colors.LinearSegmentedColormap.from_list('test', mycolors)

# As for a more fancy example, you can also give an axes by hand:
c_map_ax = figure.add_axes([0.2, 0.8, 0.6, 0.02])
c_map_ax.axes.get_xaxis().set_visible(False)
c_map_ax.axes.get_yaxis().set_visible(True)

# and create another colorbar with:
mpl.colorbar.ColorbarBase(c_map_ax, cmap=cm, orientation = 'horizontal')

figure.set_size_inches(8, 8)
figure.set_dpi(500)
plt.show()

plt.savefig(sys.argv[1] + '.log.png')