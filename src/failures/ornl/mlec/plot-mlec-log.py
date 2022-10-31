import matplotlib.pyplot as plt
import matplotlib
import math
import pandas as pd
import sys
import numpy as np 

occuranceDataLoss = pd.read_csv(sys.argv[1], sep=' ')
# prob[i,j] means the probability 
print(occuranceDataLoss)

df = pd.read_csv ('../burst_node_enclosure.csv')
groupeddf = df.set_index(['enclosures', 'drives'])
print(groupeddf)

import matplotlib.pyplot as plt
import matplotlib
import math
figure, axes = plt.subplots()

axes.set_aspect( 1 )
x_range = [0.8,500]
y_range = [0.8,500]
axes.set_xlim(x_range)
axes.set_ylim(y_range)


def cal_radius(count):
    # slope = 11.8
    slope = 20
    # intercept = 2.4
    #   intercept = 4
    if count == 0:
        return 2.5
    intercept = 7
    return slope * math.log10(count) + intercept

def coloring(x):
    if x == 0:
        return 'lightgreen'
    elif x <= 0.0001:
        return 'green'
    elif x <= 0.001:
        return 'lightblue'
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
    enclosures = row['affected_racks']
    dl = float(row['dl_prob'])
    dl_color = coloring(dl)
    if (enclosures, disks) in groupeddf.index:
        count = groupeddf.loc[enclosures, disks]['count']
        for i in range(int(count)):
            survival_prob *= (1-dl)
    else:
        count = 0
    total_count += count
    radius = cal_radius(count)
    if count == 0:
        axes.plot(enclosures, disks,1,marker='o',ms=radius,mfc=dl_color,mec=dl_color, mew=0.1)
    else:
        axes.plot(enclosures, disks,1,marker='o',ms=radius,mfc=dl_color,mec='black', mew=0.6)
    
for index, row in occuranceDataLoss.iterrows():
    disks = row['failed_disks']
    enclosures = row['affected_racks']
    dl = float(row['dl_prob'])
    dl_color = coloring(dl)
    if (enclosures, disks) in groupeddf.index:
        count = groupeddf.loc[enclosures, disks]['count']
    else:
        count = 0
    single_burst_survival_prob += (1-dl) * count / total_count


if survival_prob == 1:
    survival_prob_nines = 100
else:
    survival_prob_nines = round(abs(math.log10(1-survival_prob)),1)

if single_burst_survival_prob == 1:
    single_burst_survival_prob_nines = 100
else:
    single_burst_survival_prob_nines = round(-math.log10(1-single_burst_survival_prob),1)

plt.plot(x_range, y_range, linewidth=0.4, color='green')

plt.text(100, 15, "1 occurances")
axes.plot(50, 15.5,1,marker='o',ms=cal_radius(1),mfc='None',mec='black')
plt.text(100, 8, "10 occurances")
axes.plot(50, 8.3,1,marker='o',ms=cal_radius(10),mfc='None',mec='black')
plt.text(100, 4, "100 occurances")
axes.plot(50, 4.15,1,marker='o',ms=cal_radius(100),mfc='None',mec='black')
plt.text(100, 1.5, "1000 occurances")
axes.plot(50, 1.55,marker='o',ms=cal_radius(1000),mfc='None',mec='black')


plt.legend()
plt.xlabel('Number of enclosures affected', fontsize=14)
plt.xscale("log")
plt.ylabel('Number of drives affected', fontsize=14)
plt.yscale("log")
# plt.title('Frequency of failure bursts sorted by racks and drives affected')
plt.title(occuranceDataLoss.iloc[1]['config'] + ' Clustered\nProbability to survive all ORNL bursts:{:.5f} Nines:{}\n'
                'Probability to survive a random burst:{:.5f} Nines:{}'.
                format(survival_prob, survival_prob_nines, 
                        single_burst_survival_prob, single_burst_survival_prob_nines), fontsize=16)
axes.set_xticks([1,2,5,10,20,50,100,200,500])
axes.set_yticks([1,2,5,10,20,50,100,200,500])
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

plt.text(1, 350, 'PDL: 0.0')
plt.text(3.5, 350, '0.0001')
plt.text(10, 350, '0.001')
plt.text(25, 350, '0.01')
plt.text(80, 350, '0.1')
plt.text(200, 350, '1.0')

import matplotlib as mpl
import matplotlib.colors as colors

dd = 10**(-16)  # a number that is very close to 0
hc = ['lightgreen', 'lightgreen','green', 'green', 'lightblue', 'lightblue', 'blue', 'blue', 'orange',  'orange', 'purple', 'purple',  'red', 'red']
th = [0,       0.01,  0.01+dd, 0.2,  0.2+dd,  0.4, 0.4+dd,  0.6,      0.6+dd,   0.8,      0.8+dd,       0.99-dd,         0.99, 1]

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