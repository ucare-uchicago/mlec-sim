import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib
import math
import pandas as pd
import sys
import numpy as np 

# plt.rcParams["font.family"] = "Times New Roman"
# label_font = {'fontname':'Times New Roman', 'fontsize':'20'}

title_font_size = 28
tick_font_size = 22

plt.rcParams["font.family"] = "Helvetica"
label_font = {'fontname':'Helvetica', 'fontsize':'20'}
annotation_font = {'fontname':'Helvetica', 'fontsize':title_font_size}



if len(sys.argv) - 1 < 6:
    print("python plot_xxx.py filepath [cc/cd/dc/dd] [k_n] [p_n] [k_l] [p_l]")
    exit(-1)

input_file_path = sys.argv[1]
placements = {'cc': 'C/C', 'cd': 'C/D', 'dc': 'D/C', 'dd': 'D/D'}
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
x_range = [0,61]
y_range = [0,70]
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

colorlist = ['darkgreen', '#00AF00', 'lime', 'yellow', 'lightgray', '#FFAF00', '#ff7200', 'red', 'maroon', 'black']
colorranges = [0,      0.0000001, 0.000001,    0.00001,    0.0001,  0.001,    0.01,     0.1,      0.99,    1]

def coloring(x):
    for i in range(len(colorranges)):
        if x <= colorranges[i]:
            return colorlist[i]

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
plt.title('({}+{})/({}+{}) MLEC {}'.format(k_n, p_n, k_l, p_l, placement), fontsize=title_font_size)
# plt.title(r'$\mathdefault{(18+2)/(18+2)}$ MLEC C/C\n', fontsize=24)

plt.xticks([0,10,20,30,40,50,60],fontsize=tick_font_size)
plt.yticks([0,10,20,30,40,50,60],fontsize=tick_font_size)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())


label_y = 65.5
plt.text(0.8, label_y, '0', **label_font)
plt.text(6.5, label_y, r'$\mathdefault{10^{-7}}$', **label_font)
plt.text(14, label_y, r'$\mathdefault{10^{-6}}$', **label_font)
plt.text(21.1, label_y, r'$\mathdefault{10^{-5}}$', **label_font)
plt.text(28.2, label_y, r'$\mathdefault{10^{-4}}$', **label_font)
plt.text(35.2, label_y, r'$\mathdefault{10^{-3}}$', **label_font)
plt.text(42.3, label_y, r'$\mathdefault{10^{-2}}$', **label_font)
plt.text(49.4, label_y, r'$\mathdefault{10^{-1}}$', **label_font)
plt.text(58, label_y, r'$\mathdefault{1}$', **label_font)

import matplotlib as mpl
import matplotlib.colors as colors

dd = 10**(-100)  # a number that is very close to 0

# hc = ['green', 'green', 'lightgreen', 'lightgreen','yellow', 'yellow', 'lightblue', 'lightblue', 'blue', 'blue', 'orange', 'orange', 'orchid',  'orchid', 'brown', 'brown', 'purple', 'purple', 'red', 'red']
# th = [0,       0.01,    0.01+dd,   0.125,   0.125+dd,    0.25,         0.25+dd,      0.375,      0.375+dd,   0.5,  0.5+dd, 0.625,  0.625+dd,  0.75,     0.75+dd, 0.875,    0.875+dd, 0.99,      0.99+dd, 1]
hc = []
th = []
for i in range(len(colorranges)):
    hc.append(colorlist[i])
    hc.append(colorlist[i])

num_blocks = len(colorranges) - 2
th.append(0)
th.append(0.01)
for i in range(num_blocks):
    block_left = (i)/num_blocks*0.98+0.01
    block_right = (i+1)/num_blocks*0.98+0.01
    th.append(block_left+dd)
    th.append(block_right)
th.append(0.99+dd)
th.append(1)
# print(th)


# annotations
anotation_color = 'blue'


if placement == 'C/C':
    plt.text(18, 5, 'Finding #3', **annotation_font, color=anotation_color)
    plt.arrow(25,10,-23,20,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')
    plt.arrow(25,10,5,22,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')

    # plt.text(26.5, 22.5, 'Finding #4', **annotation_font, color=anotation_color)
    # plt.arrow(33,26,-29,32,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')

if placement == 'C/D':
    axes.add_patch(patches.Rectangle((2.5, 2.5), 2, 58, linewidth=3, edgecolor=anotation_color, facecolor='none'))
    plt.text(15, 6, 'Finding #1', **annotation_font, color=anotation_color)
    plt.arrow(20,10,-15,10,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')

    plt.text(36.5, 22.5, 'Finding #5', **annotation_font, color=anotation_color)
    plt.arrow(43,26,-32,27,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')
    

if placement == 'D/C':
    axes.add_patch(patches.Rectangle((0.5, 48.5), 48, 2, linewidth=3, edgecolor=anotation_color, facecolor='none'))
    plt.text(37.5, 22.5, 'Finding #2', **annotation_font, color=anotation_color)
    plt.arrow(44,26,-18,22,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')
    plt.text(15, 6, 'Finding #6', **annotation_font, color=anotation_color)
    plt.arrow(22,11,-18,43,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')


if placement == 'D/D':
    plt.text(14, 8, 'Finding #4', **annotation_font, color=anotation_color)
    plt.arrow(21.5,13,-18,46,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')
    plt.text(35, 26, 'Finding #7', **annotation_font, color=anotation_color)
    plt.arrow(43,31,-22,22,head_width=1, head_length=2, linewidth=3, color=anotation_color,length_includes_head=True, joinstyle='miter')


mycolors=list(zip(th, hc))
cm = colors.LinearSegmentedColormap.from_list('test', mycolors)

# As for a more fancy example, you can also give an axes by hand:
c_map_ax = figure.add_axes([0.142, 0.78, 0.74, 0.015])
c_map_ax.axes.get_xaxis().set_visible(False)
c_map_ax.axes.get_yaxis().set_visible(True)

# and create another colorbar with:
mpl.colorbar.ColorbarBase(c_map_ax, cmap=cm, orientation = 'horizontal')

figure.set_size_inches(6.5, 8)
figure.set_dpi(100)

plt.savefig(sys.argv[1] + '.eps',  bbox_inches='tight')