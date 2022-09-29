import matplotlib.pyplot as plt
import matplotlib
import math
import pandas as pd
import sys

googleOccurances = pd.read_csv(sys.argv[1])
# prob[i,j] means the probability 


figure, axes = plt.subplots()

axes.set_aspect( 1 )
x_range = [0.8,500]
y_range = [0.8,500]
axes.set_xlim(x_range)
axes.set_ylim(y_range)


def cal_radius(count):
  # slope = 11.8
  slope = 14.5
  # intercept = 2.4
  intercept = 3
  return slope * math.log10(count) + intercept

for index, row in googleOccurances.iterrows():
  # print(row)
  racks = row['num of racks affected']
  nodes = row['num of nodes affected']
  count = row['Occurance']
  if count > 0:
    radius = cal_radius(count)
    axes.plot(racks,nodes,1,marker='o',ms=radius,mfc='None',mec='black')


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
plt.xlabel('Number of racks affected')
plt.xscale("log")
plt.ylabel('Number of nodes affected')
plt.yscale("log")
plt.title('Frequency of failure bursts sorted by racks and nodes')
axes.set_xticks([1,2,5,10,20,50,100,200,500])
axes.set_yticks([1,2,5,10,20,50,100,200,500])
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

figure.set_size_inches(8, 8)
figure.set_dpi(120)
plt.savefig(sys.argv[1]+'.png')