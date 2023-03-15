import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import math

title_font_size = 24
tick_font_size = 18

plt.rcParams["font.family"] = "Helvetica"
label_font = {'fontname':'Helvetica', 'fontsize':'20'}
annotation_font = {'fontname':'Helvetica', 'fontsize':title_font_size}


# prob[i,j] means the probability 
df = pd.read_csv ('Alpine_disk_failure_events_final.csv')

df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True).dt.tz_localize(None)

sorted_df = df.sort_values(by='Timestamp')
pd.set_option('display.max_columns', None)
sorted_df.to_string(index=False)

from datetime import datetime, timedelta
current_time = None
prev_time = pd.to_datetime('1900-01-01 00:00:00')

window = timedelta(seconds=900) # 15min

intervals = []

node_bursts = []
burst = {'nodes':[], 'racks':[]}
first_row = True
for index, row in sorted_df.iterrows():
  current_time = row['Timestamp']
  interval = (current_time - prev_time)
  # print("current time: {}, prev time: {}, interval: {}".format(
  #     current_time, prev_time, interval
  # ))
  intervals.append(interval)
  prev_time = current_time
  # print(interval)

  node = str(row['Rack']) + '-' + str(row['Enclosure'])
  rack = row['Rack']

  if interval < window:
    burst['nodes'].append(node)
    burst['racks'].append(rack)
    # print(interval)
  else:
    if (len(burst['nodes']) >= 10):
      print(row)
    if first_row:
      burst['nodes'].append(node)
      burst['racks'].append(rack)
      first_row = False
    else:
      node_bursts.append(burst)
      burst = {'nodes':[], 'racks':[]}
      burst['nodes'].append(node)
      burst['racks'].append(rack)

node_bursts.append(burst)

intervals.sort()

burst_counts_by_node_rack = {}
for bur in node_bursts:
  node_count = len(bur['nodes'])
  rack_count = len(list(dict.fromkeys(bur['racks'])))
  key = (rack_count, node_count)
  if key in burst_counts_by_node_rack:
    burst_counts_by_node_rack[key] += 1
  else:
    burst_counts_by_node_rack[key] = 1


print(burst_counts_by_node_rack)


with open('burst_node_rack.csv', 'w') as the_file:
  the_file.write('racks,drives,count\n')
  for burst in burst_counts_by_node_rack:
    count = burst_counts_by_node_rack[burst]
    the_file.write('{},{},{}\n'.format(burst[0], burst[1], count))




figure, axes = plt.subplots()

axes.set_aspect( 1 )
x_range = [0.8,200]
y_range = [0.8,200]
axes.set_xlim(x_range)
axes.set_ylim(y_range)


def cal_radius(count):
  # slope = 11.8
  slope = 20
  # intercept = 2.4
  intercept = 4
  return slope * math.log10(count) + intercept

for burst in burst_counts_by_node_rack:
  count = burst_counts_by_node_rack[burst]
  radius = cal_radius(count)
  axes.plot(burst[0], burst[1],1,marker='o',ms=radius,mfc='None',mec='black')


plt.plot(x_range, y_range, linewidth=0.4, color='green')

plt.text(100, 15, "1 occurances", **label_font)
axes.plot(50, 15.5,1,marker='o',ms=cal_radius(1),mfc='None',mec='black')
plt.text(100, 8, "10 occurances", **label_font)
axes.plot(50, 8.3,1,marker='o',ms=cal_radius(10),mfc='None',mec='black')
plt.text(100, 4, "100 occurances", **label_font)
axes.plot(50, 4.15,1,marker='o',ms=cal_radius(100),mfc='None',mec='black')
plt.text(100, 1.5, "1000 occurances", **label_font)
axes.plot(50, 1.55,marker='o',ms=cal_radius(1000),mfc='None',mec='black')


# plt.legend()
plt.xlabel('Number of racks affected', fontsize=title_font_size)
plt.xscale("log")
plt.ylabel('Number of drives affected', fontsize=title_font_size)
plt.yscale("log")
plt.title('Frequency of failure bursts sorted by racks and drives affected', fontsize=title_font_size)
plt.xticks([1,2,5,10,20,50,100,200], fontsize=tick_font_size)
plt.yticks([1,2,5,10,20,50,100,200], fontsize=tick_font_size)
axes.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

figure.set_size_inches(8, 8)
figure.set_dpi(100)
plt.show()

plt.savefig('ornl_bursts_rack.png')