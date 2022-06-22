from cProfile import label
from functools import partial
import matplotlib.pyplot as plt
import re

def plot(path='s-result-RAID.log', color=[1]):
    f = open(path, 'r')
    
    afrs = []
    nns = []

    groups = re.search("^(.*)-result-(.*).log", path)
    mode = groups.group(2)

    for row in f:
        groups = re.search("^(.*)-(.*)-(.*): (.*)", row)
        
        data_shards = int(groups.group(1))
        parity_shards = int(groups.group(2))
        afr = int(groups.group(3))
        nn = float(groups.group(4))
        print("{}-{}-{}: {}".format(data_shards, parity_shards, afr, nn))

        afrs.append(afr)
        nns.append(nn)
    
    if "s-" in path:
        plt.scatter(x=afrs, y=nns, marker='o', label="({}+{}){} sim".format(data_shards, parity_shards, mode), color=color)
    else:
        plt.scatter(x=afrs, y=nns, marker='D', label="({}+{}){} calc".format(data_shards, parity_shards, mode), edgecolors=color, facecolors='none')
    
    f.close()

def plot_title(path='s-result-RAID.log'):
    f = open(path, 'r')
    for row in f:
        groups = re.search("^(.*)-(.*)-(.*): (.*)", row)
        
        data_shards = int(groups.group(1))
        parity_shards = int(groups.group(2))
    
        plt.title("({}+{}) EC Config between RAID and DP".format(data_shards, parity_shards))

def plot_axis_name():
    plt.xlabel("Disk AFR in Percentage")
    plt.ylabel("Durability in # of Nines")

if __name__ == "__main__":
    plot('s-result-RAID.log', ['red'])
    plot('c-result-RAID.log', ['red'])
    plot('s-result-DP.log', ['green'])
    plot('c-result-DP.log', ['green'])

    plot_title()
    plot_axis_name()

    plt.legend(loc='upper right')
    plt.savefig('fig.png')