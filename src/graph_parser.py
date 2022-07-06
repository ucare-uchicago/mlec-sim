import os
import re
import pandas as pd
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

# DIRS = [("RAID", "./164model-D164-P0"), ("DP", "./164model-D164-P1"), ("SODP", "./164model-D164-P2"), ("dRAID", "./164model-D164-P3")]
DIRS = [("RAID", "./164model-D164-P0"), ("DP", "./164model-D164-P1"), ("dRAID", "./164model-D164-P3"), ("G-SODP (80% FR, 1hr)", "./164model-D164-P2"), ("G-SODP (1% FR, 24hr)", "./164model-D164-P2-1")]

if __name__ == "__main__":

    # This stores all the series on the graph
    series = []

    for dir in DIRS:
        # This stores all the data point for a single series (placement type)
        prob_tuple = []

        for file_name in os.listdir(dir[1]):
            regex = re.search("^.*-D(.*)-N(.*)\.txt", file_name);

            full_path = dir[1] + "/" + file_name
            num_disks = regex.group(2)

            f = open(full_path, 'r')
            
            
            for line in f:
                if (line[0:1] == '-'):
                    regex = re.search("^.*, (.*)", line)
                    #print "num_disk: " + str(num_disks) + " prob: " + regex.group(1)
                    prob_tuple.append((int(num_disks), float(regex.group(1))))


        prob_tuple.sort(key=lambda y: y[0])
        df = pd.DataFrame(prob_tuple, columns=['num_disks', 'prob'])
        series.append((dir[0], df))
        
    fig,ax = plt.subplots()

    for serie in series:
        
        name = serie[0]
        df = serie[1]

        print("Plotting " + str(name))

        df.plot(x="num_disks", y="prob", kind="line", label=name, ax=ax)
    
    plt.xlabel("Number of Disks")
    plt.ylabel("Probability of failure")
    plt.savefig('fig.png')