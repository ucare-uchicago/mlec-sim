from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    gbps = [2, 10, 20, 50, 100, 400]
    stripe = '8+2'
    
    result = {}
    for gbps_ in gbps:
        path = "src/logs/" + stripe + "/" + str(gbps_) + "gbps.log"
        print(path)
        result[gbps_] = parse_sim_result(path)

    fig, ax = plt.subplots()
    ylim = (0, 6)
    
    y_major_ticks = list(range(ylim[0], ylim[1]))
    y_minor_ticks = np.array(list(range(ylim[0], ylim[1] * 2))) / 2
    print(y_major_ticks)
    print(y_minor_ticks)
    
    ax.set_yticks(y_major_ticks)
    ax.set_yticks(y_minor_ticks, minor=True)
    ax.grid(which="minor")
    
    for gbps_ in gbps:
        plt.errorbar(result[gbps_]['afr'], result[gbps_]['nines'], yerr=result[gbps_]['sigma'], label=(str(gbps_) + " Gbps Cross-Rack"))
    
    plt.legend(loc="upper right")    
    
    # plt.axes().yaxis.set_minor_locator(y_minor_ticks)
    
    plt.ylim((0, 6))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title(stripe + " Net RAID with 100 Gbps Intra-rack various Cross-rack")
    # plt.show()
    plt.grid()
    
    plt.savefig('jiajunm/plt.png')