from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt
import numpy as np


if __name__ == "__main__":
    result = {}
    to_parse = ["0_1", "0_5", "1", "2", "4"]
    
    for bw in to_parse:
        result[bw] = parse_calc_result("src/logs/mlec-validation/20TB_30MBps_infIBW/c-result-MLEC_{}.log".format(bw))
        
    fig, ax = plt.subplots()
    ylim = (0, 7)
    
    y_major_ticks = list(range(ylim[0], ylim[1]))
    y_minor_ticks = np.array(list(range(ylim[0], ylim[1] * 2))) / 2

    ax.set_yticks(y_major_ticks)
    ax.set_yticks(y_minor_ticks, minor=True)
    ax.grid(which="minor")
    
    for bw in to_parse:
        plt.plot(result[bw]['afr'], result[bw]['nines'], label=bw.replace("_", ".") + " Gbps")
    
    plt.legend(loc="upper right")
    plt.ylim(ylim)
    plt.xlim((0,16))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("MLEC Various Network Constraint")
    # plt.show()
    plt.grid()
    
    plt.savefig("jiajunm/plt.png")
    
