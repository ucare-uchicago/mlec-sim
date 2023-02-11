from parse import parse_calc_result, parse_sim_result, setup_plot
import matplotlib.pyplot as plt
import numpy as np


if __name__ == "__main__":
    result = {}
    dataset = "150MB_diskIO"
    to_parse = ["0_1", "0_5", "1", "2", "4", "inf"]
    
    for bw in to_parse:
        result[bw] = parse_sim_result("src/logs/mlec-validation/{}/s-result-MLEC_{}.log".format(dataset, bw))
    
    setup_plot(plt, (0, 16), (0, 9))
    
    for bw in to_parse:
        plt.errorbar(result[bw]['afr'], result[bw]['nines'], yerr=result[bw]['sigma'], label=bw.replace("_", ".") + " Gbps")
    
    plt.legend(loc="upper right")
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("MLEC Various Cross-Rack Network (150MBps Disk IO)")
    # plt.show()
    plt.grid()
    
    plt.savefig("jiajunm/plt.png")
    
