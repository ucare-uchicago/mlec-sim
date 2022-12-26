from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    bw = "4"
    comb = "20TB_30MBps"
    calc = parse_calc_result("src/logs/mlec-validation/{}/c-result-MLEC_{}.log".format(comb, bw))
    sim = parse_sim_result("src/logs/mlec-validation/{}/s-result-MLEC_{}.log".format(comb, bw))
    
    fig, ax = plt.subplots()
    ylim = (0, 8)
    
    y_major_ticks = list(range(ylim[0], ylim[1]))
    y_minor_ticks = np.array(list(range(ylim[0], ylim[1] * 2))) / 2
    print(y_major_ticks)
    print(y_minor_ticks)
    
    ax.set_yticks(y_major_ticks)
    ax.set_yticks(y_minor_ticks, minor=True)
    ax.grid(which="minor")
    
    plt.errorbar(sim['afr'], sim['nines'], yerr=sim['sigma'], label="Network Sim")
    plt.plot(calc['afr'], calc['nines'], label="Web Calc")
    
    plt.legend(loc="upper right")    
    
    # plt.axes().yaxis.set_minor_locator(y_minor_ticks)
    
    plt.ylim((0, 8))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("MLEC Validation Baseline ({} Gbps)".format(bw.replace("_", ".")))
    # plt.show()
    plt.grid()
    
    plt.savefig("jiajunm/plt.png")