from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    old_sim = parse_calc_result("src/s-result-MLEC_old.log")
    calc = parse_calc_result("src/c-result-MLEC.log")
    sim = parse_sim_result("src/s-result-MLEC.log")
    
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
    plt.plot(old_sim['afr'], old_sim['nines'], label='Old Sim')
    
    plt.legend(loc="upper right")    
    
    # plt.axes().yaxis.set_minor_locator(y_minor_ticks)
    
    plt.ylim((0, 8))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("MLEC Validation Baseline (Inf Bandwidth)")
    # plt.show()
    plt.grid()
    
    plt.savefig("jiajunm/plt.png")