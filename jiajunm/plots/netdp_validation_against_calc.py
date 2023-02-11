from parse import parse_calc_result, parse_sim_result, setup_plot
import matplotlib.pyplot as plt
import numpy as np

calc_path = "src/logs/netdp-validation/c-result-DP_NET.log"
sim_path = "src/logs/netdp-validation/s-result-DP_NET.log"

if __name__ == "__main__":
    sim_result = parse_sim_result(sim_path)
    calc_result = parse_calc_result(calc_path)
    
    setup_plot(plt, (0, 13), (3, 8))
    
    plt.errorbar(sim_result['afr'], sim_result['nines'], yerr=sim_result['sigma'], label="NetDP Sim")
    plt.plot(calc_result['afr'], calc_result['nines'], label="Web Calc")
    
    plt.title("NetDP Simulator vs Calculator")
    
    plt.legend(loc="upper right")
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.grid()
    plt.savefig("netdp_validation.png")