from cProfile import label
from parse import parse_sim_result, parse_calc_result
import matplotlib.pyplot as plt
import re

if __name__ == "__main__":
    # Read from the log
    calc_result = parse_calc_result("src/c-result-DP.log")
    twenty_racks = parse_sim_result("src/s-result-DP_NET_1.log")
    forty_racks = parse_sim_result("src/s-result-DP_NET_2.log")
    ten_racks = parse_sim_result("src/s-result-DP_NET_3.log")
    
    # plt.plot(c_afr, c_nines, label="DP", color='blue')
    # plt.plot(afr, nines, label='DP_NET')
    
    print(calc_result['afr'])
    print(calc_result['nines'])
    # plt.plot(calc_result['afr'], calc_result['nines'], label="Web c")
    
    plt.errorbar(ten_racks['afr'], ten_racks['nines'], yerr=ten_racks['sigma'], label="DP Net 10 racks")
    plt.errorbar(twenty_racks['afr'], twenty_racks['nines'], yerr=twenty_racks['sigma'], label="DP Net 20 racks")
    plt.errorbar(forty_racks['afr'], forty_racks['nines'], yerr=forty_racks['sigma'], label="DP Net 40 rakcs")
    # plt.errorbar(c_afr, c_nines, yerr=c_sigmas, label="DP Sim")
    
    plt.legend(loc="upper right")
    
    plt.ylim((0, 7))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Network DP Simulator vs Calculator")
    plt.show()
    plt.savefig('plt.png')