from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt

if __name__ == "__main__":
    new_sim_result = parse_sim_result("src/logs/s-result-RAID_NET_new.log")
    sim_result = parse_sim_result("src/logs/s-result-RAID_NET.log")
    
    plt.errorbar(new_sim_result['afr'], new_sim_result['nines'], yerr=new_sim_result['sigma'], label="Net RAID w/ Inf Network Bandwidth")
    plt.errorbar(sim_result['afr'], sim_result['nines'], yerr=sim_result['sigma'], label="Old Sim")
    # plt.errorbar(c_afr, c_nines, yerr=c_sigmas, label="DP Sim")
    
    plt.legend(loc="upper right")
    
    plt.ylim((0, 1))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Net RAID with inf Network Bandwidth Constraint")
    # plt.show()
    plt.savefig('jiajunm/plt.png')