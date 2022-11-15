from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt

if __name__ == "__main__":
    new_sim_result = parse_sim_result("src/s-result-RAID_new.log")
    sim_result = parse_sim_result("src/s-result-RAID.log")
    
    plt.errorbar(new_sim_result['afr'], new_sim_result['nines'], yerr=new_sim_result['sigma'], label="New Sim RAID")
    plt.errorbar(sim_result['afr'], sim_result['nines'], yerr=sim_result['sigma'], label="Now Sim")
    # plt.errorbar(c_afr, c_nines, yerr=c_sigmas, label="DP Sim")
    
    plt.legend(loc="upper right")
    
    plt.ylim((0, 7))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Restructured sim DP")
    # plt.show()
    plt.savefig('jiajunm/plt.png')