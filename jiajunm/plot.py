from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt

if __name__ == "__main__":
    inter_10 = parse_sim_result("src/logs/s-result-RAID_NET_10_inter.log")
    inter_50 = parse_sim_result("src/logs/s-result-RAID_NET_50_inter.log")
    inter_100 = parse_sim_result("src/logs/s-result-RAID_NET_100_inter.log")
    inter_400 = parse_sim_result("src/logs/s-result-RAID_NET_400_inter.log")
    inter_inf = parse_sim_result("src/logs/s-result-RAID_NET_inf_inter.log")
    
    plt.errorbar(inter_10['afr'], inter_10['nines'], yerr=inter_10['sigma'], label="10 Gbps Interrack")
    plt.errorbar(inter_50['afr'], inter_50['nines'], yerr=inter_50['sigma'], label="50 Gbps Interrack")
    plt.errorbar(inter_100['afr'], inter_100['nines'], yerr=inter_100['sigma'], label="100 Gbps Interrack")
    plt.errorbar(inter_400['afr'], inter_400['nines'], yerr=inter_400['sigma'], label="400 Gbps Interrack")
    plt.errorbar(inter_inf['afr'], inter_inf['nines'], yerr=inter_inf['sigma'], label="inf Gbps Interrack")
    
    plt.legend(loc="upper right")
    
    plt.ylim((0, 4))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Net RAID with 100 Gbps Intrarack and Various Interrack Bandwidth")
    # plt.show()
    plt.savefig('jiajunm/plt.png')