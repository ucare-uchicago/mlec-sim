from parse import parse_calc_result, parse_sim_result
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    inter_2 = parse_sim_result("src/logs/raid_net_diff_stripewidth_150disk_100inter/3+2.log")
    inter_10 = parse_sim_result("src/logs/raid_net_diff_stripewidth_150disk_100inter/8+2.log")
    inter_20 = parse_sim_result("src/logs/raid_net_diff_stripewidth_150disk_100inter/18+2.log")
    inter_50 = parse_sim_result("src/logs/raid_net_diff_stripewidth_150disk_100inter/23+2.log")
    inter_100 = parse_sim_result("src/logs/raid_net_diff_stripewidth_150disk_100inter/48+2.log")

    fig, ax = plt.subplots()
    ylim = (0, 6)
    
    y_major_ticks = list(range(ylim[0], ylim[1]))
    y_minor_ticks = np.array(list(range(ylim[0], ylim[1] * 2))) / 2
    print(y_major_ticks)
    print(y_minor_ticks)
    
    ax.set_yticks(y_major_ticks)
    ax.set_yticks(y_minor_ticks, minor=True)
    ax.grid(which="minor")
    
    plt.errorbar(inter_2['afr'], inter_2['nines'], yerr=inter_2['sigma'], label="(3+2)")
    plt.errorbar(inter_10['afr'], inter_10['nines'], yerr=inter_10['sigma'], label="(8+2)")
    plt.errorbar(inter_20['afr'], inter_20['nines'], yerr=inter_20['sigma'], label="(18+2)")
    plt.errorbar(inter_50['afr'], inter_50['nines'], yerr=inter_50['sigma'], label="(23+2)")
    plt.errorbar(inter_100['afr'], inter_100['nines'], yerr=inter_100['sigma'], label="(48+2)")
    
    plt.legend(loc="upper right")    
    
    # plt.axes().yaxis.set_minor_locator(y_minor_ticks)
    
    plt.ylim((0, 6))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Net RAID with 100 Gbps Intra-rack, 100 Gbps Cross-rack")
    # plt.show()
    plt.grid()
    
    plt.savefig('jiajunm/plt.png')