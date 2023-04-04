from parse import parse_calc_result, parse_sim_result, setup_plot
import matplotlib.pyplot as plt
import numpy as np

stripes = [5, 6, 7, 8]
sim_path = "src/logs/netdp-validation/30diskio_diff_stripe/s-result-DP_NET_{}.log"

if __name__ == "__main__":
    setup_plot(plt, (0, 13), (0, 7))
    
    for net_k in stripes:    
        sim_result = parse_sim_result(sim_path.format(net_k))
        # print(sim_result)
        print(sim_path.format(net_k))
        plt.errorbar(sim_result['afr'], sim_result['nines'], yerr=sim_result['sigma'], label="({}+1)".format(net_k))

    plt.title("NetDP Different Stripe Width")
        
    plt.legend(loc="upper right")

    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")

    plt.grid()
    plt.savefig("jiajunm/netdp_diff_stripe_width.png")