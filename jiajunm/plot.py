from cProfile import label
import matplotlib.pyplot as plt
import re

if __name__ == "__main__":
    # Read from the log
    path = "src/s-result-DP_NET.log"
    c_path = "src/s-result-DP.log"
    calc_path = "src/c-result-DP.log"
    
    k_net = []
    p_net = []
    k_local = []
    p_local = []
    nines = []
    sigmas = []
    afr = []
    
    for row in open(path, 'r'):
        matcher = re.findall(r'\((.*)\+(.*)\)\((.*)\+(.*)\) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*)', row)
        
        (k_net_, p_net_, k_local_, p_local_, total_drives_, afr_, cap_, io_speed_, nines_, sigma_, failed_iters_, total_iters_, adapt_) = matcher[0]
        
        # NET_DP uses local shard arguments
        k_net.append(int(k_net_))
        k_local.append(int(k_local_))
        p_net.append(int(p_net_))
        p_local.append(int(p_local_))
        nines.append(float(nines_))
        sigmas.append(float(sigma_))
        afr.append(float(afr_))
    
    c_afr = []
    c_nines = []
    c_sigmas = []
        
    for row in open(c_path, 'r'):
        matcher = re.findall(r'\((.*)\+(.*)\)\((.*)\+(.*)\) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*)', row)
        
        (k_net_, p_net_, k_local_, p_local_, total_drives_, afr_, cap_, io_speed_, nines_, sigma_, failed_iters_, total_iters_, adapt_) = matcher[0]
        
        # NET_DP uses local shard arguments
        c_nines.append(float(nines_))
        c_afr.append(float(afr_))
        c_sigmas.append(float(sigma_))


    calc_afr = []
    calc_nines = []
    
    for row in open(calc_path, 'r'):
        matcher = re.findall(r'(.*) (.*)', row)
        (calc_afr_, calc_nines_) = matcher[0]
        calc_afr.append(float(calc_afr_))
        calc_nines.append(float(calc_nines_))
    
    # plt.plot(c_afr, c_nines, label="DP", color='blue')
    # plt.plot(afr, nines, label='DP_NET')
    
    plt.plot(calc_afr, calc_nines, label="Web Calc")
    
    plt.errorbar(afr, nines, yerr=sigmas, label="DP_NET Sim")
    plt.errorbar(c_afr, c_nines, yerr=c_sigmas, label="DP Sim")
    
    plt.legend(loc="upper right")
    
    plt.ylim((0, 7))
    plt.xlim((0,13))
    
    plt.xlabel("Annual Failure Rate in %")
    plt.ylabel("Durability (Num of Nines)")
    
    plt.title("Network DP Simulator vs Calculator")
    plt.show()
    plt.savefig('plt.png')