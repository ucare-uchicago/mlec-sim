import math

def calculate_weibull_nines(afr, beta, disk_cap, io, n, k, c):    
    t_l = 5
    alpha = t_l / ((-t_l*math.log((1-afr))) ** (1/beta))
    print(math.log(0.99))
    print("alpha: {}".format(alpha))


    mission_time = 1
    repair_rate = 1 / (disk_cap * 1024 * 1024 / io / 3600 / 24 / 365.25)
    # repair_rate = 1/1.5*365.25

    # print("repair rate: {}".format(repair_rate))

    beta_sys = (c+1) * (beta-1) + 1
    # print("beta_sys: {}".format(beta_sys))
    alpha_sys = ((math.factorial(n) * (beta**(c+1))) / 
                    ((repair_rate**c) * (alpha**((c+1)*beta)) * math.factorial(n-c-1) * beta_sys)
                    ) ** (-1/beta_sys)
    # print("alpha_sys: {}".format(alpha_sys))
    mttdl = alpha_sys *  math.gamma(1 + 1/beta_sys)
    reliability = math.exp(- (mission_time/alpha_sys) ** beta_sys)
    # print("reliability: {}".format(reliability))
    nines = round(-math.log10(1-reliability),3)
    # print("mttdl: {}".format(mttdl))
    # print(nines)
    return nines

# x = calculate_weibull_nines(0.01, 1, 20, 50, 10, 8, 2)
# print(x)