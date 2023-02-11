import numpy as np
import math
import time

from failure_generator import FailureGenerator, Weibull
from simulators.Simulator import Simulator
from constants.SimulationResult import SimulationResult
from constants.time import YEAR
from system import System
from metrics import Metrics
from helpers.weibullNines import calculate_weibull_nines

class WeibullSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.weibull_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    def weibull_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        # logging.basicConfig(level=logging.INFO)
        
        for beta in np.arange(1, 2.2, 0.2):
            mission = YEAR
            t_l = 5  # 5 years
            alpha = t_l / ((- t_l * math.log((1-afr/100))) ** (1/beta))  # in years

            print("{} {} {} {} {} {} {}".format(afr/100,beta,cap,io_speed,drives_per_rack,k_local,p_local))
            nines_from_calculator = calculate_weibull_nines(afr=afr/100, beta=beta, 
                                                            disk_cap=cap, io=io_speed,
                                                            n=drives_per_rack, k=k_local, c=p_local)
            rack_num = total_drives / drives_per_rack
            nines_from_calculator -= math.log10(rack_num)
            print("nines_from_calculator: {}".format(nines_from_calculator))

            distribution = Weibull(alpha, beta)
            print("alpha: {}  beta: {}".format(alpha, beta))

            failureGenerator = FailureGenerator(afr, distribution)
            
            sys = System(
                num_disks=total_drives, 
                num_disks_per_rack=drives_per_rack, 
                k=k_local, 
                m=p_local, 
                place_type=placement, 
                diskCap=cap * 1024 * 1024,
                rebuildRate=io_speed, 
                intrarack_speed=intrarack_speed, 
                interrack_speed=interrack_speed, 
                utilizeRatio=1, 
                top_k=k_net, 
                top_m=p_net, 
                adapt=adapt, 
                rack_fail=0)

            res = [0, 0, Metrics()]

            # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
            # return
            while res[0] < 20:
                start  = time.time()
                temp = self.run(failureGenerator, sys, iters=50000, epochs=200, concur=200, mission=mission)
                res[0] += temp[0]
                res[1] += temp[1]
                res[2] += temp[2]
                print(res[2])
                simulationTime = time.time() - start
                print("simulation time: {}".format(simulationTime))
                print(res)
            # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
            print('++++++++++++++++++++++++++++++++')
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))

            res[1] *= mission/YEAR
            

            if res[0] == 0:
                print("NO FAILURE!")
                return SimulationResult(0, res[1], res[2])
            else:            
                return SimulationResult(res[0], res[1], res[2])