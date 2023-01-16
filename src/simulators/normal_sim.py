import numpy as np
import logging
import math
import time

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.PlacementType import parse_placement, PlacementType
from constants.time import YEAR
from system import System
from metrics import Metrics

class NormalSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.normal_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)    

    # -----------------------------
    # normal Monte Carlo simulation
    # -----------------------------
    def normal_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        # logging.basicConfig(level=logging.INFO, filename="run_"+placement+".log")

        mission = YEAR
        failureGenerator = FailureGenerator(afr)

        place_type = parse_placement(placement)
        
        sys = System(
            num_disks=total_drives, 
            num_disks_per_rack=drives_per_rack, 
            k=k_local, 
            m=p_local, 
            place_type=place_type, 
            diskCap=cap * 1024 * 1024,
            rebuildRate=io_speed, 
            intrarack_speed=intrarack_speed, 
            interrack_speed=interrack_speed, 
            utilizeRatio=1, 
            top_k=k_net, 
            top_m=p_net, 
            adapt=adapt, 
            rack_fail=0)

        failed_iters = 0
        total_iters = 0
        metrics = Metrics()

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return

        # We need to get enough failures in order to compute accurate nines #
        while failed_iters < 20:
            logging.info(">>>>>>>>>>>>>>>>>>> simulation started >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
            start  = time.time()
            res = self.run(failureGenerator, sys, iters=iters, epochs=epoch, concur=concur, mission=mission)
            failed_iters += res[0]
            total_iters += res[1]
            metrics += res[2]
            # print(metrics)
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))

        total_iters *= mission/YEAR

        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        nines = str(round(-math.log10(failed_iters/total_iters),3))
        sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
        print("Num of Nine: " + nines)
        print("error sigma: " + sigma)

        total_down_time = metrics.getAverageAggregateDownTime()
        total_time = YEAR * total_drives
        avail_nines = str(round(-math.log10(total_down_time/total_time),3))
        print("average aggregate down time: {}\navail_nines:{}".format(
                    total_down_time, avail_nines))
        
        output = open("s-result-{}.log".format(placement), "a")
        output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            afr, cap, io_speed, nines, sigma, failed_iters, total_iters, "adapt" if adapt else "notadapt"))
        output.close()