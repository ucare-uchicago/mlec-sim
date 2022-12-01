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

class MetricSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.metric_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    # --------------------------------
    # Get metrics
    # No need to get enough failures
    # Just run enough simulations so that we get the average metrics
    # --------------------------------
    def metric_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        place_type = parse_placement(placement)

        for afr in range(2, 6):
            mission = YEAR
            failureGenerator = FailureGenerator(afr)
            
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

            res = [0, 0, Metrics()]

            start  = time.time()
            temp = temp = self.run(failureGenerator, sys, iters=50000, epochs=200, concur=200, mission=mission)
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
                # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))

            output = open("s-metric-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {}\n".format(
                k_local, p_local, k_net, p_net, total_drives,
                afr, cap, io_speed, res[0], res[1], "adapt" if adapt else "notadapt",
                res[2].getAverageRebuildIO(), res[2].getAverageNetTraffic(), res[2].getAvgNetRepairTime()))
            output.close()