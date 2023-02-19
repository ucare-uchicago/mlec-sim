import numpy as np
import logging
import math
import time

from failure_generator import FailureGenerator, GoogleBurst
from simulators.Simulator import Simulator
from constants.SimulationResult import SimulationResult
from constants.time import YEAR
from system import System
from metrics import Metrics

class BurstSim(Simulator):
    

    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters) -> SimulationResult:
        return self.burst_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    # -----------------------------
    # simulate against bursts
    # -----------------------------
    def burst_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters) -> SimulationResult:
        # logging.basicConfig(level=logging.INFO)
        
        mission = YEAR
        failureGenerator = FailureGenerator(afr, GoogleBurst(50, 50), is_burst=True)
        
        sys = System(
                num_disks=total_drives, 
                num_disks_per_rack=drives_per_rack, 
                k=k_local, 
                m=p_local, 
                place_type=placement, 
                diskCap=cap * kilo * kilo,
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

        # res = simulate(failureGenerator, sys, iters=100, epochs=1, concur=1, mission=mission)
        # return

        # We need to get enough failures in order to compute accurate nines #
        while failed_iters < 20:
            start  = time.time()
            res = self.run(failureGenerator, sys, iters=5000, epochs=200, concur=200, mission=mission)
            failed_iters += res[0]
            total_iters += res[1]
            metrics += res[2]
            # print(metrics)
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print("failed_iters: {}  failed_iters: {}".format(failed_iters, total_iters))

        total_iters *= mission/YEAR
    
        return SimulationResult(failed_iters, int(total_iters), metrics)
