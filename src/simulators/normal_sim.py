import numpy as np
import logging
import math
import time

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.SimulationResult import SimulationResult
from constants.time import YEAR
from constants.constants import kilo

from system import System
from metrics import Metrics
import json

class NormalSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                 total_drives, drives_per_rack, placement, distribution, concur, epoch, iters,
                 infinite_chunks=True, chunksize=128, spool_size=-1, repair_scheme=0, detection_time=0):
        return self.normal_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                               total_drives, drives_per_rack, placement, distribution, concur, epoch, iters,
                               infinite_chunks, chunksize, spool_size, repair_scheme, detection_time)

    # -----------------------------
    # normal Monte Carlo simulation
    # -----------------------------
    def normal_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters, infinite_chunks=True, chunksize=128,
                    spool_size=-1, repair_scheme=0, detection_time=0, 
                    num_local_fail_to_report=0, num_net_fail_to_report=0, prev_fail_reports_filename=None):
        # logging.basicConfig(level=logging.INFO, filename="run_"+placement+".log")

        mission = YEAR
        
        sys_kwargs = {
            "num_disks": total_drives, 
            "num_disks_per_rack": drives_per_rack, 
            "k": k_local, 
            "m": p_local, 
            "place_type": placement, 
            "diskCap": cap * kilo * kilo,
            "rebuildRate": io_speed, 
            "intrarack_speed": intrarack_speed, 
            "interrack_speed": interrack_speed, 
            "utilizeRatio": 1, 
            "top_k": k_net, 
            "top_m": p_net, 
            "adapt": adapt, 
            "rack_fail": 0,
            "infinite_chunks": infinite_chunks,
            "chunksize": chunksize,
            "spool_size": spool_size,
            "repair_scheme": repair_scheme,
            "collect_fail_reports": True,
            "detection_time": detection_time,
            "distribution": distribution,
            }

        failed_iters = 0
        total_iters = 0
        metrics = Metrics()
        fail_reports = []

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return

        # We need to get enough failures in order to compute accurate nines #
        while failed_iters < 10:
            logging.info(">>>>>>>>>>>>>>>>>>> simulation started >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
            start  = time.time()
            res = self.run(afr, iters=iters, epochs=epoch, concur=concur, mission=mission, prev_fail_reports_filename=None, **sys_kwargs)
            failed_iters += res[0]
            total_iters += res[1]
            metrics += res[2]
            fail_reports += res[3]
            # print(metrics)
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            # print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))
            # return None
            if distribution == "catas_local_failure":
                break
        
        if distribution == "catas_local_failure":
            print("avg_net_traffic: {}".format(metrics.getAverageNetTraffic()))
            print("avg_net_repair_time: {}".format(metrics.getAvgNetRepairTime()))
            print("avg_local_repair_time: {}".format(metrics.getAvgLocalRepairTime()))
            print(metrics.total_local_repair_count)
        else:
            # print(fail_reports)
            # print(metrics)
            with open('fail_reports.log', 'w') as fout:
                json.dump(fail_reports, fout)

            total_iters *= mission/YEAR

            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
            nines = str(round(-math.log10(failed_iters/total_iters),3))
            sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
            print("Num of Nine: " + nines)
            print("error sigma: " + sigma)

            # total_down_time = metrics.getAverageAggregateDownTime()
            # total_time = YEAR * total_drives
            # avail_nines = "NA" if total_down_time == 0 else str(round(-math.log10(total_down_time/total_time),3))
            # print("average aggregate down time: {}\navail_nines:{}".format(
            #             total_down_time, avail_nines))
            
        
        return SimulationResult(failed_iters, int(total_iters), metrics)