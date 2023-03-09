import numpy as np
import logging
import math
import time
import json

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.SimulationResult import SimulationResult
from constants.time import YEAR
from constants.constants import kilo

from system import System
from metrics import Metrics

class ManualFailSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                 total_drives, drives_per_rack, placement, distribution, concur, epoch, iters,
                 infinite_chunks=True, chunksize=128, spool_size=-1, repair_scheme=0):
        return self.manual_fail_sim(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                               total_drives, drives_per_rack, placement, distribution, concur, epoch, iters,
                               infinite_chunks, chunksize, spool_size, repair_scheme)    

    # -----------------------------
    # simulation based on manual failure injection
    # -----------------------------
    def manual_fail_sim(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters, infinite_chunks=True, chunksize=128,
                    spool_size=-1, repair_scheme=0):
        # logging.basicConfig(level=logging.INFO, filename="run_"+placement+".log")

        mission = YEAR
        failureGenerator = FailureGenerator(afr, failures_store_len=total_drives*100)

        num_local_fail_to_report_list = [3,4,5]
        
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
            rack_fail=0,
            infinite_chunks=infinite_chunks,
            chunksize=chunksize,
            spool_size=spool_size,
            repair_scheme=repair_scheme,
            collect_fail_reports=True,
            num_local_fail_to_report=num_local_fail_to_report_list[0])

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
            res = self.run(failureGenerator, sys, iters=iters, epochs=epoch, concur=concur, mission=mission)
            failed_iters += res[0]
            total_iters += res[1]
            metrics += res[2]
            fail_reports += res[3]
            # print(metrics)
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))
            # return None
        
        # print(fail_reports)

        total_iters *= mission/YEAR

        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        prob = failed_iters/total_iters
        nines = str(round(-math.log10(failed_iters/total_iters),3))
        sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
        print("Num of Nine: " + nines)
        print("error sigma: " + sigma)

        # -----
        # round 2

        failed_iters = 0
        total_iters = 0
        metrics = Metrics()
        new_fail_reports = []

        sys.num_local_fail_to_report = num_local_fail_to_report_list[1]
        fail_reports_filename = 'fail_reports.log'
        with open(fail_reports_filename, 'w') as fout:
            json.dump(fail_reports, fout)
        fail_reports = None
        

        # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # return

        # We need to get enough failures in order to compute accurate nines #
        while failed_iters < 10:
            logging.info(">>>>>>>>>>>>>>>>>>> simulation started >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
            start  = time.time()
            res = self.run(failureGenerator, sys, iters=iters, epochs=epoch, concur=concur, mission=mission, prev_fail_reports_filename=fail_reports_filename)
            failed_iters += res[0]
            total_iters += res[1]
            metrics += res[2]
            new_fail_reports += res[3]
            # print(metrics)
            simulationTime = time.time() - start
            print("simulation time: {}".format(simulationTime))
            print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))

        total_iters *= mission/YEAR

        print(metrics.count)

        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        prob = failed_iters/total_iters
        nines = str(round(-math.log10(failed_iters/total_iters),3))
        sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
        print("Num of Nine: " + nines)
        print("error sigma: " + sigma)


        # # -----
        # # round 3

        # failed_iters = 0
        # total_iters = 0
        # metrics = Metrics()
        # new_fail_reports = []

        # sys.num_local_fail_to_report = num_local_fail_to_report_list[2]
        # fail_reports_filename = 'fail_reports.log'
        # with open(fail_reports_filename, 'w') as fout:
        #     json.dump(fail_reports, fout)
        # fail_reports = None
        

        # # temp = simulate(sys_state1, iters=10000, epochs=1, concur=1, mission=mission)
        # # return

        # # We need to get enough failures in order to compute accurate nines #
        # while failed_iters < 10:
        #     logging.info(">>>>>>>>>>>>>>>>>>> simulation started >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
        #     start  = time.time()
        #     res = self.run(failureGenerator, sys, iters=iters, epochs=epoch, concur=concur, mission=mission, prev_fail_reports_filename=fail_reports_filename)
        #     failed_iters += res[0]
        #     total_iters += res[1]
        #     metrics += res[2]
        #     new_fail_reports += res[3]
        #     # print(metrics)
        #     simulationTime = time.time() - start
        #     print("simulation time: {}".format(simulationTime))
        #     print("failed_iters: {}  total_iters: {}".format(failed_iters, total_iters))

        # total_iters *= mission/YEAR

        # print(metrics.count)

        # # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        # prob = failed_iters/total_iters
        # nines = str(round(-math.log10(failed_iters/total_iters),3))
        # sigma = str(round(1/(math.log(10) * (failed_iters**0.5)),3))
        # print("Num of Nine: " + nines)
        # print("error sigma: " + sigma)


        return SimulationResult(failed_iters, int(total_iters), metrics)