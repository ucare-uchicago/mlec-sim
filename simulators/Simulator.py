from __future__ import annotations
import typing
if (typing.TYPE_CHECKING):
    from failure_generator import FailureGenerator

import copy 
import traceback
import time
import json

from concurrent.futures import ProcessPoolExecutor

from mytimer import Mytimer
from simulate import Simulate
from metrics import Metrics
from constants.time import YEAR
from constants.SimulationResult import SimulationResult
from constants.PlacementType import PlacementType

from system import System
from failure_generator import FailureGenerator

from util import wait_futures

class Simulator:
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution, concur, epoch, iters, spool_size, repair_scheme, detection_time,
                num_local_fail_to_report, num_net_fail_to_report, prev_fail_reports_filename) -> SimulationResult:
        raise NotImplementedError("simulate() not implemented")

    def iter(self, afr, iters, mission, prev_fail_reports_filename=None, manual_spool_fail=False, **sys_kwargs):
        try:
            res = 0
            start = time.time()
            mytimer: Mytimer = Mytimer()

            deepcopystart = time.time()
            
            sys = System(**sys_kwargs)
            failureGenerator = FailureGenerator(afr, failures_store_len=sys.num_disks*100)

            deepcopyend = time.time()
            mytimer.copytime += deepcopyend - deepcopystart

            prev_fail_reports = None
            if prev_fail_reports_filename:
                if sys.place_type in [PlacementType.SLEC_LOCAL_CP, PlacementType.SLEC_LOCAL_DP]:
                    prev_fail_reports_filename = 'fail_reports_{}+{}-{}+{}_{}_{}f_rs{}.log'.format(
                                sys.top_k, sys.top_m, sys.k, sys.m, sys.place_type, 
                                sys.num_local_fail_to_report-1, sys.repair_scheme)
                elif sys.place_type in [PlacementType.SLEC_NET_CP, PlacementType.SLEC_NET_DP]:
                    prev_fail_reports_filename = 'fail_reports_{}+{}-{}+{}_{}_{}f_rs{}.log'.format(
                                sys.top_k, sys.top_m, sys.k, sys.m, sys.place_type, 
                                sys.num_net_fail_to_report-1, sys.repair_scheme)
                elif sys.place_type in [PlacementType.MLEC_C_C, PlacementType.MLEC_C_D, PlacementType.MLEC_D_C, PlacementType.MLEC_D_D]:
                    prev_fail_reports_filename = 'fail_reports_{}+{}-{}+{}_{}_{}f{}f_rs{}.log'.format(
                                sys.top_k, sys.top_m, sys.k, sys.m, sys.place_type, sys.num_net_fail_to_report-1,
                                0, sys.repair_scheme)
                with open(prev_fail_reports_filename, 'r') as f:
                    prev_fail_reports = json.load(f)
            if manual_spool_fail:
                sys.manual_spool_fail = True
                if sys.place_type in [PlacementType.MLEC_C_C, PlacementType.MLEC_D_C]:
                    local_place = PlacementType.SLEC_LOCAL_CP
                else:
                    local_place = PlacementType.SLEC_LOCAL_DP
                spool_samples_filename = 'fail_reports_{}+{}-{}+{}_{}_{}f_rs{}.log'.format(
                        1, 0, sys.k, sys.m, local_place, sys.m+1, 0)
                with open(spool_samples_filename, 'r') as f:
                    sys.spool_samples = json.load(f)

            for iter in range(0, iters):
                # logging.info("")
                iter_start = time.time()
                sim = Simulate(mission, sys.num_disks, sys, prev_fail_reports=prev_fail_reports)
                mytimer.simInitTime += time.time() - iter_start
                res += sim.run_simulation(failureGenerator, mytimer)
                iter_end = time.time()
                # print(mytimer)
                # print("Finishing iter " + str(iter) + " taking " + str((iter_end - iter_start) * 1000) + "ms")
            end = time.time()
            mytimer.totalTime += end - start
            # print(mytimer)
            return (res, mytimer, sys.metrics, sys.fail_reports)
        except Exception as e:
            print(traceback.format_exc())
            return None

    # ----------------------------
    # This is a parallel/multi-iter wrapper around iter() function
    # We run X threads in parallel to run the simulation. X = concur.
    # ----------------------------
    def run(self, afr, iters, epochs, concur=10, mission=YEAR, prev_fail_reports_filename=None, manual_spool_fail=False, **sys_kwargs):
        # So tick(state) is for a single system, and we want to simulate multiple systems
        executor = ProcessPoolExecutor(concur)
        
        failed_instances = 0
        futures = []
        metrics = Metrics()
        fail_reports = []

        for epoch in range(0, epochs):
            futures.append(executor.submit(self.iter, afr, iters, mission, prev_fail_reports_filename, manual_spool_fail, **sys_kwargs))
        ress = wait_futures(futures)
        
        executor.shutdown()
        for res in ress:
            failed_instances += res[0]
            metrics += res[2]
            fail_reports += res[3]

        
        # logging.info("  failed_instances: {}".format(failed_instances))
        return [failed_instances, epochs * iters, metrics, fail_reports]