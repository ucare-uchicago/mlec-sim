from __future__ import annotations
import typing
if (typing.TYPE_CHECKING):
    from failure_generator import FailureGenerator

import copy 
import traceback
import time

from concurrent.futures import ProcessPoolExecutor

from mytimer import Mytimer
from simulate import Simulate
from metrics import Metrics
from constants.time import YEAR
from constants.SimulationResult import SimulationResult

from util import wait_futures

class Simulator:
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                total_drives, drives_per_rack, placement, distribution, concur, epoch, iters) -> SimulationResult:
        raise NotImplementedError("simulate() not implemented")

    def iter(self, failureGenerator_: FailureGenerator, sys_, iters, mission):
        try:
            res = 0
            start = time.time()
            mytimer: Mytimer = Mytimer()

            deepcopystart = time.time()
            failureGenerator = copy.deepcopy(failureGenerator_)
            sys = copy.deepcopy(sys_)

            deepcopyend = time.time()
            mytimer.copytime += deepcopyend - deepcopystart

            
            for iter in range(0, iters):
                # logging.info("")
                iter_start = time.time()
                sim = Simulate(mission, sys.num_disks, sys)
                mytimer.simInitTime += time.time() - iter_start
                res += sim.run_simulation(failureGenerator, mytimer)
                iter_end = time.time()
                # print(mytimer)
                # print("Finishing iter " + str(iter) + " taking " + str((iter_end - iter_start) * 1000) + "ms")
            end = time.time()
            mytimer.totalTime += end - start
            print(mytimer)
            return (res, mytimer, sys.metrics)
        except Exception as e:
            print(traceback.format_exc())
            return None

    # ----------------------------
    # This is a parallel/multi-iter wrapper around iter() function
    # We run X threads in parallel to run the simulation. X = concur.
    # ----------------------------
    def run(self, failureGenerator, sys, iters, epochs, concur=10, mission=YEAR):
        # So tick(state) is for a single system, and we want to simulate multiple systems
        executor = ProcessPoolExecutor(concur)
        
        failed_instances = 0
        futures = []
        metrics = Metrics()

        for epoch in range(0, epochs):
            futures.append(executor.submit(self.iter, failureGenerator, sys, iters, mission))
        ress = wait_futures(futures)
        
        executor.shutdown()
        for res in ress:
            failed_instances += res[0]
            metrics += res[2]
        
        # logging.info("  failed_instances: {}".format(failed_instances))
        return [failed_instances, epochs * iters, metrics]