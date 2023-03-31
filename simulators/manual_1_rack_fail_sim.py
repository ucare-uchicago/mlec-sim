import numpy as np
import math
import time

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.PlacementType import PlacementType
from constants.SimulationResult import SimulationResult
from constants.constants import kilo

from system import System
from metrics import Metrics

class ManualFailOneRackSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.manual_1_rack_failure(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    # ------------------------
    # Manually inject 1 rack failure in each simulation to make it easier to find system failure
    # 1. get P(rack 1 fails)
    # 2. compute P(rack 1 fails | system fails) using probability theory
    # 3. get P(system fails | rack 1 fails) by manually inject 1 rack failure in each simulation
    # 4. P(system fails) = P(system fails | rack 1 fails) * P(rack 1 fails) / P(rack 1 fails | system fails)
    # ------------------------
    def manual_1_rack_failure(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                        total_drives, drives_per_rack, placement, dist, concur, epoch, iters):
        # logging.basicConfig(level=logging.INFO)

        
        for afr in range(5, 6):
            # 1. get P(rack 1 fails)
            local_place_type = PlacementType.SLEC_LOCAL_CP        # local RAID
            if placement == PlacementType.MLEC_C_D:         # if MLEC_C_D
                local_place_type = PlacementType.SLEC_LOCAL_DP    # local DP
            failureGenerator = FailureGenerator(afr)
            sys = System(
                num_disks=drives_per_rack, 
                num_disks_per_rack=drives_per_rack, 
                k=k_local, 
                m=p_local, 
                place_type=local_place_type, 
                diskCap=cap * kilo * kilo,
                rebuildRate=io_speed, 
                intrarack_speed=intrarack_speed, 
                interrack_speed=interrack_speed, 
                utilizeRatio=1, 
                top_k=k_net, 
                top_m=p_net, 
                adapt=adapt, 
                rack_fail=0)

            res = [0, 0, Metrics()]

            # loop until we get enough failures
            while res[0] < 20:
                start  = time.time()
                temp = self.run(failureGenerator, sys, iters=50000, epochs=200, concur=200)
                res[0] += temp[0]
                res[1] += temp[1]
                simulationTime = time.time() - start
                print("simulation time: {}".format(simulationTime))
                print(res)
            # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
            rack_one_fail_prob = res[0] / res[1]
            print('++++++++++++++++++++')
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that rack one fails: {}'.format(rack_one_fail_prob))

            # Compute P(rack 1 fails | system fails)
            #       = P(rack 1 fails | rack_spool 1 fails) * P(rack_spool 1 fails | system fails)
            num_rack_spools = total_drives // drives_per_rack // (k_net + p_net)
            pro_sys_fail_contain_rack_spool_1 = (
                        1 - math.comb(num_rack_spools - 1, 1) / math.comb(num_rack_spools, 1))
            pro_rack_spool_1_fail_contain_s1 = (
                        1 - math.comb(k_net + p_net - 1, p_net + 1) / math.comb(k_net + p_net, p_net + 1))
            pro_sys_fail_contain_s1 = pro_sys_fail_contain_rack_spool_1 * pro_rack_spool_1_fail_contain_s1
            print('------------')
            print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1))

            # Compute P(system fails | rack 1 fails)
            failureGenerator2 = FailureGenerator(afr)
            sys2 = System(
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
                rack_fail=1)

            res = [0, 0, Metrics()]

            while res[0] < 20:
                start  = time.time()
                temp = self.run(failureGenerator2, sys2, iters=50000, epochs=200, concur=200)
                res[0] += temp[0]
                res[1] += temp[1]
                res[2] += temp[2]
                simulationTime = time.time() - start
                print("simulation time: {}".format(simulationTime))
                print(res)
            # res = simulate(l1sys, iters=1000, epochs=1, concur=1)
            conditional_prob = res[0] / res[1]
            print('------------')
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that the system fails when rack one fails: {}'.format(conditional_prob))

            print('------------')

            # P(system fails) = P(system fails | rack 1 fails) * P(rack 1 fails) / P(rack 1 fails | system fails)
            res[0] = res[0] * rack_one_fail_prob / pro_sys_fail_contain_s1
            aggr_prob = conditional_prob * rack_one_fail_prob / pro_sys_fail_contain_s1
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that the system fails: {}'.format(aggr_prob))

            # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))

            return SimulationResult(res[0], res[1], res[2])