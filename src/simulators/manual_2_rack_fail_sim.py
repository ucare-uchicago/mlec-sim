import numpy as np
import math
import time

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.PlacementType import PlacementType
from constants.SimulationResult import SimulationResult
from system import System
from metrics import Metrics

class ManualFailTwoRackSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.manual_2_rack_failure(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    # ------------------------
    # Manually inject 2 rack failure in each simulation to make it easier to find system failure
    # 1. get P(rack 1 fails)
    # 2. get P(rack 1 and rack 2 fail)
    # 3. compute P(rack 1,2 fail | system fails) using probability theory
    # 4. get P(system fails | rack 1,2 fails) by manually inject 2 rack failure in each simulation
    # 5. P(system fails) = P(system fails | rack 1,2 fail) * P(rack 1,2 fails) / P(rack 1,2 fails | system fails)
    # ------------------------
    def manual_2_rack_failure(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, 
                        total_drives, drives_per_rack, placement, dist, concur, epoch, iters):
        for cap in range(100, 110, 10):
            # 1. get P(rack 1 fails)
            local_place_type = PlacementType.RAID        # local RAID
            if placement == PlacementType.MLEC_C_D:         # if MLEC_C_D
                local_place_type = PlacementType.DP    # local DP
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
            rack_one_fail_prob = res[0] / res[1]

            # 2. get P(rack 1 and rack 2 fail)
            rack_one_and_two_fail_prob = rack_one_fail_prob ** 2
            print('++++++++++++++++++++')
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that rack one fails: {}'.format(rack_one_fail_prob))
            print('Probability that rack one and two fails: {}'.format(rack_one_and_two_fail_prob))

            # 3. compute P(rack 1,2 fail | system fails) using probability theory
            pro_sys_fail_contain_s1_s2 = math.comb(k_net + p_net - 2, p_net + 1 - 2) / math.comb(k_net + p_net, p_net + 1)
            print('------------')
            print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1_s2))


            # 4. get P(system fails | rack 1,2 fails) by manually inject 2 rack failure in each simulation
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
                rack_fail=2)

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
                
            conditional_prob = res[0] / res[1]
            print('------------')
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that the system fails when rack one fails: {}'.format(conditional_prob))

            print('------------')

            # 5. P(system fails) = P(system fails | rack 1,2 fail) * P(rack 1,2 fails) / P(rack 1,2 fails | system fails)
            res[0] = res[0] * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
            aggr_prob = conditional_prob * rack_one_and_two_fail_prob / pro_sys_fail_contain_s1_s2
            print('Total Fails: ' + str(res[0]))
            print('Total Iters: ' + str(res[1]))
            print('Probability that the system fails: {}'.format(aggr_prob))

            return SimulationResult(res[0], res[1], res[2])