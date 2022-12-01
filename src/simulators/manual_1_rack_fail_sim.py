import numpy as np
import math
import time

from failure_generator import FailureGenerator
from simulators.Simulator import Simulator
from constants.PlacementType import parse_placement, PlacementType
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
            place_type = parse_placement(placement)
            local_place_type = PlacementType.RAID        # local RAID
            if place_type == PlacementType.MLEC_DP:         # if MLEC_DP
                local_place_type = PlacementType.DP    # local DP
            failureGenerator = FailureGenerator(afr)
            sys = System(
                num_disks=drives_per_rack, 
                num_disks_per_rack=drives_per_rack, 
                k=k_local, 
                m=p_local, 
                place_type=local_place_type, 
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
            #       = P(rack 1 fails | rack_stripeset 1 fails) * P(rack_stripeset 1 fails | system fails)
            num_rack_stripesets = total_drives // drives_per_rack // (k_net + p_net)
            pro_sys_fail_contain_rack_stripeset_1 = (
                        1 - math.comb(num_rack_stripesets - 1, 1) / math.comb(num_rack_stripesets, 1))
            pro_rack_stripeset_1_fail_contain_s1 = (
                        1 - math.comb(k_net + p_net - 1, p_net + 1) / math.comb(k_net + p_net, p_net + 1))
            pro_sys_fail_contain_s1 = pro_sys_fail_contain_rack_stripeset_1 * pro_rack_stripeset_1_fail_contain_s1
            print('------------')
            print('Probability that system failure contains rack one: {}'.format(pro_sys_fail_contain_s1))

            # Compute P(system fails | rack 1 fails)
            failureGenerator2 = FailureGenerator(afr)
            sys2 = System(
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
                rack_fail=1)

            res = [0, 0, Metrics()]

            while res[0] < 20:
                start  = time.time()
                temp = self.run(failureGenerator2, sys2, iters=50000, epochs=200, concur=200)
                res[0] += temp[0]
                res[1] += temp[1]
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
            nn = str(round(-math.log10(res[0]/res[1]),3))
            sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
            print("Num of Nine: " + nn)
            print("error sigma: " + sigma)

            output = open("s-result-{}.log".format(placement), "a")
            output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {}\n".format(
                    k_local, p_local, k_net, p_net, total_drives,
                    afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt"))
            output.close()