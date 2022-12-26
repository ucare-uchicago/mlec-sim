import numpy as np
import logging
import math
import time

from simulators.Simulator import Simulator
from constants.time import YEAR
from system import System
from metrics import Metrics

class IoOverYearSim(Simulator):
    
    def simulate(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        return self.io_over_year(afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net, total_drives, drives_per_rack, placement, distribution, concur, epoch, iters)
    
    # ------------------------
    # Ignore this function for now
    # ------------------------

    def io_over_year(self, afr, io_speed, intrarack_speed, interrack_speed, cap, adapt, k_local, p_local, k_net, p_net,
                    total_drives, drives_per_rack, placement, distribution, concur, epoch, iters):
        pass
        # # logging.basicConfig(level=logging.INFO)
        # rebuildio_prev_year = 0
        # place_type = get_placement_index(placement)

        # for years in range(1,51,1):
        #     mission = years*YEAR
        #     drive_args1 = DriveArgs(d_shards=k_local, p_shards=p_local, afr=afr, drive_cap=cap, rec_speed=io_speed)
        #     sys_state1 = FailureGenerator(total_drives=total_drives, drive_args=drive_args1, placement=placement, drives_per_rack=drives_per_rack, 
        #                     top_d_shards=k_net, top_p_shards=p_net, adapt=adapt, rack_fail = 0, distribution = distribution)

        #     res = [0, 0, Metrics()]
        #     start  = time.time()
        #     temp = simulate(sys_state1, iters=int(10000000/200/years), epochs=200, concur=200, mission=mission)
        #     res[0] += temp[0]
        #     res[1] += temp[1]
        #     res[2] += temp[2]
        #     print(res[2])
        #     simulationTime = time.time() - start
        #     print("simulation time: {}".format(simulationTime))
        #     # res = simulate(sys_state1, iters=1000, epochs=1, concur=1)
        #     print('++++++++++++++++++++++++++++++++')
        #     print('Total Fails: ' + str(res[0]))
        #     print('Total Iters: ' + str(res[1]))

        #     res[1] *= years

        #     rebuildio = res[2].getAverageRebuildIO() - rebuildio_prev_year
        #     rebuildio_prev_year = res[2].getAverageRebuildIO()

        #     if res[0] == 0:
        #         print("NO FAILURE!")
        #         nn = 'N/A'
        #         sigma = 'N/A'
        #     else:
        #         # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        #         nn = str(round(-math.log10(res[0]/res[1]),3))
        #         sigma = str(round(1/(math.log(10) * (res[0]**0.5)),3))
        #         print("Num of Nine: " + nn)
        #         print("error sigma: " + sigma)

        #     output = open("s-rebuildio-{}.log".format(placement), "a")
        #     output.write("({}+{})({}+{}) {} {} {} {} {} {} {} {} {} {} {}\n".format(
        #         k_local, p_local, k_net, p_net, total_drives,
        #         afr, cap, io_speed, nn, sigma, res[0], res[1], "adapt" if adapt else "notadapt",
        #         years, rebuildio))
        #     output.close()