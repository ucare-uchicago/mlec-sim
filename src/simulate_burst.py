from inspect import trace
from multiprocessing.pool import ThreadPool
from placement import Placement
from repair import Repair
from state import State
from disk import Disk
from rack import Rack
from heapq import *
import logging
import time
import sys
from constants import debug, YEAR
import numpy as np
import time
import os
from mytimer import Mytimer
import random
#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    def __init__(self, mission_time, num_disks, sys = None, repair = None, placement = None):
        self.mission_time = mission_time
        #---------------------------------------
        self.sys = sys
        self.repair = repair
        self.placement = placement
        #---------------------------------------
        self.num_disks = num_disks



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, failureGenerator, mytimer):
        logging.info("---------")

        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        failures = failureGenerator.gen_failure_burst(self.sys.num_disks_per_rack, self.sys.num_racks)
        prob = self.mlec_check_burst(failures)

        return prob

    
    def mlec_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.n
        failed_disks_per_diskgroup = [0] * num_diskgroups

        num_diskgroup_stripesets = self.sys.num_disks // self.sys.n // self.sys.top_n
        failed_diskgroups_per_stripeset = [0] * num_diskgroup_stripesets

        # print(failures)

        for _, diskId in failures:
            diskgroupId = diskId // self.sys.n
            failed_disks_per_diskgroup[diskgroupId] += 1
            # print('{} {}'.format(diskgroupId, failed_disks_per_diskgroup[diskgroupId]))
            # we only increment failed_diskgroups_per_stripeset once when failed_disks_per_diskgroup first reaches m+1
            # when it reaches m+2 or more, we don't increment failed_diskgroups_per_stripeset because we already know this diskgroup failed.
            if failed_disks_per_diskgroup[diskgroupId] == self.sys.m + 1:
                # let's say we do (2+1)/(2+1). Let say we have 6 disks per rack. And we have 6 racks
                # Then each rack will have 2 disk groups.
                # (0,1), (2,3), (4,5), (6,7), (8,9), (10,11) so we have in total 12 disk groups.
                # we do network erasure between disk groups.
                # so the disk group stripesets will be:
                # (0,2,4), (1,3,5), (6,8,10), (7,9,11)
                # we want to know the disk group stripeset id for a centain disk group. 
                # Let's valiadate if the formula below is correct
                # let's check diskgroup 11:
                # diskgroupStripesetId = (11 % 2) + (11 // (2*3)) * 2 = 1 + (1*2) = 1+2 = 3
                # let's check disgroup 3:
                # diskgroupStripesetId = (3 % 2) + (3 // (2*3)) * 2 = 1 + (0*2) = 1+0 = 1
                num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.sys.n
                diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
                failed_diskgroups_per_stripeset[diskgroupStripesetId] += 1
                # print('diskgroupStripesetId:{}  {}'.format(diskgroupStripesetId, failed_diskgroups_per_stripeset[diskgroupStripesetId]))
                if failed_diskgroups_per_stripeset[diskgroupStripesetId] > self.sys.top_m:
                    return 1
        return 0


        
        


    

