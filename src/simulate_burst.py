from heapq import *
import logging
from constants.time import YEAR
import numpy as np
import os
from mytimer import Mytimer
from system import System
#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    def __init__(self, num_disks, sys, repair = None):
        #---------------------------------------
        self.sys: System = sys
        self.repair = repair
        self.place_type = sys.place_type
        #---------------------------------------
        self.num_disks = num_disks



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, failureGenerator, mytimer) -> int:
        logging.info("---------")

        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        failures = failureGenerator.gen_failure_burst(self.sys.num_disks_per_rack, self.sys.num_racks)
        if self.place_type == 0:
            return self.flat_cluster_check_burst(failures)
        if self.place_type == 1:
            return self.flat_decluster_check_burst(failures)
        if self.place_type == 2:
            return self.mlec_cluster_check_burst(failures)
        if self.place_type == 3:
            return self.network_cluster_check_burst(failures)
        if self.place_type == 4:
            return self.mlec_decluster_check_burst(failures)
        
        raise NotImplementedError("placement type not recognized")


    def flat_cluster_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.n
        failed_disks_per_diskgroup = [0] * num_diskgroups
        for _, diskId in failures:
            diskgroupId = diskId // self.sys.n
            failed_disks_per_diskgroup[diskgroupId] += 1
            if failed_disks_per_diskgroup[diskgroupId] > self.sys.m:
                return 1
        return 0

    def flat_decluster_check_burst(self, failures):
        num_enclosures = self.sys.num_disks // self.sys.num_disks_per_enclosure
        failed_disks_per_enclosure = [0] * num_enclosures
        for _, diskId in failures:
            enclosureId = diskId // self.sys.num_disks_per_enclosure
            failed_disks_per_enclosure[enclosureId] += 1
            if failed_disks_per_enclosure[enclosureId] > self.sys.m:
                return 1
        return 0


    
    def mlec_cluster_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.n
        failed_disks_per_diskgroup = [0] * num_diskgroups
        # print(self.sys.num_disks)

        num_diskgroup_stripesets = self.sys.num_disks // self.sys.n // self.sys.top_n
        failed_diskgroups_per_stripeset = [0] * num_diskgroup_stripesets

        # print(failures)

        for _, diskId in failures:
            diskgroupId = diskId // self.sys.n
            # print('diskgroupId:{}'.format(diskgroupId))
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



    def network_cluster_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.top_n
        failed_disks_per_diskgroup = [0] * num_diskgroups
        for _, diskId in failures:
            # similar to mlec diskgroupStripesetId
            diskgroupId = (diskId % self.sys.num_disks_per_rack) + (diskId // (self.sys.num_disks_per_rack * self.sys.top_n)) * self.sys.num_disks_per_rack 
            failed_disks_per_diskgroup[diskgroupId] += 1
            if failed_disks_per_diskgroup[diskgroupId] > self.sys.top_m:
                return 1
        return 0

    def mlec_decluster_check_burst(self, failures):
        num_enclosures = self.sys.num_disks // self.sys.num_disks_per_enclosure
        num_enclosures_per_rack = self.sys.num_disks_per_rack // self.sys.num_disks_per_enclosure
        failed_disks_per_enclosure = [0] * num_enclosures
        failed_enclosures_per_enclosuregroup = [0] * (num_enclosures // self.sys.top_n)
        for _, diskId in failures:
            enclosureId = diskId // self.sys.num_disks_per_enclosure
            failed_disks_per_enclosure[enclosureId] += 1
            if failed_disks_per_enclosure[enclosureId] == self.sys.m + 1:
                enclosuregroupId = (enclosureId % num_enclosures_per_rack) + (enclosureId // (num_enclosures_per_rack * self.sys.top_n)) * num_enclosures_per_rack
                failed_enclosures_per_enclosuregroup[enclosuregroupId] += 1
                if failed_enclosures_per_enclosuregroup[enclosuregroupId] > self.sys.top_m:
                    return 1
        return 0

    

