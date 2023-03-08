import logging
import numpy as np
import os
from mytimer import Mytimer
from system import System
from constants.PlacementType import PlacementType

from policies.netdp.layout import net_dp_layout_chunk


#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    def __init__(self, num_disks, sys):
        #---------------------------------------
        self.sys: System = sys
        self.place_type = sys.place_type
        #---------------------------------------
        self.num_disks = num_disks



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, failureGenerator, num_chunks_per_disk) -> int:
        logging.info("---------")

        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        failures = failureGenerator.gen_failure_burst(self.sys.num_disks_per_rack, self.sys.num_racks)
        if self.place_type == PlacementType.SLEC_LOCAL_CP:
            return self.flat_cluster_check_burst(failures)
        if self.place_type == PlacementType.DP:
            return self.flat_decluster_check_burst(failures)
        if self.place_type == PlacementType.MLEC_C_C:
            return self.mlec_cluster_check_burst(failures)
        if self.place_type == PlacementType.RAID_NET:
            return self.network_cluster_check_burst(failures)
        if self.place_type == PlacementType.DP_NET:
            return self.network_decluster_check_burst(failures, num_chunks_per_disk)
            # return self.network_decluster_check_burst_theory(failures, num_chunks_per_disk)
        if self.place_type == PlacementType.MLEC_C_D:
            return self.mlec_decluster_check_burst(failures)
        if self.place_type == PlacementType.MLEC_D_C:
            return self.mlec_d_c_check_burst(failures)
        if self.place_type == PlacementType.MLEC_D_D:
            return self.mlec_d_d_check_burst(failures)

            
        
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

        num_diskgroup_spools = self.sys.num_disks // self.sys.n // self.sys.top_n
        failed_diskgroups_per_spool = [0] * num_diskgroup_spools

        # print(failures)

        for _, diskId in failures:
            diskgroupId = diskId // self.sys.n
            # print('diskgroupId:{}'.format(diskgroupId))
            failed_disks_per_diskgroup[diskgroupId] += 1
            # print('{} {}'.format(diskgroupId, failed_disks_per_diskgroup[diskgroupId]))
            # we only increment failed_diskgroups_per_spool once when failed_disks_per_diskgroup first reaches m+1
            # when it reaches m+2 or more, we don't increment failed_diskgroups_per_spool because we already know this diskgroup failed.
            if failed_disks_per_diskgroup[diskgroupId] == self.sys.m + 1:
                # let's say we do (2+1)/(2+1). Let say we have 6 disks per rack. And we have 6 racks
                # Then each rack will have 2 disk groups.
                # (0,1), (2,3), (4,5), (6,7), (8,9), (10,11) so we have in total 12 disk groups.
                # we do network erasure between disk groups.
                # so the disk group spools will be:
                # (0,2,4), (1,3,5), (6,8,10), (7,9,11)
                # we want to know the disk group spool id for a centain disk group. 
                # Let's valiadate if the formula below is correct
                # let's check diskgroup 11:
                # diskgroupSpoolId = (11 % 2) + (11 // (2*3)) * 2 = 1 + (1*2) = 1+2 = 3
                # let's check disgroup 3:
                # diskgroupSpoolId = (3 % 2) + (3 // (2*3)) * 2 = 1 + (0*2) = 1+0 = 1
                num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.sys.n
                diskgroupSpoolId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
                failed_diskgroups_per_spool[diskgroupSpoolId] += 1
                # print('diskgroupSpoolId:{}  {}'.format(diskgroupSpoolId, failed_diskgroups_per_spool[diskgroupSpoolId]))
                if failed_diskgroups_per_spool[diskgroupSpoolId] > self.sys.top_m:
                    return 1
        return 0

    # top declustered, bottom clustered
    def mlec_d_c_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.n
        failed_disks_per_diskgroup = [0] * num_diskgroups

        failed_diskgroups_per_rack = [0] * self.sys.num_racks
        num_failed_racks = 0

        for _, diskId in failures:
            diskgroupId = diskId // self.sys.n
            # print('diskgroupId:{}'.format(diskgroupId))
            failed_disks_per_diskgroup[diskgroupId] += 1
            # print('{} {}'.format(diskgroupId, failed_disks_per_diskgroup[diskgroupId]))
            # we only increment failed_diskgroups_per_spool once when failed_disks_per_diskgroup first reaches m+1
            # when it reaches m+2 or more, we don't increment failed_diskgroups_per_spool because we already know this diskgroup failed.
            if failed_disks_per_diskgroup[diskgroupId] == self.sys.m + 1:
                rackId = diskId // self.sys.num_disks_per_rack
                failed_diskgroups_per_rack[rackId] += 1
                if failed_diskgroups_per_rack[rackId] == 1:
                    num_failed_racks += 1
                    if num_failed_racks > self.sys.top_m:
                        return 1
        return 0
    
    def mlec_d_d_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.num_disks_per_enclosure
        failed_disks_per_diskgroup = [0] * num_diskgroups

        failed_diskgroups_per_rack = [0] * self.sys.num_racks
        num_failed_racks = 0

        for _, diskId in failures:
            diskgroupId = diskId // self.sys.num_disks_per_enclosure
            # print('diskgroupId:{}'.format(diskgroupId))
            failed_disks_per_diskgroup[diskgroupId] += 1
            # print('{} {}'.format(diskgroupId, failed_disks_per_diskgroup[diskgroupId]))
            # we only increment failed_diskgroups_per_spool once when failed_disks_per_diskgroup first reaches m+1
            # when it reaches m+2 or more, we don't increment failed_diskgroups_per_spool because we already know this diskgroup failed.
            if failed_disks_per_diskgroup[diskgroupId] == self.sys.m + 1:
                rackId = diskId // self.sys.num_disks_per_rack
                failed_diskgroups_per_rack[rackId] += 1
                if failed_diskgroups_per_rack[rackId] == 1:
                    num_failed_racks += 1
                    if num_failed_racks > self.sys.top_m:
                        return 1
        return 0



    def network_cluster_check_burst(self, failures):
        num_diskgroups = self.sys.num_disks // self.sys.top_n
        failed_disks_per_diskgroup = [0] * num_diskgroups
        for _, diskId in failures:
            # similar to mlec diskgroupSpoolId
            diskgroupId = (diskId % self.sys.num_disks_per_rack) + (diskId // (self.sys.num_disks_per_rack * self.sys.top_n)) * self.sys.num_disks_per_rack 
            failed_disks_per_diskgroup[diskgroupId] += 1
            if failed_disks_per_diskgroup[diskgroupId] > self.sys.top_m:
                return 1
        return 0

    def network_decluster_check_burst(self, failures, num_chunks_per_disk):
        num_chunks_total = self.sys.num_racks * self.sys.num_disks_per_rack * num_chunks_per_disk
        num_stripes_total = num_chunks_total // (self.sys.top_n)

        stripeid_per_disk_all, stripes = net_dp_layout_chunk(self.sys.num_racks, self.sys.num_disks_per_rack, num_chunks_per_disk, self.sys.top_n)
        failed_disks = []
        stripe_damage = [0 for i in range(num_stripes_total)]
        affected_stripes = {}
        for _, diskId in failures:
            # similar to mlec diskgroupSpoolId
            for stripeid in stripeid_per_disk_all[diskId]:
                stripe_damage[stripeid] += 1
                affected_stripes[stripeid] = 1
        for affected_stripe in affected_stripes:
            if stripe_damage[affected_stripe] > self.sys.top_m:
                return 1
        return 0
    

    def network_decluster_check_burst_theory(self, failures, num_chunks_per_disk):
        num_chunks_total = self.sys.num_racks * self.sys.num_disks_per_rack * num_chunks_per_disk
        num_stripes_total = num_chunks_total // (self.sys.top_n)

        
        return 0
    
    # def network_decluster_check_loss(failures_per_rack, num_chunks_per_disk, k, p):
        
    #     num_racks = len(failures_per_rack)
    #     if k+p > num_racks:
            
    #     prob_pick_rack_0 = math.comb(len(failures_per_rack)-1, )math.comb(len(failures_per_rack), k+p)



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

    

