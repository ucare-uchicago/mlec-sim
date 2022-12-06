import logging
import time
from typing import List, Dict

from components.disk import Disk
from components.diskgroup import Diskgroup
from components.network import NetworkUsage
from policies.policy import Policy
from .pdl import mlec_cluster_pdl
from .repair import mlec_repair
from .network import update_network_state, update_network_state_diskgroup

class MLEC(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)

        self.failed_diskgroups = {}

        self.failed_diskgroups_per_stripeset = []
        self.num_diskgroup_stripesets = self.sys.num_disks // self.n // self.top_n
        for i in range(self.num_diskgroup_stripesets):
            self.failed_diskgroups_per_stripeset.append({})
        
        self.num_diskgroups_per_rack = self.sys.num_disks_per_rack // self.sys.n
        self.num_diskgroups = self.sys.num_disks // self.n
        diskgroup_repair_data = self.sys.diskSize * self.n  # when disk group fails, we repair the whole disk group
        self.diskgroups: Dict[int, Diskgroup] = {}
        for diskgroupId in range(self.num_diskgroups):
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            rackId = self.num_diskgroups // self.num_diskgroups_per_rack
            self.diskgroups[diskgroupId] = Diskgroup(diskgroupId, diskgroup_repair_data, self.n, rackId, diskgroupStripesetId)

    def update_disk_state(self, event_type, diskId):
        diskgroupId = diskId // self.n
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.diskgroups[diskgroupId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.diskgroups[diskgroupId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        # if disk fail event
        if event_type == Disk.EVENT_FAIL:
            diskgroupId = diskId // self.n

            # If the diskgroup is already failing, we do nothing
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                return
            
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the read IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            self.disks[diskId].repair_start_time = self.curr_time
            for failedDiskId in fail_per_diskgroup:
                self.update_disk_repair_time(failedDiskId, fail_per_diskgroup)

        # if disk repair event
        if event_type == Disk.EVENT_REPAIR:
            disk = self.disks[diskId]
            diskgroupId = diskId // self.n

            self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.k + 1)
            
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                # if diskgroup already failed, then no need to fail
                # this assumes we treat diskgroup as a blackbox and repair everything together
                # this is not true when we only repair failed stripes
                return
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            for failedDiskId in fail_per_diskgroup:
                    self.update_disk_repair_time(failedDiskId, fail_per_diskgroup)


    def update_disk_repair_time(self, diskId, fail_per_diskgroup):
        num_fail_per_diskgroup = len(fail_per_diskgroup)
        start = time.time()
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
            
            # This means that we just begun repair for this disk, we need to check network
            update_network_state(disk, fail_per_diskgroup, self)
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_diskgroup)

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("calculate repair time for disk {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 diskId, repaired_time, disk.repair_time[0], disk.repair_start_time))

        end = time.time()
        self.mytimer.updateDiskRepairTime += end - start

    #----------------------------------------------
    # update diskgroup state
    #----------------------------------------------
    def update_diskgroup_state(self, event_type, diskId):
        if event_type == Disk.EVENT_FAIL:
            
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
            diskgroupId = diskId // self.n
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            # if diskgroup already fails, we don't need to fail it again.
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                return None
            # otherwise, we need to check if a new diskgroup fails
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            if len(fail_per_diskgroup) > self.sys.m:
                self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
                self.failed_diskgroups[diskgroupId] = 1
                self.failed_diskgroups_per_stripeset[diskgroupStripesetId][diskgroupId] = 1
                return diskgroupId

        if event_type == Diskgroup.EVENT_FAIL:
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
            self.failed_diskgroups[diskgroupId] = 1
            self.failed_diskgroups_per_stripeset[diskgroupStripesetId][diskgroupId] = 1
            return diskgroupId

        if event_type == Diskgroup.EVENT_REPAIR:
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_NORMAL
            self.failed_diskgroups.pop(diskgroupId, None)
            self.failed_diskgroups_per_stripeset[diskgroupStripesetId].pop(diskgroupId, None)
            
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            for dId in fail_per_diskgroup:
                self.failed_disks.pop(dId, None)
                
            self.diskgroups[diskgroupId].failed_disks.clear()
            
            for dId in range(diskgroupId*self.n, (diskgroupId+1)*self.n):
                self.disks[diskId].state = Disk.STATE_NORMAL 
            
            self.sys.metrics.total_net_traffic += self.diskgroups[diskgroupId].repair_data * (self.sys.top_k + 1)
            self.sys.metrics.total_net_repair_time += self.curr_time - self.diskgroups[diskgroupId].init_repair_start_time
            self.sys.metrics.total_net_repair_count += 1
            return diskgroupId
        return None


    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_diskgroup_priority(self, event_type, diskgroupId, diskId):
        num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
        diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
        if event_type == Disk.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    self.update_diskgroup_repair_time(dgId, len(failed_diskgroups_per_stripeset))

        if event_type == Diskgroup.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    self.update_diskgroup_repair_time(dgId, len(failed_diskgroups_per_stripeset))

        if event_type == Diskgroup.EVENT_REPAIR:
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    self.update_diskgroup_repair_time(dgId, len(failed_diskgroups_per_stripeset))
    
    
    def update_diskgroup_repair_time(self, diskgroupId, failed_diskgroups_per_stripeset):
        diskgroup = self.diskgroups[diskgroupId]
        repaired_time = self.curr_time - diskgroup.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            diskgroup.curr_repair_data_remaining = diskgroup.repair_data
            
            # This means that we just begun repair for this disk, we need to check network
            update_network_state_diskgroup(diskgroup, failed_diskgroups_per_stripeset, self)
        else:
            repaired_percent = repaired_time / diskgroup.repair_time[0]
            diskgroup.curr_repair_data_remaining = diskgroup.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(diskgroup.curr_repair_data_remaining)/(self.sys.diskIO * self.n / failed_diskgroups_per_stripeset)
        diskgroup.repair_time[0] = repair_time / 3600 / 24
        diskgroup.repair_start_time = self.curr_time
        diskgroup.estimate_repair_time = self.curr_time + diskgroup.repair_time[0]

    def get_failed_disks_per_diskgroup(self, diskgroupId):
        return list(self.diskgroups[diskgroupId].failed_disks.keys())
    
    def get_failed_diskgroups_per_stripeset(self, diskgroupStripesetId):
        return list(self.failed_diskgroups_per_stripeset[diskgroupStripesetId].keys())

    def get_failed_diskgroups(self):
        return list(self.failed_diskgroups.keys())

    def check_pdl(self):
        return mlec_cluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        mlec_repair(self.diskgroups, self.get_failed_diskgroups(), self.state, repair_queue)