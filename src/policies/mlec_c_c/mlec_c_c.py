import logging
import time
from typing import List, Dict, Optional, Tuple

from components.disk import Disk
from components.diskgroup import Diskgroup
from components.network import NetworkUsage
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_cluster_pdl
from .repair import mlec_repair
from .network import update_network_state, update_network_state_diskgroup, diskgroup_to_read_for_repair, disks_to_read_for_repair, used_for_repair_top_level

class MLEC_C_C(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)

        self.failed_diskgroups = {}

        self.repairing_spool = []
        self.failed_diskgroups_per_spool = []
        self.num_diskgroup_spools = self.sys.num_disks // self.n // self.top_n
        for i in range(self.num_diskgroup_spools):
            self.failed_diskgroups_per_spool.append({})
        
        self.num_diskgroups_per_rack = self.sys.num_disks_per_rack // self.sys.n
        self.num_diskgroups = self.sys.num_disks // self.n
        diskgroup_repair_data = self.sys.diskSize * self.n  # when disk group fails, we repair the whole disk group
        self.diskgroups = self.sys.diskgroups
        self.affected_mlec_groups = {}

    def update_disk_state(self, event_type, diskId):
        diskgroupId = diskId // self.n
        rackId = self.state.disks[diskId].rackId
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.racks[rackId].failed_disks.pop(diskId, None)
            self.diskgroups[diskgroupId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            
            self.sys.metrics.total_net_bandwidth_replenish_time += 1
            self.disks[diskId].network_usage = None
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.diskgroups[diskgroupId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        if event_type in [Disk.EVENT_FAIL]:
            diskgroupId = diskId // self.n

            # If the diskgroup is already failing, we do nothing
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                # logging.info("Diskgroup already in failed state, ignoring")
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
        # logging.info("Disk %s has repaired time of %s", diskId, repaired_time)
        
        
        update_result = None
        if repaired_time == 0:            
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        # Calculate the real repair rate by the dividing the total bandwidht used by k - that's the effectively write speed
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_diskgroup)

        disk.repair_time[0] = repair_time / 3600 / 24

        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]

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
            # so the disk group spools will be:
            # (0,2,4), (1,3,5), (6,8,10), (7,9,11)
            # we want to know the disk group spool id for a centain disk group. 
            # Let's valiadate if the formula below is correct
            # let's check diskgroup 11:
            # diskgroupSpoolId = (11 % 2) + (11 // (2*3)) * 2 = 1 + (1*2) = 1+2 = 3
            # let's check disgroup 3:
            # diskgroupSpoolId = (3 % 2) + (3 // (2*3)) * 2 = 1 + (0*2) = 1+0 = 1
            diskgroupId = diskId // self.n
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupSpoolId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            # if diskgroup already fails, we don't need to fail it again.
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            if len(fail_per_diskgroup) > self.sys.m:
                # logging.error("Diskgroup %s failed due to the disk failure, it has failed disks %s", diskgroupId, self.get_failed_disks_per_diskgroup(diskgroupId))

                self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
                self.failed_diskgroups[diskgroupId] = 1
                self.failed_diskgroups_per_spool[diskgroupSpoolId][diskgroupId] = 1
                self.affected_mlec_groups[diskgroupSpoolId] = len(self.failed_diskgroups_per_spool[diskgroupSpoolId])
                return diskgroupId

        if event_type == [Diskgroup.EVENT_FAIL]:
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupSpoolId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
            self.failed_diskgroups[diskgroupId] = 1
            self.failed_diskgroups_per_spool[diskgroupSpoolId][diskgroupId] = 1
            self.affected_mlec_groups[diskgroupSpoolId] = len(self.failed_diskgroups_per_spool[diskgroupSpoolId])
            return diskgroupId

        if event_type == Diskgroup.EVENT_REPAIR:
            # logging.info("Diskgroup %s is repaired", diskId)
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupSpoolId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_NORMAL
            self.failed_diskgroups.pop(diskgroupId, None)
            self.failed_diskgroups_per_spool[diskgroupSpoolId].pop(diskgroupId, None)
            self.affected_mlec_groups[diskgroupSpoolId] = len(self.failed_diskgroups_per_spool[diskgroupSpoolId])
            if self.affected_mlec_groups[diskgroupSpoolId] == 0:
                self.affected_mlec_groups.pop(diskgroupSpoolId, None)
            
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            for dId in fail_per_diskgroup:
                self.failed_disks.pop(dId, None)
                
            self.diskgroups[diskgroupId].failed_disks.clear()
            
            for dId in range(diskgroupId*self.n, (diskgroupId+1)*self.n):
                self.disks[dId].state = Disk.STATE_NORMAL
            
            
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
        diskgroupSpoolId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
        if event_type == Disk.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_spool = self.get_failed_diskgroups_per_spool(diskgroupSpoolId)
                for dgId in failed_diskgroups_per_spool:
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_spool)

        if event_type == Diskgroup.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_spool = self.get_failed_diskgroups_per_spool(diskgroupSpoolId)
                for dgId in failed_diskgroups_per_spool:
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_spool)

        if event_type == Diskgroup.EVENT_REPAIR:
                failed_diskgroups_per_spool = self.get_failed_diskgroups_per_spool(diskgroupSpoolId)
                for dgId in failed_diskgroups_per_spool:
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_spool)
    
    
    def update_diskgroup_repair_time(self, diskgroupId, diskId, failed_diskgroups_per_spool):
        diskgroup = self.diskgroups[diskgroupId]
        repaired_time = self.curr_time - diskgroup.repair_start_time
        if repaired_time == 0:            
            repaired_percent = 0
            diskgroup.curr_repair_data_remaining = diskgroup.repair_data
        else:
            repaired_percent = repaired_time / diskgroup.repair_time[0]
            diskgroup.curr_repair_data_remaining = diskgroup.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(diskgroup.curr_repair_data_remaining)/(self.sys.diskIO * self.sys.n / len(failed_diskgroups_per_spool))
            
        diskgroup.repair_time[0] = repair_time / 3600 / 24
        diskgroup.repair_start_time = self.curr_time
        diskgroup.estimate_repair_time = self.curr_time + diskgroup.repair_time[0]

    def get_failed_disks_per_diskgroup(self, diskgroupId):
        return list(self.diskgroups[diskgroupId].failed_disks.keys())
    
    def get_failed_diskgroups_per_spool(self, diskgroupSpoolId):
        return list(self.failed_diskgroups_per_spool[diskgroupSpoolId].keys())

    def get_failed_diskgroups(self):
        return list(self.failed_diskgroups.keys())

    def check_pdl(self):
        return mlec_cluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        mlec_repair(self.diskgroups, self.get_failed_diskgroups(), self.state, repair_queue)


    def clean_failures(self):
        failed_disks = self.state.get_failed_disks()
        affected_diskgroups = {}
        for diskId in failed_disks:
            disk = self.state.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}
            diskgroupId = diskId // self.n
            affected_diskgroups[diskgroupId] = 1
        for diskgroupId in affected_diskgroups:
            self.diskgroups[diskgroupId].failed_disks.clear()
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_NORMAL
