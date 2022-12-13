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
from .network import update_network_state, update_network_state_diskgroup, diskgroup_to_read_for_repair, disks_to_read_for_repair

class MLEC(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)

        self.failed_diskgroups = {}

        self.repairing_stripeset = []
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
            rackId = diskgroupId // self.num_diskgroups_per_rack
            self.diskgroups[diskgroupId] = Diskgroup(diskgroupId, diskgroup_repair_data, self.n, rackId, diskgroupStripesetId)
            for diskId in self.diskgroups[diskgroupId].disks:
                self.disks[diskId].diskgroupId = diskgroupId

    def update_disk_state(self, event_type, diskId):
        diskgroupId = diskId // self.n
        rackId = self.state.disks[diskId].rackId
        logging.info("diskId %s, diskgroupId %s, rackId %s", diskId, diskgroupId, self.disks[diskId].rackId)
        logging.info(self.failed_diskgroups_per_stripeset)
        logging.info("Network state - inter: %s, intra: %s", self.state.network.inter_rack_avail, self.state.network.intra_rack_avail)
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.racks[rackId].failed_disks.pop(diskId, None)
            self.diskgroups[diskgroupId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            
            self.state.network.replenish(self.disks[diskId].network_usage)
            self.disks[diskId].network_usage = None
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.diskgroups[diskgroupId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1
            
        if event_type == Disk.EVENT_DELAYED_FAIL:
            # Do nothing for now
            pass


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        # if disk fail event
        if event_type in [Disk.EVENT_FAIL, Disk.EVENT_DELAYED_FAIL]:
            diskgroupId = diskId // self.n

            # If the diskgroup is already failing, we do nothing
            if self.diskgroups[diskgroupId].state == Diskgroup.STATE_FAILED:
                logging.info("Diskgroup already in failed state, ignoring")
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
                if failedDiskId not in self.state.simulation.delay_repair_queue[Components.DISK]:
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
                if failedDiskId not in self.state.simulation.delay_repair_queue[Components.DISK]:
                    self.update_disk_repair_time(failedDiskId, fail_per_diskgroup)


    def update_disk_repair_time(self, diskId, fail_per_diskgroup):
        num_fail_per_diskgroup = len(fail_per_diskgroup)
        start = time.time()
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        logging.info("Disk %s has repaired time of %s", diskId, repaired_time)
        if repaired_time == 0:
            # This means that we just begun repair for this disk, we need to check network
            updated = update_network_state(disk, fail_per_diskgroup, self)
            if not updated:
                return
            
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_diskgroup)
        logging.info("Repaired percent %s, Repair time %s", repaired_percent, repair_time)

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("calculate repair time for disk {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 diskId, repaired_time, disk.repair_time[0], disk.repair_start_time))
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

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
                # logging.error("Diskgroup %s failed due to the disk failure, it has failed disks %s", diskgroupId, self.get_failed_disks_per_diskgroup(diskgroupId))

                self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
                self.failed_diskgroups[diskgroupId] = 1
                self.failed_diskgroups_per_stripeset[diskgroupStripesetId][diskgroupId] = 1
                return diskgroupId

        if event_type == [Diskgroup.EVENT_FAIL, Diskgroup.EVENT_DELAYED_FAIL]:
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
                
            # We return the network resources this diskgroup was using
            self.state.network.replenish(self.diskgroups[diskgroupId].network_usage)
            self.diskgroups[diskgroupId].network_usage = None
            
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
                    if dgId not in self.state.simulation.delay_repair_queue[Components.DISKGROUP]:
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)

        if event_type == Diskgroup.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    if dgId not in self.state.simulation.delay_repair_queue[Components.DISKGROUP]:
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)

        if event_type == Diskgroup.EVENT_REPAIR:
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    if dgId not in self.state.simulation.delay_repair_queue[Components.DISKGROUP]:
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)
    
    
    def update_diskgroup_repair_time(self, diskgroupId, diskId, failed_diskgroups_per_stripeset):
        diskgroup = self.diskgroups[diskgroupId]
        repaired_time = self.curr_time - diskgroup.repair_start_time
        pause_repair = []
        if repaired_time == 0:
            
            # This means that we just begun repair for this disk, we need to check network
            update_result = update_network_state_diskgroup(diskgroup, failed_diskgroups_per_stripeset, self)
            if type(update_result) is bool:
                if not update_result:
                    return
            elif type(update_result) is list:
                # This means that these are the disks that we need to pause repair for
                pause_repair += update_result
                # logging.warn("We are pausing repairs for disks %s", pause_repair)
            
            repaired_percent = 0
            diskgroup.curr_repair_data_remaining = diskgroup.repair_data
        else:
            repaired_percent = repaired_time / diskgroup.repair_time[0]
            diskgroup.curr_repair_data_remaining = diskgroup.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(diskgroup.curr_repair_data_remaining)/(self.sys.diskIO * self.n / len(failed_diskgroups_per_stripeset))
        
        for pauseRepairDiskId in pause_repair:
            if pauseRepairDiskId != diskId:
                logging.info("Updating disk %s repair time from %s to %s", pauseRepairDiskId, self.disks[pauseRepairDiskId].repair_time[0], self.disks[pauseRepairDiskId].repair_time[0] + (repair_time / 3600 / 24))
                self.disks[pauseRepairDiskId].repair_time[0] += (repair_time / 3600 / 24)
            
        diskgroup.repair_time[0] = repair_time / 3600 / 24
        diskgroup.repair_start_time = self.curr_time
        diskgroup.estimate_repair_time = self.curr_time + diskgroup.repair_time[0]
        logging.info("Diskgroup %s will be repaired at %s", diskgroupId, diskgroup.estimate_repair_time)

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
    
    def intercept_next_event(self, prev_event) -> Optional[Tuple[float, str, int]]:
        logging.info("Trying to intercept event")
        if (len(self.state.simulation.delay_repair_queue[Components.DISK]) == 0 \
            and len(self.state.simulation.delay_repair_queue[Components.DISKGROUP]) == 0) \
                or self.state.network.inter_rack_avail == 0:
            return None

        
        # We first check whether any waiting disk groups can be repaired
        #  We prioritize disk groups ahead of disks because they are more important
        for diskgroupId in self.state.simulation.delay_repair_queue[Components.DISKGROUP]:
            diskgroup = self.diskgroups[diskgroupId]
            diskgroups_to_read = diskgroup_to_read_for_repair(diskgroup.diskgroupStripesetId, self)
            logging.info("Trying to repair diskgroup %s with readable sibling %s (top_k=%s)", diskgroupId, diskgroups_to_read, self.sys.top_k)
            if self.state.network.inter_rack_avail != 0 \
                and len(diskgroups_to_read) >= self.sys.top_k:
                    self.state.simulation.delay_repair_queue[Components.DISKGROUP].remove(diskgroupId)
                    return (prev_event[0], Diskgroup.EVENT_DELAYED_FAIL, diskgroupId)
        
        # Check whether there are disks that can be repaired
        for diskId in self.state.simulation.delay_repair_queue[Components.DISK]:
            disk = self.state.disks[diskId]
            # This means that we have enough bandwidth to carry out the repair
            disk_to_read_from = disks_to_read_for_repair(disk, self)
            # logging.info("Trying to initiate delayed repair for disk %s with inter-rack of %s and avail peer of %s (k=%s)", diskId, self.state.network.inter_rack_avail, len(disk_to_read_from), self.sys.top_k)
            if self.state.network.inter_rack_avail != 0 and len(disk_to_read_from) >= self.sys.top_k:
                logging.info("Delayed disk %s now has enough bandwidth, repairing", diskId)
                self.state.simulation.delay_repair_queue[Components.DISK].remove(diskId)
                return (prev_event[0], Disk.EVENT_DELAYED_FAIL, diskId)
        
        logging.info("No delayed repairs can be processed")
            
        return None