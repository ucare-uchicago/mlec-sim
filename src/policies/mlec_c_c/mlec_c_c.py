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

        self.repairing_stripeset = []
        self.failed_diskgroups_per_stripeset = []
        self.num_diskgroup_stripesets = self.sys.num_disks // self.n // self.top_n
        for i in range(self.num_diskgroup_stripesets):
            self.failed_diskgroups_per_stripeset.append({})
        
        self.num_diskgroups_per_rack = self.sys.num_disks_per_rack // self.sys.n
        self.num_diskgroups = self.sys.num_disks // self.n
        diskgroup_repair_data = self.sys.diskSize * self.n  # when disk group fails, we repair the whole disk group
        self.diskgroups = self.sys.diskgroups
        self.affected_mlec_groups = {}

    def update_disk_state(self, event_type, diskId):
        # Print the disks using network, debug purpose only
        # disks_using_network = []
        # for diskId_ in self.disks.keys():
        #     if self.disks[diskId_].network_usage is not None:
        #         disks_using_network += [diskId_]
                
        # diskgroups_using_network = []
        # for diskgroupId_ in self.diskgroups.keys():
        #     if self.diskgroups[diskgroupId_].network_usage is not None:
        #         diskgroups_using_network += [diskgroupId_]
                
        # logging.info("Disks using network %s", disks_using_network)
        # logging.info("Diskgroups using network %s", diskgroups_using_network)
        
        diskgroupId = diskId // self.n
        rackId = self.state.disks[diskId].rackId
        # logging.info("diskId %s, diskgroupId %s, rackId %s", diskId, diskgroupId, self.disks[diskId].rackId)
        # logging.info(self.failed_diskgroups_per_stripeset)
        # logging.info("Network state - inter: %s, intra: %s", self.state.network.inter_rack_avail, self.state.network.intra_rack_avail)
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.racks[rackId].failed_disks.pop(diskId, None)
            self.diskgroups[diskgroupId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            
            self.sys.metrics.total_net_bandwidth_replenish_time += 1
            # logging.info("Replenishing network bandwidth from disk %s", self.disks[diskId].network_usage)
            self.state.network.replenish(self.disks[diskId].network_usage)
            self.disks[diskId].network_usage = None
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.diskgroups[diskgroupId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1
            
        if event_type == Disk.EVENT_DELAYED_FAIL:
            # Do nothing, same as EVENT_FAIL, but already marked as fail
            pass


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        # if disk fail event
        if event_type in [Disk.EVENT_FAIL, Disk.EVENT_DELAYED_FAIL]:
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
                if not self.state.simulation.delay_repair_queue[Components.DISK].get(failedDiskId, False):
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
                if not self.state.simulation.delay_repair_queue[Components.DISK].get(failedDiskId, False):
                    self.update_disk_repair_time(failedDiskId, fail_per_diskgroup)


    def update_disk_repair_time(self, diskId, fail_per_diskgroup):
        num_fail_per_diskgroup = len(fail_per_diskgroup)
        start = time.time()
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        # logging.info("Disk %s has repaired time of %s", diskId, repaired_time)
        
        
        update_result = None
        if repaired_time == 0:
            # This means that we just begun repair for this disk, we need to check network
            # logging.info("First time repair")
            update_result = update_network_state(disk, fail_per_diskgroup, self)
            if not update_result.can_repair:
                return
            
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        # Calculate the real repair rate by the dividing the total bandwidht used by k - that's the effectively write speed
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_diskgroup)
        
        # logging.info("Disk %s is being repaired with the speed of %s", disk.diskId, self.sys.diskIO)
        # repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_diskgroup)
        # logging.info("Repaired percent %s, Repair time %s", repaired_percent, repair_time)

        disk.repair_time[0] = repair_time / 3600 / 24

        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("calculate repair time for disk {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 diskId, repaired_time, disk.repair_time[0], disk.repair_start_time))
        # logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

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
                self.affected_mlec_groups[diskgroupStripesetId] = len(self.failed_diskgroups_per_stripeset[diskgroupStripesetId])
                return diskgroupId

        if event_type == [Diskgroup.EVENT_FAIL, Diskgroup.EVENT_DELAYED_FAIL]:
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_FAILED
            self.failed_diskgroups[diskgroupId] = 1
            self.failed_diskgroups_per_stripeset[diskgroupStripesetId][diskgroupId] = 1
            self.affected_mlec_groups[diskgroupStripesetId] = len(self.failed_diskgroups_per_stripeset[diskgroupStripesetId])
            return diskgroupId

        if event_type == Diskgroup.EVENT_REPAIR:
            # logging.info("Diskgroup %s is repaired", diskId)
            diskgroupId = diskId
            num_diskgroup_per_rack = self.sys.num_disks_per_rack // self.n
            diskgroupStripesetId = (diskgroupId % num_diskgroup_per_rack) + (diskgroupId // (num_diskgroup_per_rack * self.sys.top_n)) * num_diskgroup_per_rack
            self.diskgroups[diskgroupId].state = Diskgroup.STATE_NORMAL
            self.failed_diskgroups.pop(diskgroupId, None)
            self.failed_diskgroups_per_stripeset[diskgroupStripesetId].pop(diskgroupId, None)
            self.affected_mlec_groups[diskgroupStripesetId] = len(self.failed_diskgroups_per_stripeset[diskgroupStripesetId])
            if self.affected_mlec_groups[diskgroupStripesetId] == 0:
                self.affected_mlec_groups.pop(diskgroupStripesetId, None)
            
            fail_per_diskgroup = self.get_failed_disks_per_diskgroup(diskgroupId)
            for dId in fail_per_diskgroup:
                self.failed_disks.pop(dId, None)
                
            self.diskgroups[diskgroupId].failed_disks.clear()
            
            for dId in range(diskgroupId*self.n, (diskgroupId+1)*self.n):
                self.disks[dId].state = Disk.STATE_NORMAL
                
            # We return the network resources this diskgroup was using
            # logging.info("Replenishing network from diskgroup repair %s", self.diskgroups[diskgroupId].network_usage)
            self.state.network.replenish(self.diskgroups[diskgroupId].network_usage)
            self.diskgroups[diskgroupId].network_usage = None
            self.diskgroups[diskgroupId].yielded_network_usage = None
            
            # We unpause all the disks that yielded the resources
            for pausedDiskId in self.diskgroups[diskgroupId].paused_disks:
                self.disks[pausedDiskId].paused = False
            self.diskgroups[diskgroupId].paused_disks.clear()
            
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
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)

        if event_type == Diskgroup.EVENT_FAIL:
                self.diskgroups[diskgroupId].repair_start_time = self.curr_time
                self.diskgroups[diskgroupId].init_repair_start_time = self.curr_time
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)

        if event_type == Diskgroup.EVENT_REPAIR:
                failed_diskgroups_per_stripeset = self.get_failed_diskgroups_per_stripeset(diskgroupStripesetId)
                for dgId in failed_diskgroups_per_stripeset:
                    if not self.state.simulation.delay_repair_queue[Components.DISKGROUP].get(dgId, False):
                        self.update_diskgroup_repair_time(dgId, diskId, failed_diskgroups_per_stripeset)
    
    
    def update_diskgroup_repair_time(self, diskgroupId, diskId, failed_diskgroups_per_stripeset):
        diskgroup = self.diskgroups[diskgroupId]
        repaired_time = self.curr_time - diskgroup.repair_start_time
        if repaired_time == 0:
            # This means that we want to begun repair for this disk, we need to check network
            # Since the diskgroup has already failed, we are going to give all its member disk's network usage back to the system
            for diskId_ in self.diskgroups[diskgroupId].disks:
                if self.disks[diskId_].network_usage != None:
                    self.state.network.replenish(self.disks[diskId_].network_usage)
                    self.disks[diskId_].network_usage = None
                    self.disks[diskId_].repair_time
            
            update_result = update_network_state_diskgroup(diskgroup, failed_diskgroups_per_stripeset, self)
            if type(update_result) is bool:
                if not update_result:
                    # This means the repair cannot be started. We are delayed repair (logic handled by update_network_state_diskgroup())
                    return
            
            repaired_percent = 0
            diskgroup.curr_repair_data_remaining = diskgroup.repair_data
        else:
            repaired_percent = repaired_time / diskgroup.repair_time[0]
            diskgroup.curr_repair_data_remaining = diskgroup.curr_repair_data_remaining * (1 - repaired_percent)
    
        assert diskgroup.network_usage != None
        # Repair speed should be the minimum bandwidth from the intra-rack bandwidth used by the diskgroup
        # logging.info("Diskgroup network usage :%s", diskgroup.network_usage)
        # logging.info("Min usage: %s, diskgroup data %s", min(diskgroup.network_usage.intra_rack.values()), diskgroup.curr_repair_data_remaining)
        repair_speed = min(diskgroup.network_usage.intra_rack.values())
        repair_time = float(diskgroup.curr_repair_data_remaining)/(repair_speed / len(failed_diskgroups_per_stripeset))
            
        diskgroup.repair_time[0] = repair_time / 3600 / 24
        diskgroup.repair_start_time = self.curr_time
        diskgroup.estimate_repair_time = self.curr_time + diskgroup.repair_time[0]
        # logging.info("Diskgroup %s will be repaired at %s", diskgroupId, diskgroup.estimate_repair_time)

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
        # logging.info("Trying to intercept event")
        if (len(self.state.simulation.delay_repair_queue[Components.DISK]) == 0 \
            and len(self.state.simulation.delay_repair_queue[Components.DISKGROUP]) == 0):
            return None
        
        # # We check whether there are repaired things, if so we always process repair events before delay repair
        # if len(self.state.simulation.repair_queue) != 0 \
        #     and len(self.state.simulation.failure_queue) != 0 \
        #     and self.state.simulation.failure_queue[0][0] > self.state.simulation.repair_queue[0][0]:
        #         return None
        
        # We first check whether any waiting disk groups can be repaired
        #  We prioritize disk groups ahead of disks because they are more important
        for diskgroupId in self.state.simulation.delay_repair_queue[Components.DISKGROUP].keys():
            diskgroup = self.diskgroups[diskgroupId]
            diskgroups_to_read = diskgroup_to_read_for_repair(diskgroup.diskgroupStripesetId, self)
            logging.info("Trying to repair diskgroup %s with readable sibling %s (top_k=%s)", diskgroupId, diskgroups_to_read, self.sys.top_k)
            if len(diskgroups_to_read) >= self.sys.top_k:
                    del self.state.simulation.delay_repair_queue[Components.DISKGROUP][diskgroupId]
                    return (prev_event[0], Diskgroup.EVENT_DELAYED_FAIL, diskgroupId)
        
        # Check whether there are disks that can be repaired
        for diskId in self.state.simulation.delay_repair_queue[Components.DISK].keys():
            disk = self.state.disks[diskId]
            # This means that we have enough bandwidth to carry out the repair
            disk_to_read_from = disks_to_read_for_repair(disk, self)
            # Note: due to intra-rack repair no longer requiring bandwidth, lower-level repairs can always take place
            # used_for_top_level = used_for_repair_top_level(self, disk)
            # logging.info("Trying to initiate delayed repair for disk %s with inter-rack of %s and avail peer of %s (k=%s)", diskId, self.state.network.inter_rack_avail, len(disk_to_read_from), self.sys.top_k)
            if len(disk_to_read_from) >= self.sys.top_k:
                    # logging.info("Delayed disk %s now has enough bandwidth, repairing", diskId)
                    del self.state.simulation.delay_repair_queue[Components.DISK][diskId]
                    return (prev_event[0], Disk.EVENT_DELAYED_FAIL, diskId)
        
        # logging.info("No delayed repairs can be processed")
            
        return None

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
            self.state.network.replenish(self.diskgroups[diskgroupId].network_usage)
            self.diskgroups[diskgroupId].network_usage = None
            self.diskgroups[diskgroupId].yielded_network_usage = None
            self.diskgroups[diskgroupId].paused_disks.clear()