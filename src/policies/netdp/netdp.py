import logging

from components.disk import Disk
from policies.policy import Policy

from helpers import netdp_prio
from .pdl import network_decluster_pdl
from .repair import netdp_repair

class NetDP(Policy):
    
    def __init__(self, state):
        super().__init__(state)
        self.affected_racks = {}
        self.priority_queue = {}
        for i in range(self.sys.m+1):
            self.priority_queue[i+1] = {}
        self.max_priority = 0
        

    def update_disk_state(self, event_type: str, diskId: int) -> None:
        rackId = diskId // self.sys.num_disks_per_rack
        disk = self.state.disks[diskId]
        if event_type == Disk.EVENT_REPAIR:
            logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            if len(self.state.racks[rackId].failed_disks) == 0:
                self.affected_racks.pop(rackId, None)
            self.state.failed_disks.pop(diskId, None)
            self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
                            
        if event_type == Disk.EVENT_FAIL:
            logging.info("Fail event for disk {} with priority".format(diskId))
            disk.state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.affected_racks[rackId] = 1
            self.state.failed_disks[diskId] = 1
            self.disks[diskId].metric_down_start_time = self.curr_time
        
        if event_type == Disk.EVENT_FASTREBUILD:
            logging.info("Fast Repair event for disk {} with priority {}".format(diskId, disk.priority))
    
    def update_disk_priority(self, event_type, diskId: int):
        failed_racks = self.affected_racks.keys()
        
        disk = self.state.disks[diskId]
        rackId = diskId // self.sys.num_disks_per_rack
                
        # logging.info("Priorities: %s", priorities)
        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            # Get the current priority of the disk
            curr_priority = disk.priority
            # Remove the priority repair time and reduce priority because the disk is already repaired
            del disk.repair_time[curr_priority]
            self.priority_queue[curr_priority].pop(diskId, None)
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            if disk.priority > 0:
                self.priority_queue[disk.priority][diskId] = 1
            # QUESTION: Why mark the repair start time when its a repair event?
            # Meng: Because you now start the next repair period, which repairs a different priority with different amounot of data. 
            disk.repair_start_time = self.state.curr_time
            
            if len(self.priority_queue[self.max_priority]) == 0:
                self.max_priority -= 1
            
            # logging.info("Disk priority: %s", disk.priority)
            

            for dId in self.failed_disks:
                # Do not do ADAPT for now
                self.update_disk_repair_time(dId, self.state.disks[dId].priority, self.max_priority, rackId)
                # logging.info("===")
                
        if event_type == Disk.EVENT_FAIL:
            # Good num / failed num
            # Good num and failed num does not directly get involved in priority percent calculation
            # It it used to determine which formula to use to caluclate pp
            good_num = self.state.sys.num_disks - len(self.failed_disks)
            fail_num = self.state.sys.num_disks - good_num
            disk.good_num = good_num
            disk.fail_num = fail_num
            
            # Imagine there are 2 affected racks. And a new disk fails.
            # If the current failure happens in a brand new rack, we need to increment max_priority
            # If the current failure happens in an already affected rack, but the current max-priority is 1. 
            #     it means previous 2-chunk-failure have been repaired. But the new disk failure will lead to new 2-chunk-failure stripes.
            #     so we still need to increment max_priority
            # Otherwise, we don't increase max-priority.
            if (len(self.state.racks[rackId].failed_disks) == 1) or self.max_priority < len(self.affected_racks):
                self.max_priority += 1
            
            disk.priority = self.max_priority
            
            # logging.info("Max priority after: %s", max_priority)
            if self.max_priority > self.sys.m:
                return 1
            
            
            self.priority_queue[disk.priority][diskId] = 1

            disk.repair_start_time = self.state.curr_time
            
            # Ignore ADAPT for now
            for dId in self.failed_disks:
                self.update_disk_repair_time(dId, self.state.disks[dId].priority, self.max_priority, rackId)
                # logging.info("===")
        
    
    def update_disk_repair_time(self, diskId, priority, max_priority, rackId):
        failed_disk_per_rack = self.state.racks[rackId].failed_disks.keys()
        failed_racks = self.affected_racks.keys()
        # logging.info("Updating repair time for diskId %d, prio %d, max prio: %s", diskId, priority, max_priority)
        # logging.info("Disk %s", str(self.state.disks[diskId]))
        disk = self.state.disks[diskId]
        
        repaired_time = self.state.curr_time - disk.repair_start_time
        if repaired_time == 0:
            # This means that the repair just started on a Disk.EVENT_FAIL
            # We calculate how many stripes that store data on this disk is impacted by this priority
            priority_percent = netdp_prio.priority_percent(self.state, disk, failed_disk_per_rack, max_priority, priority)
            # logging.info("Priority percent: %s for racks: %s and disk prio: %s", priority_percent, failed_disk_per_rack, disk.priority)
            repaired_percent = 0
            # Disk capacity * percentage of chunks/data in this disk that we need to repair (for this priority)
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            
            # QUESTION: why minus the data amount?
            # Data remaining amplified by reading from k chunks, and writing to (priority - 1) chunks
            # if priority > 1:
            #     self.sys.metrics.total_rebuild_io_per_year -= disk.curr_repair_data_remaining * (priority - 1) * self.sys.k
                
        else:
            # We calculate the repair data remaining from itself because we iteratively 
            #  "restart" the repair every iteration by setting new repair start time to curr_time
            # print(disk.repair_time)
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)

        
        repair_time = self.calc_repair_time(disk, failed_racks, priority)
        
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.state.curr_time
        disk.estimate_repair_time = self.state.curr_time + disk.repair_time[priority]
        
        # logging.info("Time needed for repair %s d, will be repaired at day %s", (repair_time / 3600 / 24), disk.estimate_repair_time)
        
    def calc_repair_time(self, disk, failed_racks, priority):
        # For read parallelism, we need to sum all the surviving disks outside of the impacted racks
        # Note that this is an assumption made to reduce the complexity of the simulation
        # parallelism = disk.good_num / (self.sys.k + 1)
        # parallelism = self.calc_parallelism(failed_racks)
        
        # amplification = self.sys.k + priority        
        
        # if priority < failed_racks:
        #     # This means that we need to yield bandwidth to the disks with higher priority
        #     repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/failed_racks)
        # else:
        #     repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
            
        # logging.info("Failed racks %s", failed_racks)
        # logging.info("Parallelism: %s, amplification: %s, data: %s, diskIO: %s", parallelism, amplification, disk.curr_repair_data_remaining, self.sys.diskIO)
        # return repair_time
        
        # We calculate the repair time for RAID, and divide that by speed up brought by net dp
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO)
        
        participating_disks = disk.good_num
        speed_up = participating_disks / (self.sys.k + 1)
        
        return repair_time / speed_up
    
    def calc_parallelism(self, failed_racks, exp=False):
        # return (len(self.racks) - failed_racks) * self.sys.num_disks_per_rack
        return self.sys.num_disks - len(self.state.failed_disks)
    
    def check_pdl(self):
        return network_decluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        return netdp_repair(self.state, repair_queue)