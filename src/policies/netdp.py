from typing import List

from system import System
from disk import Disk
from rack import Rack
from disk import Disk

import logging
import operator as op
import numpy as np
from helpers.netdp_prio import priority_percent
from functools import reduce

class NetDP:
    
    state: any # State
    sys: System
    n: int
    racks: List[Rack]
    disks: List[Disk]
    curr_time: int
    failed_disks = List[Disk]
    failed_racks: List[Rack]
    
    def __init__(self, state):
        self.state = state
        self.sys = state.sys
        self.n = state.n
        self.racks = state.racks
        self.disks = state.disks
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_racks = state.failed_racks
    
    # Everytime we pop an event of the message queue we call this to update the state of the disk
    def update_disk_state(self, event_type, diskId: int):
        rackId = diskId // self.sys.num_disks_per_rack
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.racks[rackId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1
    
    def update_disk_priority(self, event_type, diskId: int):
        logging.info("Event %s, dID %s", event_type, diskId)
        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            disk = self.disks[diskId]
            # Get the current priority of the disk
            curr_priority = disk.priority
            # Remove the priority repair time and reduce priority because the disk is already repaired
            del disk.repair_time[curr_priority]
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            # QUESTION: Why mark the repair start time when its a repair event?
            disk.repair_start_time = self.curr_time
            
            # Note that the max priority should be number of racks that are impacted
            # Get all the failed disks out from the current stripeset and the disk's current priority
            #  update the repair time
            failed_disk_system_tuple = self.state.get_failed_disks_each_rack()
            failed_disk_per_rack = failed_disk_system_tuple[0]
            failed_racks = failed_disk_system_tuple[1]
            
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            for dId in all_failed_disks:
                # Do not do ADAPT for now
                self.update_disk_repair_time(dId, self.disks[dId].priority, failed_racks, failed_disk_system_tuple)
                
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            
            failed_disk_system_tuple = self.state.get_failed_disks_each_rack()
            failed_disk_per_rack = failed_disk_system_tuple[0]
            failed_racks = failed_disk_system_tuple[1]
            
            logging.info("Failed disk system: %s", failed_disk_per_rack)
            logging.info("Max prio: %s", failed_racks)
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            
            
            # Good num / failed num
            # Good num and failed num does not directly get involved in priority percent calculation
            # It it used to determine which formula to use to caluclate pp
            good_num = self.state.sys.num_disks - np.sum(failed_disk_per_rack.keys())
            fail_num = self.state.sys.num_disks - good_num
            disk.good_num = good_num
            disk.fail_num = fail_num
            
            # We need to check if the disk failed in a previously unimpacted rack
            # If so, the priority is failed_racks + 1
            # Otherwise, the priority remains failed_racks because failures within an impacted rack does not affect priotiy 
            if (len(failed_disk_per_rack[disk.rackId]) == 1):
                disk.priority = failed_racks + 1
            else:
                disk.priority = failed_racks
            
            disk.repair_start_time = self.curr_time
            
            # Ignore ADAPT for now
            for dId in all_failed_disks:
                self.update_disk_repair_time(dId, self.disks[dId].priority, failed_racks, failed_disk_system_tuple)
        
        logging.info("-----")
    
    def update_disk_repair_time(self, diskId, priority, fail_per_stripe, failed_disk_system_tuple):
        failed_disk_per_rack = failed_disk_system_tuple[0]
        failed_racks = failed_disk_system_tuple[1]
        logging.info("Updating repair time for diskId %d, prio %d", diskId, priority)
        logging.info("Disk %s", str(self.disks[diskId]))
        disk = self.disks[diskId]
        
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            # This means that the repair just started on a Disk.EVENT_FAIL
            # We calculate how many stripes that store data on this disk is impacted by this priority
            priority_percent = priority_percent(len(self.state.racks), len(self.state.disks)/len(self.state.racks), self.state.n, disk, failed_disk_per_rack, failed_racks)
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

        # For read parallelism, we need to sum all the surviving disks outside of the impacted racks
        # Note that this is an assumption made to reduce the complexity of the simulation
        parallelism = 0
        for rackId, disks in failed_disk_per_rack:
            if (len(failed_disk_per_rack[rackId]) == 0):
                parallelism += len(self.state.disks)/len(self.state.racks)
            
        amplification = self.sys.k + priority
        
        logging.info("Fail per stripe %s", fail_per_stripe)
        if priority < fail_per_stripe:
            # This means that we need to yield bandwidth to the disks with higher priority
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_stripe)
        else:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
            
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[priority]
        
    def ncr(self, n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        denom = reduce(op.mul, range(1, r+1), 1)
        return numer / denom