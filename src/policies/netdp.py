from typing import List

from system import System
from disk import Disk
from rack import Rack
from disk import Disk

import logging
import operator as op
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
            # Remove the priority's repair time in case we need to yield
            #  to a repair with higher priority
            del disk.repair_time[curr_priority]
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            # QUESTION: Why mark the repair start time when its a repair event?
            disk.repair_start_time = self.curr_time
            
            # Note: since we are considering all the disks as a flat logical pool
            #   we should consider all failed disks in the system
            #   max priority should be the max for all failures in each rack
            #   because we ensure one chunk per stripe placed in distinct rack
            
            # Get all the failed disks out from the current stripeset and the disk's current priority
            #  update the repair time
            failed_disk_system_tuple = self.state.get_failed_disks_each_rack()
            failed_disk_per_rack = failed_disk_system_tuple[0]
            max_per_rack_priority = failed_disk_system_tuple[1]
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            for dId in all_failed_disks:
                # Do not do ADAPT for now
                self.update_disk_repair_time(dId, self.disks[dId].priority, max_per_rack_priority)
                
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            
            failed_disk_system_tuple = self.state.get_failed_disks_each_rack()
            failed_disk_per_rack = failed_disk_system_tuple[0]
            max_per_rack_priority = failed_disk_system_tuple[1]
            
            logging.info("Failed disk system: %s", failed_disk_per_rack)
            logging.info("Max prio: %s", max_per_rack_priority)
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            
            # Note that we have disjoint rack placement for NET_DP
            #  - failure number should exclude the failures of the current rack
            #      failures in the current rack could not be of the same stripe
            #  - good number should only include the good ones in other rack
            fail_num = 0
            for rackId in failed_disk_per_rack.keys():
                if (rackId != disk.rackId):
                    fail_num += len(failed_disk_per_rack[rackId])
            good_num = self.sys.num_disks - len(self.sys.disks_per_rack[disk.rackId]) - fail_num
            
            logging.info("good_num %s, fail num %s", good_num, fail_num)
            
            # If the stripeset contains a disk with n priority, and now we have one more disk failure in the stripset
            #  (if the system survives), the new disk is the one that puts the stripe on the critical path, and hence
            #  we make the new disk to have the highest priority, and then give it more bandwidth        
            curr_priority = disk.priority
            max_priority = max_per_rack_priority + 1
            
            disk.priority = max_priority
            disk.repair_start_time = self.curr_time
            disk.good_num = good_num
            disk.fail_num = fail_num
            
            # Ignore ADAPT for now
            for dId in all_failed_disks:
                self.update_disk_repair_time(dId, self.disks[dId].priority, max_per_rack_priority)
        
        logging.info("-----")
    
    def update_disk_repair_time(self, diskId, priority, fail_per_stripe):
        logging.info("Updating repair time for diskId %d, prio %d", diskId, priority)
        logging.info("Disk %s", str(self.disks[diskId]))
        disk = self.disks[diskId]
        good_num = disk.good_num
        fail_num = disk.fail_num
        
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            # This means that the repair just started on a Disk.EVENT_FAIL
            # We calculate how many stripes that store data on this disk is impacted by this priority
            priority_sets = self.ncr(good_num, self.n-priority) * self.ncr(fail_num-1, priority-1)
            total_sets = self.ncr(good_num + fail_num - 1, self.n - 1)
            priority_percent = float(priority_sets) / total_sets
            logging.info("ncr(%s,%s)*ncr(%s,%s) %s*%s", good_num, self.n-priority, fail_num-1, priority-1, self.ncr(good_num, self.n-priority), self.ncr(fail_num-1, priority-1))
            logging.info("ncr(%s,%s), %s", good_num + fail_num - 1, self.n -1, self.ncr(good_num + fail_num - 1, self.n - 1))
            logging.info("Good num %s, Fail num %s, prio %s, n %s, Prio perc %s",good_num, fail_num, priority, self.n, priority_percent)
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

        parallelism = good_num        
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