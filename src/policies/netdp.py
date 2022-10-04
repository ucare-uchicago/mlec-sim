from typing import List

from state import State
from system import System
from disk import Disk
from rack import Rack
from disk import Disk

class NetDP:
    
    state: State
    sys: System
    n: int
    racks: List[Rack]
    disks: List[Disk]
    curr_time: int
    failed_disks = List[Disk]
    failed_racks: List[Rack]
    
    def __init__(self, state: State):
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
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            # Get the current priority of the disk
            curr_priority = self.disks[diskId].priority
            # Remove the priority's repair time in case we need to yield
            #  to a repair with higher priority
            del self.disks[diskId].repair_time[curr_priority]
            # Reduce priority because the disk has been repaired
            self.disks[diskId].priority -= 1
            # QUESTION: Why mark the repair start time when its a repair event?
            self.disks[diskId].repair_start_time = self.curr_time
            
            rackId = diskId // self.sys.num_disks_per_rack
            # If the rack has already failed, there is no priority update we can do anyways
            if self.racks[rackId].state == Rack.STATE_FAILED:
                return
            
            fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
            if len(fail_per_rack) > 0:
                # Ignore ADAPT for now
                for dId in fail_per_rack:
                    self.update_disk_repair_time(dId, self.disks[dId].priority, fail_per_rack)
    
    def update_disk_repair_time(self, diskId, priority, fail_per_rack):
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
            repaired_percent = 0
            # Disk capacity * percentage of chunks/data in this disk that we need to repair (for this priority)
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            
            # QUESTION: why minus the data amount?
            # Data remaining amplified by reading from k chunks, and writing to (priority - 1) chunks
            if priority > 1:
                self.sys.metrics.total_rebuild_io_per_year -= disk.curr_repair_data_remaining * (priority - 1) * self.sys.k
                
        else:
            # We calculate the repair data remaining from itself because we iteratively 
            #  "restart" the repair every iteration
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
