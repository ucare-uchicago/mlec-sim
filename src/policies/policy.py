from __future__ import annotations
import typing
from typing import Optional, Tuple
import logging

if typing.TYPE_CHECKING:
    from system import System
    from state import State

from components.disk import Disk

class Policy:
    
    def __init__(self, state: State) -> None:
        self.state: State = state
        self.sys: System = state.sys
        self.n = state.n
        self.top_n = self.sys.top_k + self.sys.top_m
        self.racks = state.racks
        self.disks = state.disks
        self.spools = state.sys.spools
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_racks = state.failed_racks
        self.mytimer = state.mytimer
        
        self.curr_time: float = state.curr_time
    
    # Default disk state update behavior
    #  Should override if there are layout specific changes (for example net_raid)
    def update_disk_state(self, event_type: str, diskId: int) -> None:
        rackId = diskId // self.sys.num_disks_per_rack
        disk = self.state.disks[diskId]
        if event_type == Disk.EVENT_REPAIR:
            logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            self.state.failed_disks.pop(diskId, None)
            self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
            
            # If this disk has network usage, we return those to the network state
            self.state.network.replenish(disk.network_usage)
            disk.network_usage = None
            
            logging.info("Network bandwidth after replenish: %s", self.state.network.__dict__)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.state.failed_disks[diskId] = 1
            self.disks[diskId].metric_down_start_time = self.curr_time
            
        if event_type == Disk.EVENT_DELAYED_FAIL:
            # Currently this should not do anything because the disk should already be in a failed state
            pass
    
    def update_disk_priority(self, event_type, diskset):
        raise NotImplementedError("update_disk_priority() not implemented")
            
    def update_rack_state(self, event_type, diskId):
        raise NotImplementedError("update_rack_state() not implemented")
    
    def update_rack_priority(self, event_type, new_failed_rack, diskId):
        raise NotImplementedError("update_rack_priority() not implemented")
    
    def update_diskgroup_state(self, event_type, diskId):
        raise NotImplementedError("update_diskgroup_state() not implemented")
    
    def update_diskgroup_priority(self, event_type, new_failed_rack, diskId):
        raise NotImplementedError("update_diskgroup_priority() not implemented")
    
    def check_pdl(self):
        raise NotImplementedError("check_pdl() not implemented")
    
    def update_repair_events(self, repair_queue):
        raise NotImplementedError("update_repair_events() not implemented")
    
    # This function decides whether the policy intervene before we check
    #   the repair_queue and failure_queue for the next event
    #   default is to return none
    def intercept_next_event(self, prev_event) -> Optional[Tuple[float, str, int]]:
        return None

    def clean_failures(self) -> None:
        failed_disks = self.state.get_failed_disks()
        for diskId in failed_disks:
            disk = self.state.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}
            self.curr_prio_repair_started = False