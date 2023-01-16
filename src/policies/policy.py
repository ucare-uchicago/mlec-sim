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
            if disk.network_usage is not None:
                logging.info("Replenishing bandwidth")
                self.state.network.inter_rack_avail += disk.network_usage.inter_rack
                logging.info("Replenish %s inter rack", disk.network_usage.inter_rack)
                for rackId in disk.network_usage.intra_rack:
                    self.state.network.intra_rack_avail[rackId] += disk.network_usage.intra_rack[rackId]
                    logging.info("Replenish %s intrarack for rack %s", disk.network_usage.intra_rack[rackId], rackId)
            
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
