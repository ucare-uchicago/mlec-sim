from system import System
from components.disk import Disk
from constants.PlacementType import PlacementType

from typing import Tuple, Optional


class Policy:
    
    def __init__(self, state) -> None:
        self.state = state
        self.sys: System = state.sys
        
        self.curr_time: float = state.curr_time
    
    # Default disk state update behavior
    #  Should override if there are layout specific changes (for example net_raid)
    def update_disk_state(self, event_type: str, diskId: int) -> None:
        rackId = diskId // self.sys.num_disks_per_rack
        if event_type == Disk.EVENT_REPAIR:
            self.state.disks[diskId].state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            self.state.failed_disks.pop(diskId, None)
            
        if event_type == Disk.EVENT_FAIL:
            self.state.disks[diskId].state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.state.failed_disks[diskId] = 1
    
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
