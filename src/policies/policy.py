# from state import State
from system import System
from disk import Disk
from rack import Rack

class Policy:
    
    def __init__(self, state) -> None:
        self.state: any = state
        self.sys: System = state.sys
    
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