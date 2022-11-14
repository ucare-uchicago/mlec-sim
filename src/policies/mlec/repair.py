from heapq import heappush
from components.disk import Disk
from components.diskgroup import Diskgroup

# def mlec_repair(diskId, rackId, state):
#     return (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId)


# update the repair event queue
def mlec_repair(diskgroups, failed_diskgroups, state, repair_queue):
    for diskgroupId in failed_diskgroups:
        heappush(repair_queue, (diskgroups[diskgroupId].estimate_repair_time, Diskgroup.EVENT_REPAIR, diskgroupId))
    
    for diskId in state.get_failed_disks():
        diskgroupId = diskId // state.sys.n
        if diskgroups[diskgroupId].state == Diskgroup.STATE_NORMAL:
            heappush(repair_queue, (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))