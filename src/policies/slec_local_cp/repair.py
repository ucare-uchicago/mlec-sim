from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def raid_repair(state, repair_queue):
    for diskId in state.get_failed_disks():
        rackId = diskId // state.sys.num_disks_per_rack
        if state.racks[rackId].state == Rack.STATE_NORMAL:
            repair_time = state.disks[diskId].repair_time[0]
            #-----------------------------------------------------
            estimate_time = state.disks[diskId].repair_start_time
            estimate_time  += repair_time
            heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
            # logging.debug("--------> push ", repair_time, estimate_time, Disk.EVENT_REPAIR, 
            #             "D-",diskId,"-", "S-",diskId/84, "R-",diskId/504)