from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def netraid_repair(state, repair_queue):
    for diskId in state.get_failed_disks():
        rackId = diskId // state.sys.num_disks_per_rack
        if state.racks[rackId].state == Rack.STATE_NORMAL:
            # logging.info("  update_repair_event. diskId: {}".format(diskId))
            repair_time = state.disks[diskId].repair_time[0]
            #-----------------------------------------------------
            estimate_time = state.disks[diskId].repair_start_time
            estimate_time  += repair_time
            heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))