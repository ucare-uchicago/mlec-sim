from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def decluster_repair(state, repair_queue):
    repair_queue.clear()
    for diskId in state.get_failed_disks():
        rackId = diskId // state.sys.num_disks_per_rack
        if state.racks[rackId].state == Rack.STATE_NORMAL:
            disk = state.disks[diskId]
            estimate_time = disk.repair_start_time
            priority = disk.priority
            estimate_time  += disk.repair_time[priority]
            if priority > 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                #           Disk.EVENT_FASTREBUILD, diskId))
            if priority == 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                #           Disk.EVENT_REPAIR, diskId))