from heapq import heappush
from components.disk import Disk
from components.rack import Rack
import logging


def netdp_repair(state, repair_queue):
    repair_queue.clear()
    priority = state.policy.max_priority
    if priority > 0:
        for diskId in state.policy.priority_queue[priority]:
            rackId = diskId // state.sys.num_disks_per_rack
            if state.racks[rackId].state == Rack.STATE_NORMAL:
                # This should be the same as flat decluster
                disk = state.disks[diskId]
                priority = disk.priority
                
                estimate_time = disk.estimate_repair_time
                if priority > 1:
                    heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                    # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                    #           Disk.EVENT_FASTREBUILD, diskId))
                if priority == 1:
                    heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                    # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                    #           Disk.EVENT_REPAIR, diskId))