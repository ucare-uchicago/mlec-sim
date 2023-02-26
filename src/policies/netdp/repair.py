from heapq import heappush
from components.disk import Disk
from components.rack import Rack
import logging


def netdp_repair(state, repair_queue):
    for diskId in state.get_failed_disks():
        rackId = diskId // state.sys.num_disks_per_rack
        if state.racks[rackId].state == Rack.STATE_NORMAL:
            # This should be the same as flat decluster
            disk = state.disks[diskId]
            estimate_time = disk.repair_start_time
            priority = disk.priority
            
            logging.info('  repair disk {}  priority {}'.format(diskId, priority))
            estimate_time  += disk.repair_time[priority]
            if priority > 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                #           Disk.EVENT_FASTREBUILD, diskId))
            if priority == 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                #           Disk.EVENT_REPAIR, diskId))