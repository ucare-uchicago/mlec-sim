from heapq import heappush
from components.disk import Disk
from components.rack import Rack
import logging


def slec_net_dp_repair(slec_net_dp, repair_queue):
    repair_queue.clear()
    repair_max_priority = slec_net_dp.repair_max_priority
    if repair_max_priority > 0:
        for diskId in slec_net_dp.priority_queue[repair_max_priority]:
            disk = slec_net_dp.disks[diskId]
            
            if disk.priority > 1:
                heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))

            if disk.priority == 1:
                heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))