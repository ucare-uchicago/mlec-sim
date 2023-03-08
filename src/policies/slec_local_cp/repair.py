from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def slec_local_cp_repair(slec_local_cp, repair_queue):
    for diskId in slec_local_cp.failed_disks:
        disk = slec_local_cp.disks[diskId]
        heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))