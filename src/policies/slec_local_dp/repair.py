from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def slec_local_dp_repair(slec_local_dp, repair_queue):
    repair_queue.clear()

    for spoolId in slec_local_dp.affected_spools:
        spool = slec_local_dp.spools[spoolId]
        if spool.disk_repair_max_priority > 0:
            for diskId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                disk = slec_local_dp.disks[diskId]
                if disk.priority > 1:
                    heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))

                if disk.priority == 1:
                    heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))