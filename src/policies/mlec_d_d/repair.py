from heapq import heappush
from components.disk import Disk
from components.spool import Spool

def mlec_d_d_repair(mlec_d_d, repair_queue):
    repair_queue.clear()
    repair_max_priority = mlec_d_d.repair_max_priority
    if repair_max_priority > 0:
        for spoolId in mlec_d_d.priority_queue[repair_max_priority]:
            spool = mlec_d_d.spools[spoolId]
            if spool.priority > 1:
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_FASTREBUILD, spoolId))

            if spool.priority == 1:
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))
    
    for spoolId in mlec_d_d.affected_spools:
        spool = mlec_d_d.spools[spoolId]
        if spool.disk_max_priority <= mlec_d_d.sys.m:
            if spool.disk_repair_max_priority > 0:
                for diskId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    disk = mlec_d_d.disks[diskId]
                    if disk.priority > 1:
                        heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))

                    if disk.priority == 1:
                        heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
    return