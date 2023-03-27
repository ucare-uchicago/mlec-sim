from heapq import heappush
from components.disk import Disk
from components.spool import Spool

def mlec_d_c_repair(mlec_d_c, repair_queue):
    repair_queue.clear()
    repair_max_priority = mlec_d_c.repair_max_priority
    if repair_max_priority > 0:
        for spoolId in mlec_d_c.priority_queue[repair_max_priority]:
            spool = mlec_d_c.spools[spoolId]
            if spool.priority > 1:
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_FASTREBUILD, spoolId))

            if spool.priority == 1:
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))
    
    for spoolId in mlec_d_c.affected_spools:
        spool = mlec_d_c.spools[spoolId]
        if len(spool.failed_disks) <= mlec_d_c.sys.m:
            for diskId in spool.failed_disks_in_repair:
                heappush(repair_queue, (mlec_d_c.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
    return