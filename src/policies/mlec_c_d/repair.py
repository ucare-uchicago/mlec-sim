from heapq import heappush
from components.disk import Disk
from components.spool import Spool

def mlec_c_d_repair(mlec_c_d, repair_queue):
    repair_queue.clear()
    
    for spoolId in mlec_c_d.affected_spools:
        spool = mlec_c_d.spools[spoolId]
        if spool.state == Spool.STATE_NORMAL:
            for diskId in spool.failed_disks:
                disk = mlec_c_d.disks[diskId]
                if disk.priority > 1:
                    heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))
                else:
                    heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
        else:
            heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))