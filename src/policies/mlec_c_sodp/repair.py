from heapq import heappush
from components.disk import Disk
from components.spool import Spool
import logging

# update the repair event queue
def mlec_c_sodp_repair(mlec_c_sodp, repair_queue):
    repair_queue.clear()
    # logging.info('affected spools: {}'.format(mlec_c_c.affected_spools))
    for spoolId in mlec_c_sodp.affected_spools:
        spool = mlec_c_sodp.spools[spoolId]
        # logging.info("spool {} failed disks {}".format(spoolId, spool.failed_disks))
        if spool.disk_max_priority <= mlec_c_sodp.sys.m:
            if spool.disk_repair_max_priority > 0:
                for diskId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    disk = mlec_c_sodp.disks[diskId]
                    if disk.priority > 1:
                        heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))

                    if disk.priority == 1:
                        heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
                        
    # logging.info("affected rack groups: {}".format(mlec_c_c.affected_rackgroups.keys()))
    for rackgroupId in mlec_c_sodp.affected_rackgroups:
        rackgroup = mlec_c_sodp.rackgroups[rackgroupId]
        # logging.info("    rackgroup.affected_mpools_in_repair: {}".format(rackgroup.affected_mpools_in_repair.keys()))
        for mpoolId in rackgroup.affected_mpools_in_repair:
            mpool = mlec_c_sodp.mpools[mpoolId]
            # logging.info("        mpool.failed_spools_in_repair: {}".format(mpool.failed_spools_in_repair.keys()))
            for spoolId in mpool.failed_spools_in_repair:
                spool = mlec_c_sodp.spools[spoolId]
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))

# def mlec_c_sodp_repair(mlec_c_sodp, repair_queue):
#     repair_queue.clear()
    
#     for spoolId in mlec_c_sodp.affected_spools:
#         spool = mlec_c_sodp.spools[spoolId]
#         if spool.state == Spool.STATE_NORMAL:
#             for diskId in spool.failed_disks:
#                 disk = mlec_c_sodp.disks[diskId]
#                 if disk.priority > 1:
#                     heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))
#                 else:
#                     heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
#         else:
#             heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))