from heapq import heappush
from components.disk import Disk
from components.spool import Spool
import logging

# update the repair event queue
def mlec_c_d_repair(mlec_c_c, repair_queue):
    repair_queue.clear()
    # logging.info('affected spools: {}'.format(mlec_c_c.affected_spools))
    for spoolId in mlec_c_c.affected_spools:
        spool = mlec_c_c.spools[spoolId]
        # logging.info("spool {} failed disks {}".format(spoolId, spool.failed_disks))
        if len(spool.failed_disks) <= mlec_c_c.sys.m:
            for diskId in spool.failed_disks_in_repair:
                heappush(repair_queue, (mlec_c_c.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
    
    # logging.info("affected rack groups: {}".format(mlec_c_c.affected_rackgroups.keys()))
    for rackgroupId in mlec_c_c.affected_rackgroups:
        rackgroup = mlec_c_c.rackgroups[rackgroupId]
        # logging.info("    rackgroup.affected_mpools_in_repair: {}".format(rackgroup.affected_mpools_in_repair.keys()))
        for mpoolId in rackgroup.affected_mpools_in_repair:
            mpool = mlec_c_c.mpools[mpoolId]
            # logging.info("        mpool.failed_spools_in_repair: {}".format(mpool.failed_spools_in_repair.keys()))
            for spoolId in mpool.failed_spools_in_repair:
                spool = mlec_c_c.spools[spoolId]
                heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))

# def mlec_c_d_repair(mlec_c_d, repair_queue):
#     repair_queue.clear()
    
#     for spoolId in mlec_c_d.affected_spools:
#         spool = mlec_c_d.spools[spoolId]
#         if spool.state == Spool.STATE_NORMAL:
#             for diskId in spool.failed_disks:
#                 disk = mlec_c_d.disks[diskId]
#                 if disk.priority > 1:
#                     heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))
#                 else:
#                     heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
#         else:
#             heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))