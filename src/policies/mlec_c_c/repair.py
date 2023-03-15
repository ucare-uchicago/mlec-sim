from __future__ import annotations
import typing 
import logging

from heapq import heappush, heapify
from components.disk import Disk
from components.spool import Spool

# update the repair event queue
def mlec_c_c_repair(mlec_c_c, repair_queue):
    repair_queue.clear()
    return mlec_repair_heappush(mlec_c_c, repair_queue)


# using heapify
def mlec_repair_heapify(mlec_c_c, repair_queue):
    for spoolId in mlec_c_c.affected_spools:
        spool = mlec_c_c.spools[spoolId]
        if len(spool.failed_disks) <= mlec_c_c.sys.m:
            for diskId in spool.failed_disks:
                repair_queue.append((mlec_c_c.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
        else:
            repair_queue.append((spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))
    heapify(repair_queue)



# update the repair event queue
def mlec_repair_heappush(mlec_c_c, repair_queue):
    # logging.info('affected spools: {}'.format(mlec_c_c.affected_spools))
    for spoolId in mlec_c_c.affected_spools:
        spool = mlec_c_c.spools[spoolId]
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
    # logging.info("repair queue after heappush: {}".format(repair_queue))