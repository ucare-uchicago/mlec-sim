from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from system import System
from components.spool import Spool
from components.rackgroup import Rackgroup

def slec_net_cp_layout(sys: System):
    sys.spool_size = sys.top_n
    num_rack_group = sys.num_racks // sys.spool_size
    # How many spools in total can we have, non-overlapping disks
    num_spools = sys.num_disks_per_rack * num_rack_group
    
    sys.rackgroups = []
    for rackgroupId in range(num_rack_group):
        rackgroup = Rackgroup(rackgroupId)
        sys.rackgroups.append(rackgroup)


    sys.spools = []
    for spoolId in range(num_spools):
        num_spools_per_rack_group = sys.num_disks_per_rack
        rackgroupId = spoolId // num_spools_per_rack_group
        diskIds_in_spool = []
        for rackId in range(rackgroupId * sys.top_n, (rackgroupId + 1) * sys.top_n):
            diskId = rackId * num_spools_per_rack_group + spoolId % num_spools_per_rack_group
            disk = sys.disks[diskId]
            disk.rackId = rackId
            disk.spoolId = spoolId
            disk.rackgroupId = rackgroupId
            diskIds_in_spool.append(diskId)
            # logging.info(" spoolId: {} diskId: {}".format(i, diskId))
        spool = Spool(spoolId=spoolId, num_disks=sys.top_n)
        spool.rackgroupId = rackgroupId
        sys.rackgroups[rackgroupId].affected_spools[spoolId] = 1
        sys.spools.append(spool)
    
    