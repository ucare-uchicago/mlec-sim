from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from system import System
from components.spool import Spool

def net_raid_layout(sys: System):
    # Non-overlapping stripe sets (each member is a rack)
    num_rack_group = sys.num_racks // sys.top_n
    # How many spools in total can we have, non-overlapping disks
    num_spools = sys.num_disks_per_rack * num_rack_group
        
    sys.spools = []
    for spoolId in range(num_spools):
        
        num_spools_per_rack_group = sys.num_disks_per_rack
        rackGroupId = spoolId // num_spools_per_rack_group
        diskIds_in_spool = []
        for rackId in range(rackGroupId * sys.top_n, (rackGroupId + 1) * sys.top_n):
            diskId = rackId * num_spools_per_rack_group + spoolId % num_spools_per_rack_group
            disk = sys.disks[diskId]
            disk.rackId = rackId
            disk.spoolId = spoolId
            disk.rackGroupId = rackGroupId
            diskIds_in_spool.append(diskId)
            # logging.info(" spoolId: {} diskId: {}".format(i, diskId))
        spool = Spool(spoolId=spoolId, repair_data=-1, num_disks=sys.top_n)
        spool.rackGroupId = rackGroupId
        sys.spools.append(spool)
    
    sys.affected_spools_per_rackGroup = []
    for rackGroupId in range(num_rack_group):
        sys.affected_spools_per_rackGroup.append({})