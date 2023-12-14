from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

from components.spool import Spool
from helpers.sodp.position import gsodp_stripeset_layout, invert_stripeset_disk


def slec_local_sodp_layout(sys: System):
    sys.spools = []
    num_spools = sys.num_disks // sys.spool_size
    num_spools_per_rack = sys.num_disks_per_rack // sys.spool_size

    raw_stripesets = gsodp_stripeset_layout(sys.spool_size, sys.k + sys.m, 0)
    num_stripesets_per_spool = len(raw_stripesets)
    assert num_stripesets_per_spool == sys.spool_size, "num_stripesets_per_spool should be equal to spool size"

    for spoolId in range(num_spools):
        spool = Spool(spoolId, sys.spool_size)
        spool.rackId = spoolId // num_spools_per_rack

        for i in range(sys.m + 1):
            spool.disk_priority_queue[i + 1] = {}

        spool.diskIds = range(spoolId*sys.spool_size, (spoolId+1)*sys.spool_size)
        for diskId in spool.diskIds:
            sys.disks[diskId].spoolId = spoolId
            sys.disks[diskId].stripesets = set()
        
        for i in range(num_stripesets_per_spool):
            raw_stripeset = raw_stripesets[i]
            stripeset = tuple([x+spoolId * num_stripesets_per_spool for x in raw_stripeset])
            stripesetId = i+spoolId * num_stripesets_per_spool
            sys.stripesets[stripesetId] = stripeset
            for diskId in stripeset:
                sys.disks[diskId].stripesets.add(stripesetId)
        sys.spools.append(spool)
    
    # print(sys.stripesets)
    # for diskId in range(11,20):
    #     print(sys.disks[diskId].stripesets)
    # print()