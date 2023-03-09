from __future__ import annotations
import typing

from typing import List

if typing.TYPE_CHECKING:
    from system import System

from components.spool import Spool

def slec_local_cp_layout(sys: System):
    sys.spools = []
    num_spools = sys.num_disks // sys.spool_size
    num_spools_per_rack = sys.num_disks_per_rack // sys.spool_size

    for spoolId in range(num_spools):
        spool = Spool(spoolId, sys.spool_size)
        spool.rackId = spoolId // num_spools_per_rack
        spool.diskIds = range(spoolId*sys.spool_size, (spoolId+1)*sys.spool_size)
        for diskId in spool.diskIds:
            sys.disks[diskId].spoolId = spoolId
        sys.spools.append(spool)
            
    
    # for diskId in range(sys.num_disks):
    #     print(sys.disks[diskId].spoolId)