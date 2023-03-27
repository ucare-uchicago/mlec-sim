from __future__ import annotations
import typing

from components.spool import Spool

if typing.TYPE_CHECKING:
    from system import System

def mlec_d_c_layout(sys: System):
    sys.num_spools = sys.num_disks // sys.spool_size
    sys.spools = {}
    
    # Initialize spool layout
    sys.num_spools_per_rack = sys.num_disks_per_rack // sys.spool_size
    
    for spoolId in range(sys.num_spools):
        spool = Spool(spoolId=spoolId, num_disks=sys.spool_size)
        spool.diskIds = range(spoolId*sys.spool_size, (spoolId+1)*sys.spool_size)
        spool.rackId = spoolId // sys.num_spools_per_rack
        for diskId in spool.diskIds:
            sys.disks[diskId].spoolId = spoolId
        sys.spools[spoolId] = spool


    # for spoolId in sys.spools:
    #     spool = sys.spools[spoolId]
    #     print("spool {}  rackid {}  disks: {}".format(spoolId, spool.rackId, spool.diskIds))
    return