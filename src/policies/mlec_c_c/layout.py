from __future__ import annotations
import typing
from typing import List
import numpy as np
from typing import List, Dict, Optional, Tuple

from components.diskgroup import Diskgroup
from components.spool import Spool
from components.mpool import Mpool
from components.rackgroup import Rackgroup

if typing.TYPE_CHECKING:
    from system import System
    
# let's say we do (2+1)/(2+1). Let say we have 6 disks per rack. And we have 6 racks
# Then each rack will have 2 disk groups.
# (0,1), (2,3), (4,5), (6,7), (8,9), (10,11) so we have in total 12 disk groups.
# we do network erasure between disk groups.
# so the disk group spools will be:
# (0,2,4), (1,3,5), (6,8,10), (7,9,11)
# we want to know the disk group spool id for a centain disk group. 
# Let's valiadate if the formula below is correct
# let's check diskgroup 11:
# diskgroupSpoolId = (11 % 2) + (11 // (2*3)) * 2 = 1 + (1*2) = 1+2 = 3
# let's check disgroup 3:
# diskgroupSpoolId = (3 % 2) + (3 // (2*3)) * 2 = 1 + (0*2) = 1+0 = 1

# layout for mlec cluster raid
def mlec_c_c_layout(sys: System):
    # In network level, we form top_n spools into a mpool
    sys.num_spools = sys.num_disks // sys.spool_size
    sys.num_mpools = sys.num_spools // sys.top_n
    sys.num_rackgroups = sys.num_racks // sys.top_n
    sys.rackgroups = {}
    sys.mpools = {}
    sys.spools = {}
    
    # Initialize spool layout
    num_spools_per_rack = sys.num_disks_per_rack // sys.n
    num_spools_per_rackgroup = num_spools_per_rack * sys.top_n
    num_mpools_per_rackgroup = sys.num_mpools // sys.num_rackgroups
    

    for rackgroupId in range(sys.num_rackgroups):
        rackgroup = Rackgroup(rackgroupId)
        for mpoolId in range(rackgroupId*num_mpools_per_rackgroup, (rackgroupId+1)*num_mpools_per_rackgroup):
            mpool = Mpool(mpoolId)
            mpool.rackgroupId = rackgroupId
            for i in range(sys.top_n):
                spoolId = rackgroupId*num_spools_per_rackgroup + i*num_spools_per_rack + mpoolId%num_mpools_per_rackgroup
                mpool.spoolIds.append(spoolId)
                spool = Spool(spoolId=spoolId, num_disks=sys.spool_size)
                spool.mpoolId = mpoolId
                spool.rackgroupId = rackgroupId
                spool.diskIds = range(spoolId*sys.spool_size, (spoolId+1)*sys.spool_size)
                for diskId in spool.diskIds:
                    sys.disks[diskId].spoolId = spoolId
                sys.spools[spoolId] = spool
            rackgroup.mpoolIds.append(mpoolId)
            sys.mpools[mpoolId] = mpool
        sys.rackgroups[rackgroupId] = rackgroup

    
    # print(sys.rackgroups.keys())
    # for rackgroupId in range(sys.num_rackgroups):
    #     rackgroup = sys.rackgroups[rackgroupId]
    #     print(rackgroup.mpoolIds)
    #     for mpoolId in rackgroup.mpoolIds:
    #         mpool = sys.mpools[mpoolId]
    #         print('  {}'.format(mpool.spoolIds))
    #         for spoolId in mpool.spoolIds:
    #             spool = sys.spools[spoolId]
    #             print('    {}'.format(spool.diskIds))