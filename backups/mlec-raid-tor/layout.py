from __future__ import annotations
import typing
from typing import List
import numpy as np
from typing import List, Dict, Optional, Tuple

from components.diskgroup import Diskgroup

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
def mlec_cluster_layout(sys: System):
    # In network level, we form top_n diskgroups into a diskgroup_spool
    sys.n = sys.k + sys.m
    sys.top_n = sys.top_k + sys.top_m
    sys.num_diskgroups = sys.num_disks // sys.n
    sys.num_diskgroup_spools = sys.num_diskgroups // sys.top_n
    sys.diskgroup_spools = {}
    # print(self.rack_spools)
    # print(self.spools_per_racks)
    
    # Initialize diskgroup spool layout
    diskgroup_each_rack = sys.num_disks_per_rack // sys.n
    diskgroups: List[List[int]] = np.arange(0, sys.num_diskgroups).reshape((-1, diskgroup_each_rack, )).tolist()
    
    # print(diskgroups)
    diskgroup_spools = {}
    for spoolId in range(sys.num_diskgroup_spools):
        spool = []
        # print(diskgroups)
        # For each spool, we need to select sys.top_n diskgroups
        for diskgroupId in range(sys.top_n):
            spool.append(diskgroups[diskgroupId].pop(0))
        # After we pop, we remove those diskgroup banks that are already empty
        diskgroups = list(filter(lambda a: len(a) != 0, diskgroups))
        diskgroup_spools[spoolId] = spool
    
    sys.diskgroup_spools = diskgroup_spools
