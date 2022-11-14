from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

def flat_cluster_layout(sys: System):
    sys.flat_cluster_rack_layout = {}
    for rackId in sys.racks:
        disks_per_rack = sys.disks_per_rack[rackId]
        num_stripesets = len(disks_per_rack) // (sys.k+sys.m)
        sets = []
        for i in range(num_stripesets):
            stripeset  = disks_per_rack[i*(sys.k+sys.m) :(i+1)*(sys.k+sys.m)]
            sets.append(stripeset)
        sys.flat_cluster_rack_layout[rackId] = sets
    
        # logging.info("* rack {} has {} stripesets".format(rackId, num_stripesets))
    #for rackId in self.racks:
    #    print "rackId", rackId, len(self.flat_cluster_rack_layout[rackId])