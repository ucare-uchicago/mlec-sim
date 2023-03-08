from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

def slec_local_cp_layout(sys: System):
    sys.flat_cluster_rack_layout = {}
    for rackId in sys.rackIds:
        disks_per_rack = sys.disks_per_rack[rackId]
        num_spools = len(disks_per_rack) // (sys.k+sys.m)
        sets = []
        for i in range(num_spools):
            spool  = disks_per_rack[i*(sys.k+sys.m) :(i+1)*(sys.k+sys.m)]
            sets.append(spool)
        sys.flat_cluster_rack_layout[rackId] = sets
    
        # logging.info("* rack {} has {} spools".format(rackId, num_spools))
    #for rackId in self.racks:
    #    print "rackId", rackId, len(self.flat_cluster_rack_layout[rackId])