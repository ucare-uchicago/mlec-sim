import numpy as np
import logging
from metrics import Metrics
from disk import Disk

from typing import Dict, List
from numpy.typing import NDArray


#----------------------------
# Configuration of the system
#----------------------------

class System:
    def __init__(self, num_disks, num_disks_per_rack, k, m, place_type, diskCap, rebuildRate,
                    utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = 0, num_disks_per_enclosure = -1):
        #--------------------------------------------
        # Set up the system parameters
        #--------------------------------------------
        self.num_disks_per_rack: int = num_disks_per_rack
        #--------------------------------------------
        # set up the system racks, disks
        #--------------------------------------------
        self.num_disks: int = num_disks
        self.disks: Dict[int, Disk] = {}
        for diskId in range(num_disks):
            disk = Disk(diskId, diskCap)
            self.disks[diskId] = disk
        #--------------------------------------------
        # Set the system system layout
        #--------------------------------------------
        self.disks_per_rack: Dict[int, NDArray] = {}
        #--------------------------------------------
        # record racks/disks inside each rack
        #--------------------------------------------
        if (num_disks//num_disks_per_rack) * num_disks_per_rack == num_disks:
            self.num_racks = self.num_disks//self.num_disks_per_rack
        else:
            self.num_racks = self.num_disks//self.num_disks_per_rack+1
        self.racks: range = range(self.num_racks)
        for rackId in self.racks:
            if rackId == 0:
                self.disks_per_rack[rackId] = np.array(range(num_disks_per_rack))
            else:
                self.disks_per_rack[rackId] = self.disks_per_rack[rackId-1] + num_disks_per_rack
        #--------------------------------------------
        # set up the erasure coding configuration
        #--------------------------------------------
        self.k: int = k
        self.m: int = m
        self.n: int = m + k
        self.top_k: int = top_k
        self.top_m: int = top_m
        self.top_n: int = top_m + top_k
        #--------------------------------------------
        self.place_type = place_type
        if place_type == 0:
            self.flat_cluster_layout()
        elif place_type == 1:
            self.flat_decluster_layout()
        elif place_type == 2:
            self.mlec_cluster_layout()
        elif place_type == 3:
            self.net_raid_layout()
        elif place_type == 4:
            self.mlec_dp_layout()
        elif place_type == 5:
            self.net_dp_layout()
        else:
            raise NotImplementedError("The placment type does not have a defined layout")
        #--------------------------------------------
        self.diskSize: int = diskCap
        self.diskIO: int = rebuildRate
        self.utilizeRatio: float = utilizeRatio
        self.adapt: bool = adapt
        self.rack_fail: int = rack_fail
        self.metrics: Metrics = Metrics()
        # ----------
        if num_disks_per_enclosure == -1:
            self.num_disks_per_enclosure: int = self.num_disks_per_rack
        else:
            self.num_disks_per_enclosure: int = num_disks_per_enclosure


    def flat_cluster_layout(self):
        self.flat_cluster_rack_layout = {}
        for rackId in self.racks:
            disks_per_rack = self.disks_per_rack[rackId]
            num_stripesets = len(disks_per_rack) // (self.k+self.m)
            sets = []
            for i in range(num_stripesets):
                stripeset  = disks_per_rack[i*(self.k+self.m) :(i+1)*(self.k+self.m)]
                sets.append(stripeset)
            self.flat_cluster_rack_layout[rackId] = sets
            # logging.info("* rack {} has {} stripesets".format(rackId, num_stripesets))
        #for rackId in self.racks:
        #    print "rackId", rackId, len(self.flat_cluster_rack_layout[rackId])


    
    def flat_decluster_layout(self):
        self.flat_decluster_rack_layout = {}
        for rackId in self.racks:
            disks_per_rack = self.disks_per_rack[rackId]
            self.flat_decluster_rack_layout[rackId] = disks_per_rack


    def mlec_dp_layout(self):
        self.flat_decluster_rack_layout = {}
        for rackId in self.racks:
            disks_per_rack = self.disks_per_rack[rackId]
            self.flat_decluster_rack_layout[rackId] = disks_per_rack

    def net_dp_layout(self):
        # Same as flat decluster
        self.flat_decluster_rack_layout = {}
        for rackId in self.racks:
            disks_per_rack = self.disks_per_rack[rackId]
            self.flat_decluster_rack_layout[rackId] = disks_per_rack

        for diskId in self.disks:
            self.disks[diskId].diskId = diskId
            self.disks[diskId].rackId = diskId // self.num_disks_per_rack

    # layout for mlec cluster raid
    def mlec_cluster_layout(self):
        # In network level, we form top_n diskgroups into a diskgroup_stripeset
        # 
        self.top_n = self.top_k + self.top_m
        self.num_diskgroups = self.num_disks // self.n
        self.num_diskgroup_stripesets = self.num_diskgroups // self.top_n
        self.diskgroup_stripesets = []
        # print(self.rack_stripesets)
        # print(self.stripesets_per_racks)
        
    def net_raid_layout(self):
        stripe_width = self.top_k + self.top_m
        num_rack_group = self.num_racks // stripe_width
        num_stripesets = self.num_disks_per_rack * num_rack_group
        
        
        sets = {}
        for i in range(num_stripesets):
            
            num_stripesets_per_rack_group = self.num_disks_per_rack
            rackGroupId = i // num_stripesets_per_rack_group
            stripeset = []
            for rackId in range(rackGroupId*stripe_width, (rackGroupId+1)*stripe_width):
                diskId = rackId * num_stripesets_per_rack_group + i % num_stripesets_per_rack_group
                disk = self.disks[diskId]
                disk.rackId = rackId
                disk.stripesetId = i
                stripeset.append(diskId)
                # logging.info(" stripesetId: {} diskId: {}".format(i, diskId))
            sets[i] = stripeset
        self.net_raid_stripesets_layout = sets
        logging.info("* there are {} stripesets:\n{}".format(
                num_stripesets, sets))

if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)
        # __init__(self, num_disks, num_disks_per_rack, k, m, place_type, diskCap, rebuildRate,
        #             utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = False):
    sys = System(100, 10, 4, 1, 2,2,1,1, 4, 1)

