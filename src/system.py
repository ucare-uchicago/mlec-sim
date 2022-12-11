import numpy as np
import logging
from metrics import Metrics
from components.disk import Disk
from components.network import Network
from constants.PlacementType import PlacementType
from policies.policy_factory import config_system_layout

from typing import Dict, List
from numpy.typing import NDArray


#----------------------------
# Configuration of the system
#----------------------------

class System:
    def __init__(self, num_disks, num_disks_per_rack, k, m, place_type: PlacementType, diskCap, rebuildRate, intrarack_speed, interrack_speed,
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
            disk = Disk(diskId, diskCap, diskId // num_disks_per_rack)
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
        self.place_type: PlacementType = place_type
        self.flat_decluster_rack_layout = {}
        self.flat_cluster_rack_layout = {}
        self.net_raid_stripesets_layout = {}
        self.num_diskgroups = 0
        self.num_diskgroup_stripesets = 0
        self.diskgroup_stripesets: Dict[int, List[int]] = {}
        
        config_system_layout(self.place_type, self)
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
        #----------------------
        # initialize network
        #----------------------
        # We need to convert Gbps to GBps and then to MBps
        self.network: Network = Network(self, intrarack_speed * 1024, interrack_speed * 1024)


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)
        # __init__(self, num_disks, num_disks_per_rack, k, m, place_type, diskCap, rebuildRate,
        #             utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = False):
    sys = System(100, 10, 4, 1, PlacementType.DP ,2,1,1, 4, 1)