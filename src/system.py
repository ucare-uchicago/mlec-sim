import numpy as np
import logging
from metrics import Metrics
from components.disk import Disk
from components.network import Network
from components.rack import Rack
from components.spool import Spool
from components.mpool import Mpool
from components.rackgroup import Rackgroup

from constants.PlacementType import PlacementType
from constants.constants import kilo

from policies.policy_factory import config_system_layout

from typing import Dict, List
from numpy.typing import NDArray


#----------------------------
# Configuration of the system
#----------------------------

class System:
    def __init__(self, num_disks, num_disks_per_rack, k, m, place_type: PlacementType, diskCap, rebuildRate, intrarack_speed, interrack_speed,
                    utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = 0, num_disks_per_enclosure = -1, 
                    infinite_chunks = True, chunksize=128, spool_size=-1, repair_scheme=0, num_local_fail_to_report=-1, num_top_fail_to_report=-1,
                    collect_fail_reports = True, detection_time=0):
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
        # Set up disk parameters
        #--------------------------------------------
        self.diskSize: int = diskCap    # in MB
        self.diskIO: int = rebuildRate
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

        # todo: for dp pool size is different
        if place_type in [PlacementType.MLEC_C_C, PlacementType.MLEC_D_C, PlacementType.SLEC_LOCAL_CP]:
            self.spool_size = self.n
        elif place_type in [PlacementType.MLEC_C_D, PlacementType.MLEC_D_D, PlacementType.SLEC_LOCAL_DP]:
            self.spool_size = spool_size
        self.spools: List[Spool] = []
        self.mpools: List[Mpool] = []
        self.rackgroups: List[Rackgroup] = []

        self.rackIds: range = range(self.num_racks)
        self.racks: Dict[int, Rack] = {}
        for rackId in self.rackIds:
            self.racks[rackId] = Rack(rackId)
        for rackId in self.rackIds:
            if rackId == 0:
                self.disks_per_rack[rackId] = np.array(range(num_disks_per_rack))
            else:
                self.disks_per_rack[rackId] = self.disks_per_rack[rackId-1] + num_disks_per_rack
        #--------------------------------------------
        self.place_type: PlacementType = place_type
        self.flat_decluster_rack_layout = {}
        self.flat_cluster_rack_layout = {}
        self.num_diskgroups = 0
        self.num_diskgroup_spools = 0
        self.diskgroup_spools: Dict[int, List[int]] = {}
        
        #--------------------------------------------
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
        self.intrarack_speed = intrarack_speed / 8 * kilo
        self.interrack_speed = interrack_speed / 8 * kilo
        self.network: Network = Network(self, intrarack_speed / 8 * 1000, interrack_speed / 8 * 1000)
        # ----------
        self.infinite_chunks = infinite_chunks
        self.chunksize = chunksize
        self.num_chunks_per_disk = self.diskSize * kilo // chunksize
        self.repair_scheme = repair_scheme
        # --------
        if num_local_fail_to_report == -1:
            self.num_local_fail_to_report = m+1
        else:
            self.num_local_fail_to_report = num_local_fail_to_report
        if num_top_fail_to_report == -1:
            self.num_top_fail_to_report = top_m+1
        else:
            self.num_top_fail_to_report = num_top_fail_to_report
        self.collect_fail_reports = collect_fail_reports
        self.fail_reports = []
        self.detection_time = float(detection_time)/60/24   # convert it from in min to in days
        # ---
        config_system_layout(self.place_type, self)


if __name__ == "__main__":
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)
        # __init__(self, num_disks, num_disks_per_rack, k, m, place_type, diskCap, rebuildRate,
        #             utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = False):
    sys = System(100, 10, 4, 1, PlacementType.SLEC_LOCAL_DP ,2,1,1, 4, 1)