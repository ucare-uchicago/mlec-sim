import numpy as np
import random
import logging
from rack import Rack
from metrics import Metrics
from disk import Disk



#----------------------------
# Configuration of the system
#----------------------------

class System:
    def __init__(self, num_disks, num_disks_per_rack, k, m, place_type, diskCap, rebuildRate,
                    utilizeRatio, top_k = 1, top_m = 0, adapt = False, rack_fail = False):
        #--------------------------------------------
        # Set up the system parameters
        #--------------------------------------------
        self.num_disks_per_rack = num_disks_per_rack
        #--------------------------------------------
        # set up the system racks, disks
        #--------------------------------------------
        self.num_disks = num_disks
        self.disks = {}
        for diskId in range(num_disks):
            self.disks[diskId] = Disk(diskId, diskCap)
        #--------------------------------------------
        # Set the system system layout
        #--------------------------------------------
        self.disks_per_rack = {}
        #--------------------------------------------
        # record racks/disks inside each rack
        #--------------------------------------------
        if (num_disks//num_disks_per_rack) * num_disks_per_rack == num_disks:
            self.num_racks = self.num_disks//self.num_disks_per_rack
        else:
            self.num_racks = self.num_disks//self.num_disks_per_rack+1
        self.racks = range(self.num_racks)
        for rackId in self.racks:
            if rackId == 0:
                self.disks_per_rack[rackId] = np.array(range(num_disks_per_rack))
            else:
                self.disks_per_rack[rackId] = self.disks_per_rack[rackId-1] + num_disks_per_rack
        #--------------------------------------------
        # set up the erasure coding configuration
        #--------------------------------------------
        self.k = k
        self.m = m
        self.top_k = top_k
        self.top_m = top_m
        #--------------------------------------------
        self.place_type = place_type
        if place_type == 0:
            self.flat_cluster_layout()
        if place_type == 1:
            self.flat_decluster_layout()
        if place_type == 2:
            self.mlec_cluster_layout()
        if place_type == 3:
            self.net_raid_layout()
        if place_type == 4:
            self.mlec_dp_layout()
        if place_type == 5:
            self.net_dp_layout()
        #--------------------------------------------
        self.diskSize = diskCap
        self.diskIO = rebuildRate
        self.utilizeRatio = utilizeRatio
        self.adapt = adapt
        self.rack_fail = rack_fail
        self.metrics = Metrics()


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



    # same as flat_cluster_layout
    def mlec_cluster_layout(self):
        self.rack_stripesets = []
        self.top_n = self.top_k + self.top_m
        self.num_rack_stripesets = self.num_racks // self.top_n
        for i in range(self.num_rack_stripesets):
            # rack_stripeset  = self.racks[i*self.top_n :(i+1)*self.top_n]
            rack_stripeset = []
            for j in range(self.top_n):
                rack_stripeset.append(i+j*self.num_rack_stripesets)
            self.rack_stripesets.append(rack_stripeset)
        




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
    sys = System(100, 10, 4, 1, 2,2,1,1)

