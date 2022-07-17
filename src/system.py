import numpy as np
import random
import logging
import constants
#----------------------------
# Logging Settings
#----------------------------
logger = logging.getLogger('Trinity')
logger.setLevel('DEBUG')
#logger.addHandler(logging.FileHandler('log/trinity.log'))
logger.addHandler(logging.StreamHandler())

class System:
    def __init__(self, num_disks, num_disks_per_server, k, m, place_type, diskCap, rebuildRate, utilizeRatio, top_k = 1, top_m = 0):
        #--------------------------------------------
        # Set up the system parameters
        self.num_disks_per_server = num_disks_per_server
        self.num_disks = num_disks
        self.disks = range(num_disks)
        #--------------------------------------------
        # Set the system layout
        #--------------------------------------------
        self.servers_per_rack = {}
        self.disks_per_server = {}
        self.disks_per_rack = {}
        #--------------------------------------------
        # record servers/disks inside each rack
        #--------------------------------------------
        if num_disks % num_disks_per_server == 0:
            self.num_servers = num_disks//num_disks_per_server
        else:
            self.num_servers = num_disks//num_disks_per_server+1
        self.servers = range(self.num_servers)
        #---------------------------------------------------------
        for serverId in self.servers:
            #if serverId == num_servers -1:
                #candidate = self.disks_per_server[serverId-1] +num_disks_per_server
            if serverId == 0:
                self.disks_per_server[serverId] = np.array(range(num_disks_per_server))
                #print serverId,"check", self.disks_per_server[serverId]
            else:
                self.disks_per_server[serverId] = self.disks_per_server[serverId-1] + num_disks_per_server
                #print serverId,"check", self.disks_per_server[serverId]
        #--------------------------------------------
        # set up the erasure coding configuration
        #--------------------------------------------
        self.k = k
        self.m = m
        self.top_k = top_k
        self.top_m = top_m
        #--------------------------------------------
        self.place_type = place_type
        if place_type == constants.RAID:
            self.flat_cluster_layout()
        if place_type == constants.DP:
            self.flat_draid_layout()
        #--------------------------------------------
        self.diskSize = diskCap
        self.diskIO = rebuildRate
        self.utilizeRatio = utilizeRatio


    def flat_cluster_layout(self):
        logger.info("* flat cluster generation")
        self.flat_cluster_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            num_stripesets = len(disks_per_server) // (self.k+self.m)
            sets = []
            for i in range(num_stripesets):
                stripeset  = disks_per_server[i*(self.k+self.m) :(i+1)*(self.k+self.m)]
                sets.append(stripeset)
            self.flat_cluster_server_layout[serverId] = sets
        for serverId in self.servers:
            print("serverId {} layout: {}\n".format(serverId, self.flat_cluster_server_layout[serverId]))



    def flat_draid_layout(self):
        logger.info("* flat dRAID generation *")
        self.flat_draid_server_layout = {}
        self.stripesets_per_disk = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            sets = []
            max_groups = 128
            for i in range(max_groups):
                basePermu = random.sample(disks_per_server, self.k+self.m)
                for j in range(self.num_disks_per_server):
                    stripeset = [(x+j)%self.num_disks_per_server+self.num_disks_per_server*serverId for x in basePermu]
                    sets.append(stripeset)
                    for diskId in stripeset:
                        if diskId not in self.stripesets_per_disk:
                            self.stripesets_per_disk[diskId] = [stripeset]
                        else:
                            self.stripesets_per_disk[diskId].append(stripeset)
            self.flat_draid_server_layout[serverId] = sets





if __name__ == "__main__":
    sys = System(50, 50, 8, 2, constants.RAID, 2, 1, 1)

