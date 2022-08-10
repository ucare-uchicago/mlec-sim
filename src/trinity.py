import numpy as np
import random
from newposition import Position
import logging
from server import Server
from metrics import Metrics
from disk import Disk
#----------------------------
# Logging Settings
#----------------------------

class Trinity:
    def __init__(self, num_disks, num_disks_per_server, k, m, place_type, diskCap, rebuildRate,
                    utilizeRatio, top_k = 1, top_m = 0, adapt = False, server_fail = False):
        #--------------------------------------------
        # Set up the Campaign system parameters
        #--------------------------------------------
        #self.num_racks = num_racks
        #self.num_servers_per_rack = num_servers_per_rack
        logging.debug("TRINITY NUM_DISKS_PER_SERVER: " + str(num_disks_per_server))
        self.num_disks_per_server = num_disks_per_server
        #self.num_disks_per_rack = num_disks_per_server * num_servers_per_rack
        #self.num_servers = num_disks / num_servers_per_rack
        #self.num_disks = self.num_servers * num_disks_per_server
        #--------------------------------------------
        # set up the Trinity racks, servers, disks
        #--------------------------------------------
        #self.racks = range(self.num_racks)
        #self.servers = range(self.num_servers)
        self.num_disks = num_disks
        self.disks = {}
        for diskId in range(num_disks):
            self.disks[diskId] = Disk(diskId, diskCap)
        #--------------------------------------------
        # Set the Trinity system layout
        #--------------------------------------------
        self.servers_per_rack = {}
        self.disks_per_server = {}
        self.disks_per_rack = {}
        #--------------------------------------------
        # record servers/disks inside each rack
        #--------------------------------------------
        if (num_disks//num_disks_per_server) * num_disks_per_server == num_disks:
            self.num_servers = self.num_disks//self.num_disks_per_server
        else:
            self.num_servers = self.num_disks//self.num_disks_per_server+1
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
        if place_type == 0:
            self.flat_cluster_layout()
        if place_type == 1:
            self.flat_decluster_layout()
        if place_type == 2:
            self.mlec_cluster_layout()
        if place_type == 3:
            self.net_raid_layout()
        #--------------------------------------------
        self.diskSize = diskCap
        self.diskIO = rebuildRate
        self.utilizeRatio = utilizeRatio
        self.adapt = adapt
        self.server_fail = server_fail
        self.metrics = Metrics()


    def flat_cluster_layout(self):
        logging.debug("* flat cluster generation")
        self.flat_cluster_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            num_stripesets = len(disks_per_server) // (self.k+self.m)
            sets = []
            for i in range(num_stripesets):
                stripeset  = disks_per_server[i*(self.k+self.m) :(i+1)*(self.k+self.m)]
                sets.append(stripeset)
            self.flat_cluster_server_layout[serverId] = sets
            logging.info("* server {} has {} stripesets".format(serverId, num_stripesets))
        #for serverId in self.servers:
        #    print "serverId", serverId, len(self.flat_cluster_server_layout[serverId])


    
    def flat_decluster_layout(self):
        logging.debug("* flat decluster generation *")
        self.flat_decluster_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            self.flat_decluster_server_layout[serverId] = disks_per_server






    # same as flat_cluster_layout
    def mlec_cluster_layout(self):
        logging.debug("* mlec cluster generation")
        self.flat_cluster_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            num_stripesets = len(disks_per_server) // (self.k+self.m)
            sets = []
            for i in range(num_stripesets):
                stripeset  = disks_per_server[i*(self.k+self.m) :(i+1)*(self.k+self.m)]
                sets.append(stripeset)
            self.flat_cluster_server_layout[serverId] = sets
            logging.info("* server {} has {} stripesets".format(serverId, num_stripesets))



    def net_raid_layout(self):
        logging.debug("* net raid generation")
        self.net_raid_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            sets = []
            for i in range(num_stripesets):
                stripeset  = disks_per_server[i*(self.k+self.m) :(i+1)*(self.k+self.m)]
                sets.append(stripeset)
            self.flat_cluster_server_layout[serverId] = sets
            logging.info("* server {} has {} stripesets".format(serverId, num_stripesets))










if __name__ == "__main__":
    sys = Trinity(330, 110, 8, 2, 0,2,1,1)
    sys.flat_cluster_layout()


