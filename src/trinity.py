import numpy as np
import random
from newposition import Position
import logging
from stripeset import Stripeset
from server import Server
#----------------------------
# Logging Settings
#----------------------------

class Trinity:
    def __init__(self, num_disks, num_disks_per_server, k, m, place_type, diskCap, rebuildRate, utilizeRatio, top_k = 1, top_m = 0):
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
        self.disks = range(num_disks)
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
            #self.flat_greedy_layout()
        #--------------------------------------------
        self.diskSize = diskCap
        self.diskIO = rebuildRate
        self.utilizeRatio = utilizeRatio


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
            self.server_states[serverId] = Server.STATE_NORMAL
            logging.info("* server {} has {} stripesets".format(serverId, num_stripesets))
        #for serverId in self.servers:
        #    print "serverId", serverId, len(self.flat_cluster_server_layout[serverId])


    
    def flat_decluster_layout(self):
        logging.debug("* flat decluster generation *")
        self.flat_decluster_server_layout = {}
        for serverId in self.servers:
            disks_per_server = self.disks_per_server[serverId]
            self.flat_decluster_server_layout[serverId] = disks_per_server
            self.server_states[serverId] = Server.STATE_NORMAL






    # same as flat_cluster_layout
    def mlec_cluster_layout(self):
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



    def flat_greedy_layout(self):
        logger.info("* flat greedy generation *")
        self.flat_draid_server_layout = {}
        self.stripesets_per_disk = {}
        basePermu = [[0,13,15,20,28,39,51,57,60,61]]
        #basePermu = [[0,2,7,15,26,32,35,36]]
        #6basePermu = [[0,2,7,13,16,17],[0,19,37,49,69,77]]
        #4basePermu = [[0,2,5,6],[0,9,17,24],[0,12,23,33],[0,16,30,43],[0,20,39,57],[0,25,47,75],[0,34,66,92],[0,36,65,105]]
        for serverId in self.servers:
            sets = []
            for base in basePermu:
                for j in range(self.num_disks_per_server):
                    stripeset = [(x+j)%self.num_disks_per_server + self.num_disks_per_server*serverId for x in base]
                    sets.append(stripeset)
                    for diskId in stripeset:
                        if diskId not in self.stripesets_per_disk:
                            self.stripesets_per_disk[diskId] = [stripeset]
                        else:
                            self.stripesets_per_disk[diskId].append(stripeset)
            self.flat_draid_server_layout[serverId] = sets









if __name__ == "__main__":
    sys = Trinity(330, 110, 8, 2, 0,2,1,1)
    sys.flat_cluster_layout()


