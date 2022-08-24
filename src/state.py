from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from server import Server
from policies import *

class State:
    #--------------------------------------
    # The 2 possible state
    #--------------------------------------
    SYSTEM_STATE_NORMAL = "state normal"
    SYSTEM_STATE_FAILED = "state failed"

    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, sys):
        #----------------------------------
        self.sys = sys
        self.n = sys.k + sys.m
        self.servers = {}
        self.disks = self.sys.disks
        for diskId in self.disks:
            disk = self.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}
        if self.sys.place_type == 2:
            server_repair_data = sys.diskSize * self.n
        else:
            # server_repair_data = sys.diskSize * self.sys.num_disks_per_server
            server_repair_data = sys.diskSize * (self.sys.m + 1)
        for serverId in self.sys.servers:
            self.servers[serverId] = Server(serverId, server_repair_data, sys.num_disks_per_server // self.n)
        self.curr_time = 0
        self.failed_disks = {}
        self.failed_servers = {}
        if self.sys.place_type == 0:
            self.policy = RAID(self)
        elif self.sys.place_type == 1:
            self.policy = Decluster(self)
        elif self.sys.place_type == 2:
            self.policy = MLEC(self)
        elif self.sys.place_type == 3:
            self.policy = NetRAID(self)
        elif self.sys.place_type == 4:
            self.policy = MLECDP(self)
        self.repairing = True
        self.repair_start_time = 0
        #----------------------------------



    def update_curr_time(self, curr_time):
        self.curr_time = curr_time
        self.policy.curr_time = curr_time



    #----------------------------------------------
    # update diskset state
    #----------------------------------------------
    def update_state(self, event_type, diskset):
        for diskId in diskset:
            serverId = diskId // self.sys.num_disks_per_server
            if event_type == Disk.EVENT_REPAIR:
                self.disks[diskId].state = Disk.STATE_NORMAL
                self.servers[serverId].failed_disks.pop(diskId, None)
                self.failed_disks.pop(diskId, None)
                # logging.info("server {} after pop: {}".format(serverId, self.servers[serverId].failed_disks))
                
                
            if event_type == Disk.EVENT_FAIL:
                self.disks[diskId].state = Disk.STATE_FAILED
                self.servers[serverId].failed_disks[diskId] = 1
                self.failed_disks[diskId] = 1
                # logging.info("server {} after add: {}".format(serverId, self.servers[serverId].failed_disks))


    #----------------------------------------------
    # update server state
    #----------------------------------------------
    def update_server_state(self, event_type, diskset):
        return self.policy.update_server_state(event_type, diskset)



    #----------------------------------------------
    # update decluster: priority, #stripesets
    #----------------------------------------------
    def update_priority(self, event_type, diskset):
        return self.policy.update_priority(event_type, diskset)
                                
    
    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_server_priority(self, event_type, new_failed_servers, serverset):
        self.policy.update_server_priority(event_type, new_failed_servers, serverset)









    def get_failed_disks_per_server(self, serverId):
        # logging.info("sedrver {} get: {}".format(serverId, list(self.servers[serverId].failed_disks.keys())))
        return list(self.servers[serverId].failed_disks.keys())


    def get_failed_disks_per_stripeset(self, stripesetId):
        failed_disks = []
        stripeset = self.sys.net_raid_stripesets_layout[stripesetId]
        for diskId in stripeset:
            # logging.info("  get_failed_disks_per_stripeset  diskId: {}".format(diskId))
            if self.disks[diskId].state == Disk.STATE_FAILED:
                failed_disks.append(diskId)
        return failed_disks


    def get_failed_disks_per_stripeset_diskId(self, diskId):
        failed_disks = []
        serverId = diskId // self.sys.num_disks_per_server
        stripesetId = (diskId % self.sys.num_disks_per_server) // self.n
        stripeset = self.sys.flat_cluster_server_layout[serverId][stripesetId]
        for d in stripeset:
            # logging.info("  get_failed_disks_per_stripeset  diskId: {}".format(diskId))
            if self.disks[d].state == Disk.STATE_FAILED:
                failed_disks.append(d)
        return failed_disks


    def get_failed_disks(self):
        return list(self.failed_disks.keys())

    def get_failed_servers(self):
        return list(self.failed_servers.keys())
