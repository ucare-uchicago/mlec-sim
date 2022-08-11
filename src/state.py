from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from server import Server
from netraid import NetRAID
from raid import RAID
from decluster import Decluster
from mlec import MLEC

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
        server_repair_data = sys.diskSize * sys.num_disks_per_server
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
                self.sys.metrics.total_net_traffic_per_year += self.disks[diskId].repair_data * (self.sys.k + 1)
                
            if event_type == Disk.EVENT_FAIL:
                self.disks[diskId].state = Disk.STATE_FAILED
                self.servers[serverId].failed_disks[diskId] = 1
                self.failed_disks[diskId] = 1
                # logging.info("server {} after add: {}".format(serverId, self.servers[serverId].failed_disks))


    #----------------------------------------------
    # update server state
    #----------------------------------------------
    def update_server_state(self, event_type, diskset):
        new_server_failures = []
        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                # if server already fails, we don't need to fail it again.
                if self.servers[serverId].state == Server.STATE_FAILED:
                    continue
                # otherwise, we need to check if a new server fails
                fail_per_server = self.get_failed_disks_per_server(serverId)
                stripesets_per_server = self.sys.flat_cluster_server_layout[serverId]
                for stripeset in stripesets_per_server:
                    fail_per_set = set(stripeset).intersection(set(fail_per_server))
                    if len(fail_per_set) > self.sys.m:
                        if serverId not in new_server_failures:
                            new_server_failures.append(serverId)
                        self.servers[serverId].state = Server.STATE_FAILED
                        self.failed_servers[serverId] = 1
                        break
        
        if event_type == Server.EVENT_FAIL:
            serverset = diskset
            for serverId in serverset:
                self.servers[serverId].state = Server.STATE_FAILED
                new_server_failures.append(serverId)
                self.failed_servers[serverId] = 1

        if event_type == Server.EVENT_REPAIR:
            serverset = diskset
            for serverId in serverset:
                self.servers[serverId].state = Server.STATE_NORMAL
                self.failed_servers.pop(serverId, None)
                for diskId in self.servers[serverId].failed_disks:
                    self.failed_disks.pop(diskId, None)
                self.servers[serverId].failed_disks.clear()
                
                for diskId in self.sys.disks_per_server[serverId]:
                    self.disks[diskId].state = Disk.STATE_NORMAL 


        return new_server_failures


    #----------------------------------------------
    # update decluster: priority, #stripesets
    #----------------------------------------------
    def update_priority(self, event_type, diskset):
        self.policy.update_priority(event_type, diskset)
                                
    
    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_server_priority(self, event_type, new_failed_servers, serverset):
        failed_servers = self.get_failed_servers()
        if event_type == Disk.EVENT_FAIL:
            if self.sys.place_type == 2:
                for serverId in new_failed_servers:
                    self.servers[serverId].repair_start_time = self.curr_time
                for serverId in failed_servers:
                    self.update_mlec_cluster_server_repair_time(serverId, len(failed_servers))

        if event_type == Server.EVENT_FAIL:
            if self.sys.place_type == 2:
                for serverId in new_failed_servers:
                    self.servers[serverId].repair_start_time = self.curr_time
                for serverId in failed_servers:
                    self.update_mlec_cluster_server_repair_time(serverId, len(failed_servers))

        if event_type == Server.EVENT_REPAIR:
            if self.sys.place_type == 2:
                for serverId in failed_servers:
                    self.update_mlec_cluster_server_repair_time(serverId, len(failed_servers))











    def update_mlec_cluster_server_repair_time(self, serverId, failed_servers):
        server = self.servers[serverId]
        repaired_time = self.curr_time - server.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            server.curr_repair_data_remaining = server.repair_data
        else:
            
            repaired_percent = repaired_time / server.repair_time[0]
            server.curr_repair_data_remaining = server.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(server.curr_repair_data_remaining)/(self.sys.diskIO * self.sys.num_disks_per_server / failed_servers)
        server.repair_time[0] = repair_time / 3600 / 24
        server.repair_start_time = self.curr_time
        server.estimate_repair_time = self.curr_time + server.repair_time[0]
        logging.info("calculate repair time for server {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
                        serverId, repaired_time, server.repair_time[0], server.repair_start_time))







    def get_failed_disks_per_server(self, serverId):
        logging.info("sedrver {} get: {}".format(serverId, list(self.servers[serverId].failed_disks.keys())))
        return list(self.servers[serverId].failed_disks.keys())


    def get_failed_disks_per_stripeset(self, stripesetId):
        failed_disks = []
        stripeset = self.sys.net_raid_stripesets_layout[stripesetId]
        for diskId in stripeset:
            # logging.info("  get_failed_disks_per_stripeset  diskId: {}".format(diskId))
            if self.disks[diskId].state == Disk.STATE_FAILED:
                failed_disks.append(diskId)
        return failed_disks


    def get_failed_disks(self):
        return list(self.failed_disks.keys())

    def get_failed_servers(self):
        return list(self.failed_servers.keys())
