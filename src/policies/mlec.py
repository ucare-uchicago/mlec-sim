from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from server import Server

class MLEC:
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        self.state = state
        self.sys = state.sys
        self.n = state.n
        self.servers = state.servers
        self.disks = state.disks
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_servers = state.failed_servers

    #----------------------------------------------
    def update_priority(self, event_type, diskset):
        updated_servers = {}
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            for diskId in diskset:
                disk = self.disks[diskId]
                self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.k + 1)
                serverId = diskId // self.sys.num_disks_per_server
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                if self.servers[serverId].state == Server.STATE_FAILED:
                    logging.info("update_priority(): server {} is failed. Event type: {}".format(serverId, event_type))
                    continue
                fail_per_stripeset = self.state.get_failed_disks_per_stripeset_diskId(diskId)
                #  what if there are multiple servers
                if len(fail_per_stripeset) > 0:
                    if self.sys.place_type == 2:
                        for diskId in fail_per_stripeset:
                            self.update_disk_repair_time(diskId, len(fail_per_stripeset))
        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                if self.servers[serverId].state == Server.STATE_FAILED:
                    logging.info("update_priority(): server {} is failed".format(serverId))
                    continue
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                fail_per_stripeset = self.state.get_failed_disks_per_stripeset_diskId(diskId)
                new_failures = set(fail_per_stripeset).intersection(set(diskset))
                if len(new_failures) > 0:
                    logging.debug(serverId, "======> ",fail_per_stripeset, diskset, new_failures)
                    #--------------------------------------------
                    # calculate repair time for mlec cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 2:
                        if len(new_failures) > 0:
                            for diskId in new_failures:
                                self.disks[diskId].repair_start_time = self.curr_time
                            for diskId in fail_per_stripeset:
                                self.update_disk_repair_time(diskId, len(fail_per_stripeset))

    def update_disk_repair_time(self, diskId, fail_per_stripeset):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_stripeset)
        # if repaired_percent > 0 and (fail_per_stripeset > 1  or 
        #     disk.repair_time[0] != float(disk.curr_repair_data_remaining)/self.sys.diskIO):
        #     print("fail_per_stripeset {}  old repair time: {}  old repair time:{}  new repair time: {} new finish time {}".format(
        #         fail_per_stripeset, disk.repair_time[0], disk.repair_time[0] + disk.repair_start_time, repair_time / 3600 / 24,
        #         repair_time / 3600 / 24 + self.curr_time
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("calculate repair time for disk {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
                        diskId, repaired_time, disk.repair_time[0], disk.repair_start_time))


    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_server_priority(self, event_type, new_failed_servers, serverset):
        failed_servers = self.state.get_failed_servers()
        if event_type == Disk.EVENT_FAIL:
                for serverId in new_failed_servers:
                    self.servers[serverId].repair_start_time = self.curr_time
                for serverId in failed_servers:
                    self.update_server_repair_time(serverId, len(failed_servers))

        if event_type == Server.EVENT_FAIL:
                for serverId in new_failed_servers:
                    self.servers[serverId].repair_start_time = self.curr_time
                for serverId in failed_servers:
                    self.update_server_repair_time(serverId, len(failed_servers))

        if event_type == Server.EVENT_REPAIR:
                for serverId in failed_servers:
                    self.update_server_repair_time(serverId, len(failed_servers))
    
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
                fail_per_server = self.state.get_failed_disks_per_server(serverId)
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
                
                self.sys.metrics.total_net_traffic += self.servers[serverId].repair_data * (self.sys.top_k + 1)


        return new_server_failures
    
    def update_server_repair_time(self, serverId, failed_servers):
        server = self.servers[serverId]
        repaired_time = self.curr_time - server.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            server.curr_repair_data_remaining = server.repair_data
        else:
            
            repaired_percent = repaired_time / server.repair_time[0]
            server.curr_repair_data_remaining = server.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(server.curr_repair_data_remaining)/(self.sys.diskIO * self.n / failed_servers)
        server.repair_time[0] = repair_time / 3600 / 24
        server.repair_start_time = self.curr_time
        server.estimate_repair_time = self.curr_time + server.repair_time[0]
        logging.info("calculate repair time for server {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
                        serverId, repaired_time, server.repair_time[0], server.repair_start_time))
