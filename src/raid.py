from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from server import Server

class RAID:
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
    
    def update_priority(self, event_type, diskset):
        updated_servers = {}
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                if self.servers[serverId].state == Server.STATE_FAILED:
                    logging.info("update_priority(): server {} is failed. Event type: {}".format(serverId, event_type))
                    continue
                fail_per_server = self.state.get_failed_disks_per_server(serverId)
                #  what if there are multiple servers
                if len(fail_per_server) > 0:
                    if self.sys.place_type == 0:
                        if not self.sys.adapt:
                            for diskId in fail_per_server:
                                self.update_disk_repair_time(diskId, len(fail_per_server))

        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                if self.servers[serverId].state == Server.STATE_FAILED:
                    logging.info("update_priority(): server {} is failed".format(serverId))
                    continue
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                fail_per_server = self.state.get_failed_disks_per_server(serverId)
                new_failures = set(fail_per_server).intersection(set(diskset))
                if len(new_failures) > 0:
                    logging.debug(serverId, "======> ",fail_per_server, diskset, new_failures)
                    #--------------------------------------------
                    # calculate repair time for cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 0:
                        if len(new_failures) > 0:
                            for diskId in new_failures:
                                self.disks[diskId].repair_start_time = self.curr_time
                            if self.sys.adapt:
                                for diskId in new_failures:
                                    self.update_disk_repair_time_adapt(diskId, len(fail_per_server))
                            else:
                                for diskId in fail_per_server:
                                    self.update_disk_repair_time(diskId, len(fail_per_server))
        
        

    def update_disk_repair_time(self, diskId, fail_per_server):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_server)
        # if repaired_percent > 0 and (fail_per_server > 1  or 
        #     disk.repair_time[0] != float(disk.curr_repair_data_remaining)/self.sys.diskIO):
        #     print("fail_per_server {}  old repair time: {}  old repair time:{}  new repair time: {} new finish time {}".format(
        #         fail_per_server, disk.repair_time[0], disk.repair_time[0] + disk.repair_start_time, repair_time / 3600 / 24,
        #         repair_time / 3600 / 24 + self.curr_time
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

    
    def update_disk_repair_time_adapt(self, diskId, fail_per_server):
        disk = self.disks[diskId]
        serverId = diskId // self.sys.num_disks_per_server
        server = self.servers[serverId]
        stripesetId = (diskId % self.sys.num_disks_per_server) // self.n
        repair_time = float(disk.repair_data)/(self.sys.diskIO)
        # if repaired_percent > 0 and (fail_per_server > 1  or 
        #     disk.repair_time[0] != float(disk.curr_repair_data_remaining)/self.sys.diskIO):
        #     print("fail_per_server {}  old repair time: {}  old repair time:{}  new repair time: {} new finish time {}".format(
        #         fail_per_server, disk.repair_time[0], disk.repair_time[0] + disk.repair_start_time, repair_time / 3600 / 24,
        #         repair_time / 3600 / 24 + self.curr_time
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = max(self.curr_time, server.stripesets_repair_finish[stripesetId])
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        server.stripesets_repair_finish[stripesetId] = disk.estimate_repair_time
        