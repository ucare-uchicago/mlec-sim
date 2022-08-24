from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from server import Server

class MLECDP:
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
                # print("{} {} for disk {} priority {}".format(self.curr_time, event_type, diskId, self.disks[diskId].priority))
                    curr_priority = self.disks[diskId].priority
                    del self.disks[diskId].repair_time[curr_priority]
                    # print("delete repair time for disk {} priority {}".format(diskId, curr_priority))
                    self.disks[diskId].priority -= 1
                    self.disks[diskId].repair_start_time = self.curr_time

            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                if self.servers[serverId].state == Server.STATE_FAILED:
                    # logging.info("update_priority(): server {} is failed. Event type: {}".format(serverId, event_type))
                    continue
                fail_per_server = self.state.get_failed_disks_per_server(serverId)
                #  what if there are multiple servers
                if len(fail_per_server) > 0:
                        if self.sys.adapt:
                            priorities = []
                            for diskId in fail_per_server:
                                priorities.append(self.disks[diskId].priority)
                            max_priority = max(priorities)
                            for diskId in fail_per_server:
                                self.update_disk_repair_time_adapt(diskId, 
                                    self.disks[diskId].priority, len(fail_per_server), max_priority)
                        else:
                            for diskId in fail_per_server:
                                self.update_disk_repair_time(diskId, self.disks[diskId].priority, len(fail_per_server))

        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                serverId = diskId // self.sys.num_disks_per_server
                if self.servers[serverId].state == Server.STATE_FAILED:
                    # logging.info("update_priority(): server {} is failed".format(serverId))
                    continue
                if serverId in updated_servers:
                    continue
                updated_servers[serverId] = 1
                fail_per_server = self.state.get_failed_disks_per_server(serverId)
                new_failures = set(fail_per_server).intersection(set(diskset))
                if len(new_failures) > 0:
                    #-----------------------------------------------------
                    # calculate repairT and update priority for decluster
                    #-----------------------------------------------------
                    if len(new_failures) > 0:
                            #----------------------------------------------
                            fail_num = len(fail_per_server) # count total failed disks number
                            good_num = len(self.sys.disks_per_server[serverId]) - fail_num
                            #----------------------------------------------
                            priorities = []
                            for diskId in fail_per_server:
                                priorities.append(self.disks[diskId].priority)
                            max_priority = max(priorities)+len(new_failures)
                            #----------------------------------------------
                            for diskId in new_failures:
                                curr_priority = self.disks[diskId].priority
                                #-----------------------------------------------
                                # disk's priority can be increased by #new-fails
                                #-----------------------------------------------
                                self.disks[diskId].priority = max_priority
                                self.disks[diskId].repair_start_time = self.curr_time
                                self.disks[diskId].good_num = good_num
                                self.disks[diskId].fail_num = fail_num
                                # logging.info("\tdisk {} priority {}".format(diskId, self.disks[diskId].priority))

                            if self.sys.adapt:
                                for diskId in fail_per_server:
                                    self.update_disk_repair_time_adapt(diskId, self.disks[diskId].priority, 
                                        len(fail_per_server), max_priority)
                            else:
                                for diskId in fail_per_server:
                                    self.update_disk_repair_time(diskId, self.disks[diskId].priority, 
                                        len(fail_per_server))

    def update_disk_repair_time(self, diskId, priority, fail_per_server):
        disk = self.disks[diskId]
        good_num = disk.good_num
        fail_num = disk.fail_num
        #----------------------------
        repaired_time = self.curr_time - disk.repair_start_time
        # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
        if repaired_time == 0:
            priority_sets = self.ncr(good_num, self.n-priority)*self.ncr(fail_num-1, priority-1)
            total_sets = self.ncr((good_num+fail_num-1), (self.n-1)) 
            priority_percent = float(priority_sets)/total_sets
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            if priority > 1:
                self.sys.metrics.total_rebuild_io_per_year -= disk.curr_repair_data_remaining * (priority - 1) * self.sys.k

        else:
            # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        #----------------------------------------------------
        #print priority, "priority percent ", priority_percent
        parallelism = good_num
        #print "decluster parallelism", diskId, parallelism
        #----------------------------------------------------
        # amplification = self.sys.k + priority
        amplification = self.sys.k + 1
        if priority < fail_per_server:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_server)
        else:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
        #print "-----", self.sys.diskSize, amplification, self.sys.diskIO, parallelism
        #----------------------------------------------------
        # self.disks[diskId].repair_time[priority] = repair_time/3600
        self.disks[diskId].repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[priority]
        # print("{}  disk {}  priority {}  repair time {}".format(self.curr_time, diskId, priority, disk.repair_time))
        #----------------------------------------------------


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
                max_priority = 0
                for diskId in fail_per_server:
                    # logging.info("\tdisk {} priority {}".format(diskId, self.disks[diskId].priority))
                    if self.disks[diskId].priority > max_priority:
                        max_priority = self.disks[diskId].priority
                # logging.info("max_priority: {}  fail_per_server: {}"
                #                 .format(max_priority, fail_per_server))
                if max_priority > self.sys.m:
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
        repair_time = float(server.curr_repair_data_remaining)/(self.sys.diskIO * self.sys.num_disks_per_server / failed_servers)
        server.repair_time[0] = repair_time / 3600 / 24
        server.repair_start_time = self.curr_time
        server.estimate_repair_time = self.curr_time + server.repair_time[0]
        # logging.info("calculate repair time for server {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 serverId, repaired_time, server.repair_time[0], server.repair_start_time))

    def ncr(self, n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        denom = reduce(op.mul, range(1, r+1), 1)
        return numer / denom