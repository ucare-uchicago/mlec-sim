from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce

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
        self.disks = {}
        for diskId in range(sys.num_disks):
            self.disks[diskId] = Disk(diskId, None, None)#disk-fail-distr, trace-fail-times
        self.initial_priority = 0 #means no failure in stripesets
        #----------------------------------


    def update_clock(self, curr_time):
        for diskId in range(self.sys.num_disks):
            self.disks[diskId].update_clock(curr_time)


    #----------------------------------------------
    # update decluster: diskset state
    #----------------------------------------------
    def update_state(self, event_type, diskset):
        for diskId in diskset:
            if event_type == Disk.EVENT_REPAIR:
                self.disks[diskId].state = Disk.STATE_NORMAL 
            if event_type == Disk.EVENT_FAIL:
                self.disks[diskId].state = Disk.STATE_FAILED



    #----------------------------------------------
    # update decluster: priority, #stripesets
    #----------------------------------------------
    def update_priority(self, event_type, diskset):
        current_clock = self.disks[diskset[0]].clock
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            
            for diskId in diskset:
                # print("{} {} for disk {} priority {}".format(current_clock, event_type, diskId, self.disks[diskId].priority))
                if self.sys.place_type == 1:
                    curr_priority = self.disks[diskId].priority
                    logging.debug("pop diskId",diskId, curr_priority)
                    if curr_priority == self.initial_priority+1:
                        self.disks[diskId].percent[self.initial_priority] = 1.0
                    del self.disks[diskId].percent[curr_priority]
                    del self.disks[diskId].repair_time[curr_priority]
                    # print("delete repair time for disk {} priority {}".format(diskId, curr_priority))
                    self.disks[diskId].priority -= 1
                    self.disks[diskId].repair_start_time = self.disks[diskId].clock

            for serverId in self.sys.servers:
                fail_per_server = self.get_failed_disks_per_server(serverId)
                #  what if there are multiple servers
                if len(fail_per_server) > 0:
                    if self.sys.place_type == 0:
                        for diskId in fail_per_server:
                            self.update_cluster_repair_time(diskId, len(fail_per_server))
                    if self.sys.place_type == 1:
                        for diskId in fail_per_server:
                            self.update_decluster_repair_time(diskId, self.disks[diskId].priority, len(fail_per_server))
        if event_type == Disk.EVENT_FAIL:
            for serverId in self.sys.servers:
                fail_per_server = self.get_failed_disks_per_server(serverId)
                new_failures = set(fail_per_server).intersection(set(diskset))
                if len(new_failures) > 0:
                    logging.debug(serverId, "======> ",fail_per_server, diskset, new_failures)
                    #--------------------------------------------
                    # calculate repair time for cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 0:
                        if len(new_failures) > 0:
                            for diskId in new_failures:
                                self.disks[diskId].repair_start_time = self.disks[diskId].clock
                            for diskId in fail_per_server:
                                self.update_cluster_repair_time(diskId, len(fail_per_server))
                    #-----------------------------------------------------
                    # calculate repairT and update priority for decluster
                    #-----------------------------------------------------
                    if self.sys.place_type == 1:
                        if len(new_failures) > 0:
                            # print("{} {} for disk {} priority {}".format(current_clock, event_type, diskset, self.disks[diskset[0]].priority))

                            logging.debug("server {} {} {}".format(serverId, fail_per_server, new_failures))
                            #--------------------------------------------
                            #print "serverId", serverId, "-", fail_per_server
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
                                del self.disks[diskId].percent[curr_priority]
                                #-----------------------------------------------
                                # disk's priority can be increased by #new-fails
                                #-----------------------------------------------
                                self.disks[diskId].priority = max_priority
                                self.disks[diskId].repair_start_time = self.disks[diskId].clock
                                self.disks[diskId].good_num = good_num
                                self.disks[diskId].fail_num = fail_num
                            for diskId in fail_per_server:
                                self.update_decluster_repair_time(diskId, self.disks[diskId].priority, len(fail_per_server))
                                #--------------------------------------------
                                
                                

    def update_cluster_repair_time(self, diskId, fail_per_server):
        disk = self.disks[diskId]
        repaired_time = disk.clock - disk.repair_start_time
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
        #         repair_time / 3600 / 24 + disk.clock
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = disk.clock

                     


    def calculate_cluster_repair_time(self, diskId, fail_per_server):
        repair_data = self.disks[diskId].repair_data
        repair_time = float(repair_data)/(self.sys.diskIO/fail_per_server)
        #---------------------------------------------
        # no priority reconstruct, use key 0 in default
        #---------------------------------------------
        # self.disks[diskId].repair_time[0] = repair_time/3600
        self.disks[diskId].repair_time[0] = repair_time / 3600 / 24
        logging.debug(diskId, "repair_time", self.disks[diskId].repair_time[0])




    def calculate_decluster_repair_time(self, diskId, priority, good_num, fail_num):
        #----------------------------------------------------
        priority_sets = self.ncr(good_num, self.n-priority)*self.ncr(fail_num-1, priority-1)
        total_sets = self.ncr((good_num+fail_num-1), (self.n-1)) 
        priority_percent = float(priority_sets)/total_sets
        self.disks[diskId].percent[priority] = priority_percent
        #----------------------------------------------------
        #print priority, "priority percent ", priority_percent
        parallelism = good_num
        #print "decluster parallelism", diskId, parallelism
        #----------------------------------------------------
        amplification = self.sys.k + priority
        repair_data = self.disks[diskId].repair_data
        repair_time = priority_percent*repair_data*amplification/(self.sys.diskIO*parallelism)
        #print "-----", self.sys.diskSize, amplification, self.sys.diskIO, parallelism
        #----------------------------------------------------
        # self.disks[diskId].repair_time[priority] = repair_time/3600
        self.disks[diskId].repair_time[priority] = repair_time / 3600 / 24
        #----------------------------------------------------


    def update_decluster_repair_time(self, diskId, priority, fail_per_server):
        disk = self.disks[diskId]
        good_num = disk.good_num
        fail_num = disk.fail_num
        #----------------------------
        repaired_time = disk.clock - disk.repair_start_time
        # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
        if repaired_time == 0:
            priority_sets = self.ncr(good_num, self.n-priority)*self.ncr(fail_num-1, priority-1)
            total_sets = self.ncr((good_num+fail_num-1), (self.n-1)) 
            priority_percent = float(priority_sets)/total_sets
            self.disks[diskId].percent[priority] = priority_percent
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
        else:
            # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        #----------------------------------------------------
        #print priority, "priority percent ", priority_percent
        parallelism = good_num
        #print "decluster parallelism", diskId, parallelism
        #----------------------------------------------------
        amplification = self.sys.k + priority
        if priority < fail_per_server:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_server)
        else:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
        #print "-----", self.sys.diskSize, amplification, self.sys.diskIO, parallelism
        #----------------------------------------------------
        # self.disks[diskId].repair_time[priority] = repair_time/3600
        self.disks[diskId].repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = disk.clock
        # print("{}  disk {}  priority {}  repair time {}".format(disk.clock, diskId, priority, disk.repair_time))
        #----------------------------------------------------


    def ncr(self, n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        denom = reduce(op.mul, range(1, r+1), 1)
        return numer / denom


    def get_failed_disks_per_server(self, serverId):
        fail_per_server = []
        disks_per_server = self.sys.disks_per_server[serverId]
        for diskId in disks_per_server:
            if self.disks[diskId].state == Disk.STATE_FAILED:
                fail_per_server.append(diskId)
        return fail_per_server



    def get_failed_disks(self):
        failed_disks = []
        for diskId in self.disks:
            if self.disks[diskId].state == Disk.STATE_FAILED:
                failed_disks.append(diskId)
        return failed_disks
