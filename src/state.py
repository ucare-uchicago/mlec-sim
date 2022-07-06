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
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            for diskId in diskset:
                if self.sys.place_type == 1:
                    curr_priority = self.disks[diskId].priority
                    logging.debug("pop diskId",diskId, curr_priority)
                    if curr_priority == self.initial_priority+1:
                        self.disks[diskId].percent[self.initial_priority] = 1.0
                    del self.disks[diskId].percent[curr_priority]
                    self.disks[diskId].priority -= 1
        if event_type == Disk.EVENT_FAIL:
            for serverId in self.sys.servers:
                fail_per_server = self.get_failed_disks_per_server(serverId)
                new_failures = set(fail_per_server).intersection(set(diskset))
                if len(fail_per_server) > 0:
                    logging.debug(serverId, "======> ",fail_per_server, diskset, new_failures)
                    #--------------------------------------------
                    # calculate repair time for cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 0:
                        if len(new_failures) > 0:
                            for diskId in new_failures:
                                self.calculate_cluster_repair_time(diskId, fail_per_server)
                    #-----------------------------------------------------
                    # calculate repairT and update priority for decluster
                    #-----------------------------------------------------
                    if self.sys.place_type == 1:
                        if len(new_failures) > 0:
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
                                for priority in range(1, max_priority+1):
                                    self.calculate_decluster_repair_time(diskId, priority, good_num, fail_num)
                                #--------------------------------------------
                                self.disks[diskId].priority = max_priority
                                

                     


    def calculate_cluster_repair_time(self, diskId, fail_per_server):
        repair_data = self.disks[diskId].repair_data
        repair_time = float(repair_data)/(self.sys.diskIO)
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
