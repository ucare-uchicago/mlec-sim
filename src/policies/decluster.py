from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from rack import Rack


class Decluster:
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        self.state = state
        self.sys = state.sys
        self.n = state.n
        self.racks = state.racks
        self.disks = state.disks
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_racks = state.failed_racks


    def update_priority(self, event_type, diskset):
        updated_racks = {}

        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            for diskId in diskset:
                # print("{} {} for disk {} priority {}".format(self.curr_time, event_type, diskId, self.disks[diskId].priority))
                if self.sys.place_type == 1:
                    curr_priority = self.disks[diskId].priority
                    del self.disks[diskId].repair_time[curr_priority]
                    # print("delete repair time for disk {} priority {}".format(diskId, curr_priority))
                    self.disks[diskId].priority -= 1
                    self.disks[diskId].repair_start_time = self.curr_time

            for diskId in diskset:
                rackId = diskId // self.sys.num_disks_per_rack
                if rackId in updated_racks:
                    continue
                updated_racks[rackId] = 1
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    # logging.info("update_priority(): rack {} is failed. Event type: {}".format(rackId, event_type))
                    continue
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                #  what if there are multiple racks
                if len(fail_per_rack) > 0:
                    if self.sys.place_type == 1:
                        if self.sys.adapt:
                            priorities = []
                            for diskId in fail_per_rack:
                                priorities.append(self.disks[diskId].priority)
                            max_priority = max(priorities)
                            for diskId in fail_per_rack:
                                self.update_disk_repair_time_adapt(diskId, 
                                    self.disks[diskId].priority, len(fail_per_rack), max_priority)
                        else:
                            for diskId in fail_per_rack:
                                self.update_disk_repair_time(diskId, self.disks[diskId].priority, len(fail_per_rack))

        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                rackId = diskId // self.sys.num_disks_per_rack
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    logging.info("update_priority(): rack {} is failed".format(rackId))
                    continue
                if rackId in updated_racks:
                    continue
                updated_racks[rackId] = 1
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                new_failures = set(fail_per_rack).intersection(set(diskset))
                if len(new_failures) > 0:
                    logging.debug(rackId, "======> ",fail_per_rack, diskset, new_failures)
                    #-----------------------------------------------------
                    # calculate repairT and update priority for decluster
                    #-----------------------------------------------------
                    if self.sys.place_type == 1:
                        if len(new_failures) > 0:
                            # print("{} {} for disk {} priority {}".format(self.curr_time, event_type, diskset, self.disks[diskset[0]].priority))

                            logging.debug("rack {} {} {}".format(rackId, fail_per_rack, new_failures))
                            #--------------------------------------------
                            #print "rackId", rackId, "-", fail_per_rack
                            #----------------------------------------------
                            fail_num = len(fail_per_rack) # count total failed disks number
                            good_num = len(self.sys.disks_per_rack[rackId]) - fail_num
                            #----------------------------------------------
                            priorities = []
                            for diskId in fail_per_rack:
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
                            if self.sys.adapt:
                                for diskId in fail_per_rack:
                                    self.update_disk_repair_time_adapt(diskId, self.disks[diskId].priority, 
                                        len(fail_per_rack), max_priority)
                            else:
                                for diskId in fail_per_rack:
                                    self.update_disk_repair_time(diskId, self.disks[diskId].priority, 
                                        len(fail_per_rack))
                            
    

    def update_disk_repair_time(self, diskId, priority, fail_per_rack):
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
        amplification = self.sys.k + priority
        if priority < fail_per_rack:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_rack)
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

    def update_disk_repair_time_adapt(self, diskId, priority, fail_per_rack, max_priority):
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
        else:
            # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        #----------------------------------------------------
        #print priority, "priority percent ", priority_percent
        parallelism = good_num
        #print "decluster parallelism", diskId, parallelism
        #----------------------------------------------------
        amplification = self.sys.k + 1
        if priority < fail_per_rack:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_rack)
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
        if max_priority == fail_per_rack and disk.priority < max_priority:
            # the disk repair will be delayed because other disk is doing critical rebuild
            max_priority_sets = self.ncr(good_num, self.n-max_priority)*self.ncr(fail_num-1, max_priority-1)
            total_sets = self.ncr((good_num+fail_num-1), (self.n-1)) 
            max_priority_percent = float(max_priority_sets)/total_sets
            critical_data = disk.repair_data * max_priority_percent
            critical_repair_time = critical_data*amplification/(self.sys.diskIO*parallelism) / 3600 / 24
            disk.repair_start_time += critical_repair_time
            disk.estimate_repair_time += critical_repair_time
            logging.info("disk ID {}  disk priority {}  fail_per_rack {} critical time {}  estimate finish time {}".format(
                            diskId, disk.priority, fail_per_rack, critical_repair_time, disk.estimate_repair_time))


    def ncr(self, n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        denom = reduce(op.mul, range(1, r+1), 1)
        return numer / denom