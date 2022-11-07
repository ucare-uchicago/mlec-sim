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


    def update_disk_state(self, event_type, diskId):
        rackId = diskId // self.sys.num_disks_per_rack
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.racks[rackId].failed_disks.pop(diskId, None)
            self.failed_disks.pop(diskId, None)
            # logging.info("rack {} after pop: {}".format(rackId, self.racks[rackId].failed_disks))
            
            
        if event_type == Disk.EVENT_FAIL:
            self.disks[diskId].state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.failed_disks[diskId] = 1
            # logging.info("rack {} after add: {}".format(rackId, self.racks[rackId].failed_disks))




    def update_disk_priority(self, event_type, diskId):
        logging.info("Event %s, dID %s, time: %s", event_type, diskId, self.state.curr_time)
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            curr_priority = self.disks[diskId].priority
            del self.disks[diskId].repair_time[curr_priority]
            self.disks[diskId].priority -= 1
            self.disks[diskId].repair_start_time = self.curr_time

            rackId = diskId // self.sys.num_disks_per_rack
            if self.racks[rackId].state == Rack.STATE_FAILED:
                # logging.info("update_disk_priority(): rack {} is failed. Event type: {}".format(rackId, event_type))
                return
            fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
            if len(fail_per_rack) > 0:
                if self.sys.adapt:
                    priorities = []
                    for diskId in fail_per_rack:
                        priorities.append(self.disks[diskId].priority)
                    max_priority = max(priorities)
                    for dId in fail_per_rack:
                        self.update_disk_repair_time_adapt(dId, 
                            self.disks[dId].priority, len(fail_per_rack), max_priority)
                else:
                    for dId in fail_per_rack:
                        self.update_disk_repair_time(dId, self.disks[dId].priority, len(fail_per_rack))

        if event_type == Disk.EVENT_FAIL:
                rackId = diskId // self.sys.num_disks_per_rack
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    # logging.info("update_disk_priority(): rack {} is failed".format(rackId))
                    return
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                    #-----------------------------------------------------
                    # calculate repairT and update priority for decluster
                    #-----------------------------------------------------
                fail_num = len(fail_per_rack) # count total failed disks number
                good_num = len(self.sys.disks_per_rack[rackId]) - fail_num
                #----------------------------------------------
                priorities = []
                for dId in fail_per_rack:
                    priorities.append(self.disks[dId].priority)
                max_priority = max(priorities)+1
                
                logging.info("Failed disk system: %s", self.state.failed_disks)
                logging.info("Max prio: %s", max_priority)
                #----------------------------------------------
                curr_priority = self.disks[diskId].priority
                #-----------------------------------------------
                # disk's priority can be increased by #new-fails
                #-----------------------------------------------
                self.disks[diskId].priority = max_priority
                self.disks[diskId].repair_start_time = self.curr_time
                self.disks[diskId].good_num = good_num
                self.disks[diskId].fail_num = fail_num
                if self.sys.adapt:
                    for dId in fail_per_rack:
                        self.update_disk_repair_time_adapt(dId, self.disks[dId].priority, 
                            len(fail_per_rack), max_priority)
                else:
                    for dId in fail_per_rack:
                        self.update_disk_repair_time(dId, self.disks[dId].priority, 
                            len(fail_per_rack))
        logging.info("-----")         
    

    def update_disk_repair_time(self, diskId, priority, fail_per_rack):
        logging.info("Updating repair time for diskId %d, prio %d", diskId, priority)
        logging.info("Disk %s", str(self.disks[diskId]))
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
            logging.info("Priority percent: %s and disk prio: %s", priority_percent, disk.priority)
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
        
        logging.info("Fail per rack %s", fail_per_rack)
        logging.info("Parallelism: %s, amplification: %s, data: %s, diskIO: %s", parallelism, amplification, disk.curr_repair_data_remaining, self.sys.diskIO)
        logging.info("Time needed for repair %s d", (repair_time / 3600 / 24))
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
            logging.info("ncr(%s,%s)*ncr(%s,%s) %s*%s", good_num, self.n-priority, fail_num-1, priority-1, self.ncr(good_num, self.n-priority), self.ncr(fail_num-1, priority-1))
            logging.info("ncr(%s,%s), %s", good_num + fail_num - 1, self.n -1, self.ncr(good_num + fail_num - 1, self.n - 1))
            logging.info("Good num %s, Fail num %s, prio %s, n %s, Prio perc %s",good_num, fail_num, priority, self.n, priority_percent)
            
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
            
        logging.info("Fail per rack %s", fail_per_rack)
        logging.info("Parallelism: %s, amplification: %s, data: %s, diskIO: %s", parallelism, amplification, disk.curr_repair_data_remaining, self.sys.diskIO)
        logging.info("Time needed for repair %s d", (repair_time / 3600 / 24))
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