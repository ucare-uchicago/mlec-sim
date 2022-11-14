import logging

from components.disk import Disk
from components.rack import Rack
from system import System
from policies.policy import Policy
from helpers.common_math import ncr
from .pdl import mlec_dp_pdl
from .repair import mlecdp_repair

class MLECDP(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.sys: System = state.sys
        self.n: int = state.n
        self.racks = state.racks
        self.disks = state.disks
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_racks = state.failed_racks
        self.num_rack_groups = self.sys.num_racks // self.sys.top_n 
        self.rack_group_failures = [0] * self.num_rack_groups

    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
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
                    for diskId in fail_per_rack:
                        self.update_disk_repair_time_adapt(diskId, 
                            self.disks[diskId].priority, len(fail_per_rack), max_priority)
                else:
                    for diskId in fail_per_rack:
                        self.update_disk_repair_time(diskId, self.disks[diskId].priority, len(fail_per_rack))

        if event_type == Disk.EVENT_FAIL:
                rackId = diskId // self.sys.num_disks_per_rack
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    # logging.info("update_disk_priority(): rack {} is failed".format(rackId))
                    return
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)

                #-----------------------------------------------------
                # calculate repairT and update priority for decluster
                #-----------------------------------------------------
                #----------------------------------------------
                fail_num = len(fail_per_rack) # count total failed disks number
                good_num = len(self.sys.disks_per_rack[rackId]) - fail_num
                #----------------------------------------------
                priorities = []
                for diskId in fail_per_rack:
                    priorities.append(self.disks[diskId].priority)
                max_priority = max(priorities)+1
                #----------------------------------------------
                curr_priority = self.disks[diskId].priority
                self.disks[diskId].priority = max_priority
                self.disks[diskId].repair_start_time = self.curr_time
                self.disks[diskId].good_num = good_num
                self.disks[diskId].fail_num = fail_num
                # logging.info("\tdisk {} priority {}".format(diskId, self.disks[diskId].priority))

                if self.sys.adapt:
                    for dId in fail_per_rack:
                        self.update_disk_repair_time_adapt(dId, self.disks[dId].priority, 
                            len(fail_per_rack), max_priority)
                else:
                    for dId in fail_per_rack:
                        self.update_disk_repair_time(dId, self.disks[dId].priority, 
                            len(fail_per_rack))

    def update_disk_repair_time(self, diskId, priority, fail_per_rack):
        disk = self.disks[diskId]
        good_num = disk.good_num
        fail_num = disk.fail_num
        #----------------------------
        repaired_time = self.curr_time - disk.repair_start_time
        # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
        if repaired_time == 0:
            priority_sets = ncr(good_num, self.n-priority)*ncr(fail_num-1, priority-1)
            total_sets = ncr((good_num+fail_num-1), (self.n-1)) 
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


    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_rack_priority(self, event_type, rackId, diskId):
        failed_racks = self.state.get_failed_racks()
        if event_type == Disk.EVENT_FAIL:
                self.racks[rackId].repair_start_time = self.curr_time
                self.racks[rackId].init_repair_start_time = self.curr_time
                for rId in failed_racks:
                    self.update_rack_repair_time(rId, len(failed_racks))

        if event_type == Rack.EVENT_FAIL:
                self.racks[rackId].repair_start_time = self.curr_time
                self.racks[rackId].init_repair_start_time = self.curr_time
                for rId in failed_racks:
                    self.update_rack_repair_time(rId, len(failed_racks))

        if event_type == Rack.EVENT_REPAIR:
                for rId in failed_racks:
                    self.update_rack_repair_time(rId, len(failed_racks))
    
    #----------------------------------------------
    # update rack state
    #----------------------------------------------
    def update_rack_state(self, event_type, diskId):
        if event_type == Disk.EVENT_FAIL:
            rackId = diskId // self.sys.num_disks_per_rack
            rackGroupId = rackId // self.sys.top_n
            # if rack already fails, we don't need to fail it again.
            if self.racks[rackId].state == Rack.STATE_FAILED:
                return None
            # otherwise, we need to check if a new rack fails
            fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
            max_priority = 0
            for dId in fail_per_rack:
                # logging.info("\tdisk {} priority {}".format(diskId, self.disks[diskId].priority))
                if self.disks[dId].priority > max_priority:
                    max_priority = self.disks[dId].priority
            # logging.info("max_priority: {}  fail_per_rack: {}"
            #                 .format(max_priority, fail_per_rack))
            if max_priority > self.sys.m:
                self.racks[rackId].state = Rack.STATE_FAILED
                self.failed_racks[rackId] = 1
                self.rack_group_failures[rackGroupId] += 1
                return rackId
        
        if event_type == Rack.EVENT_FAIL:
            rackId = diskId
            rackGroupId = rackId // self.sys.top_n
            self.racks[rackId].state = Rack.STATE_FAILED
            self.failed_racks[rackId] = 1
            self.rack_group_failures[rackGroupId] += 1
            return rackId

        if event_type == Rack.EVENT_REPAIR:
            rackId = diskId
            rackGroupId = rackId // self.sys.top_n
            self.racks[rackId].state = Rack.STATE_NORMAL
            self.failed_racks.pop(rackId, None)
            for dId in self.racks[rackId].failed_disks:
                self.failed_disks.pop(dId, None)
            self.racks[rackId].failed_disks.clear()
            self.rack_group_failures[rackGroupId] -= 1
            
            for dId in self.sys.disks_per_rack[rackId]:
                self.disks[dId].state = Disk.STATE_NORMAL 
            
            self.sys.metrics.total_net_traffic += self.racks[rackId].repair_data * (self.sys.top_k + 1)
            self.sys.metrics.total_net_repair_time += self.curr_time - self.racks[rackId].init_repair_start_time
            self.sys.metrics.total_net_repair_count += 1
            return rackId

    
    def update_rack_repair_time(self, rackId, failed_racks):
        rack = self.racks[rackId]
        repaired_time = self.curr_time - rack.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            rack.curr_repair_data_remaining = rack.repair_data
        else:
            repaired_percent = repaired_time / rack.repair_time[0]
            rack.curr_repair_data_remaining = rack.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(rack.curr_repair_data_remaining)/(self.sys.diskIO * self.sys.num_disks_per_rack / failed_racks)
        rack.repair_time[0] = repair_time / 3600 / 24
        rack.repair_start_time = self.curr_time
        rack.estimate_repair_time = self.curr_time + rack.repair_time[0]
        # logging.info("calculate repair time for rack {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 rackId, repaired_time, rack.repair_time[0], rack.repair_start_time))
    
    def update_disk_repair_time_adapt(self, diskId, priority, fail_per_rack, max_priority):
        raise NotImplementedError("update_disk_repair_time_adapt() for mlecdp is not implemented")
    
    def check_pdl(self):
        return mlec_dp_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        mlecdp_repair(self.state, repair_queue)