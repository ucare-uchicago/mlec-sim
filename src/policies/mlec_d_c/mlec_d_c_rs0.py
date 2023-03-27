import logging
import time
from typing import List, Dict, Optional, Tuple
from heapq import heappush
import json
import ast
import math
from pprint import pformat
import numpy as np
import traceback
import random
import numpy as np

from helpers import mlec_d_c_prio
from components.disk import Disk
from components.spool import Spool
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_d_c_pdl
from .repair import mlec_d_c_repair

class MLEC_D_C_RS0(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = state.sys.spools

        self.affected_racks = {}
        self.priority_queue = {}
        for i in range(self.sys.top_m + 1):
            self.priority_queue[i + 1] = {}
        
        self.max_priority = 0
        self.repair_max_priority = 0
        self.total_interrack_bandwidth = state.sys.interrack_speed * state.sys.num_racks
        self.failed_spools_undetected = []

        self.racks_in_repair = {}
        self.affected_spools = {}
        self.failed_spools = {}
        self.sys_failed = False
        self.loss_trigger_diskId = -1

        self.manual_spoolId = -1
        self.manual_spool_fail = False
        self.manual_spool_fail_sample = None
    
    def update_disk_state(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.disks[diskId].no_need_to_detect = False
            spool.failed_disks.pop(diskId, None)
            spool.failed_disks_in_repair.pop(diskId, None)
            if len(spool.failed_disks) == 0:
                self.affected_spools.pop(disk.spoolId, None)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            spool.failed_disks_undetected[diskId] = 1
            self.affected_spools[disk.spoolId] = 1
        
        if event_type == Disk.EVENT_DETECT:
            spool.failed_disks_undetected.pop(diskId, None)
            spool.failed_disks_in_repair[diskId] = 1
    
    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_FAIL:
            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            # If the spool is already failing, we do nothing because it's in reconstruction anyway
            if spool.state == Spool.STATE_FAILED:
                return

        if event_type == Disk.EVENT_DETECT:
            disk.repair_start_time = self.curr_time
            disk.failure_detection_time = 0
            self.update_disk_repair_time(diskId)

        if event_type == Disk.EVENT_REPAIR:
            return
    
    def update_disk_repair_time(self, diskId):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        # logging.info("Disk %s has repaired time of %s", diskId, repaired_time)
        
        if repaired_time == 0:            
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        # Calculate the real repair rate by the dividing the total bandwidht used by k - that's the effectively write speed
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO)

        disk.repair_time[0] = repair_time / 3600 / 24

        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
    

    #----------------------------------------------
    # update diskgroup state
    #----------------------------------------------
    def update_diskgroup_state(self, event_type, diskId):
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            rackId = disk.rackId
            # if spool already fails, we don't need to fail it again.
            if spool.state == Spool.STATE_FAILED:
                disk.no_need_to_detect = True
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            # logging.info("disk id: {}  spoolid: {} spool failed disks: {}  sys.m: {}".format(
            #                 diskId, disk.spoolId, spool.failed_disks, self.sys.m))
            if len(spool.failed_disks) > self.sys.m:
                spool.state = Spool.STATE_FAILED
                spool.total_network_repair_data = self.sys.diskSize * self.sys.spool_size
                self.racks[rackId].failed_spools[spool.spoolId] = 1
                self.affected_racks[rackId] = 1
                self.failed_spools[spool.spoolId] = 1
                self.failed_spools_undetected.append(spool.spoolId)
                self.racks[rackId].failed_spools_undetected.append(spool.spoolId)
                return spool.spoolId
        
        if event_type == Disk.EVENT_DETECT:
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            if len(spool.failed_disks_in_repair) > self.sys.m:
                spool.is_in_repair = True
                return disk.spoolId
        
        if event_type == Spool.EVENT_REPAIR:
            spoolId = diskId
            spool = self.spools[spoolId]
            rackId = spool.rackId
            new_failure_intervals = self.simulation.failure_generator.gen_new_failures(len(spool.failed_disks))
            for i, failedDiskId in enumerate(spool.failed_disks):
                self.disks[failedDiskId].state = Disk.STATE_NORMAL
                self.disks[failedDiskId].failure_detection_time = 0
                self.disks[failedDiskId].no_need_to_detect = False
                disk_fail_time = new_failure_intervals[i] + self.curr_time
                if disk_fail_time < self.simulation.mission_time:
                    heappush(self.simulation.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, failedDiskId))
            
            spool.total_network_repair_data = 0
            spool.failed_disks.clear()
            spool.failed_disks_in_repair.clear()
            spool.failed_disks_undetected.clear()
            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            self.affected_spools.pop(spool.spoolId, None)
            
            rack = self.racks[rackId]
            rack.failed_spools.pop(spoolId, None)
            if len(self.racks[rackId].failed_spools) == 0:
                self.affected_racks.pop(rackId, None)
            self.failed_spools.pop(spoolId, None)
            rack.num_spools_in_repair -= 1
            if rack.num_spools_in_repair == 0:
                self.racks_in_repair.pop(rackId, None)
            return spoolId
        
        if event_type == Spool.EVENT_FASTREBUILD:
            spoolId = diskId
            return spoolId
        return None

    def update_diskgroup_priority(self, event_type, spoolId, diskId):
        spool = self.spools[spoolId]
        rackId = spool.rackId
        if event_type == Disk.EVENT_FAIL:
            spool.failure_detection_time = self.curr_time + self.sys.detection_time
            rack = self.racks[rackId]
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.pause_spool_repair_time(spId, self.repair_max_priority)
            
            # Imagine there are 2 affected racks. And a new spool fails.
            # If the current failure happens in a brand new rack, we need to increment max_priority
            # If the current failure happens in an already affected rack, but the current max-priority is 1. 
            #     it means previous 2-chunk-failure have been repaired. But the new spool failure will lead to new 2-chunk-failure stripes.
            #     so we still need to increment max_priority
            # Otherwise, we don't increase max-priority.
            if self.repair_max_priority < len(self.racks_in_repair):
                if len(rack.failed_spools_undetected) == 1:
                    self.max_priority += 1
            else:
                if len(rack.failed_spools) == 1:
                    self.max_priority += 1
            
            spool.priority = self.max_priority

            good_num = self.sys.num_spools - len(self.failed_spools)
            fail_num = len(self.failed_spools)
            spool.good_num = good_num
            spool.fail_num = fail_num
            self.compute_spool_priority_percents(spool, rackId)

            spool.failure_detection_time = self.curr_time + self.sys.detection_time

            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)

            if self.max_priority >= self.sys.num_net_fail_to_report:
                self.sys_failed = True
                self.loss_trigger_diskId = diskId
                return
        
        if event_type == Disk.EVENT_DETECT:
            rack = self.racks[rackId]

            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.pause_spool_repair_time(spId, self.repair_max_priority)
            
            self.repair_max_priority = max(self.repair_max_priority, spool.priority)
            self.priority_queue[spool.priority][spoolId] = 1
            assert self.failed_spools_undetected[0] == spoolId, "the detected spool should be the first element in the list!"
            self.failed_spools_undetected.pop(0)
            rack.failed_spools_undetected.pop(0)
            rack.num_spools_in_repair += 1
            if rack.num_spools_in_repair == 1:
                self.racks_in_repair[rackId] = 1

            spool.repair_start_time = self.curr_time
            spool.curr_prio_repair_started = False
            spool.failure_detection_time = 0
            
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)
        
        if event_type == Spool.EVENT_FASTREBUILD or event_type == Spool.EVENT_REPAIR:
            logging.info("event {}  spool {}  disk {}".format(event_type, spoolId, diskId))
            # ----------------
            # Remove the priority repair time and reduce priority because the spool is already repaired
            # Also remove it from the priority queue, and pause the repair of other spools in the queue.
            curr_priority = spool.priority
            assert curr_priority == self.repair_max_priority, "repair spool priority is not system spool repair max priority"
            del spool.repair_time[curr_priority]
            del spool.priority_percents[curr_priority]
            
            self.priority_queue[curr_priority].pop(spoolId, None)
            for spId in self.priority_queue[curr_priority]:
                self.pause_spool_repair_time(spId, curr_priority)
            
            # ----------------
            # Reduce priority because the spool has been repaired
            spool.priority -= 1
            if spool.priority > 0:
                self.priority_queue[spool.priority][spoolId] = 1

            spool.repair_start_time = self.state.curr_time
            spool.curr_prio_repair_started = False
            
            rack = self.racks[rackId]
            # update max priority when its queue is empty
            if len(self.priority_queue[self.repair_max_priority]) == 0:
                self.repair_max_priority -= 1
                self.max_priority = self.repair_max_priority

                num_racks_in_repair = len(self.racks_in_repair)
                num_failed_spools_per_rack = [0] * self.sys.num_racks
                for inrepairRackId in self.racks_in_repair:
                    num_failed_spools_per_rack[inrepairRackId] = self.racks[inrepairRackId].num_spools_in_repair

                num_undetected_spools_per_rack = [0] * self.sys.num_racks

                for undetectedDiskId in self.failed_spools_undetected:
                    undetectedDisk = self.spools[undetectedDiskId]
                    num_undetected_spools_per_rack[undetectedDisk.rackId] += 1
                    num_failed_spools_per_rack[undetectedDisk.rackId] += 1
                    if self.repair_max_priority < num_racks_in_repair:
                        if num_undetected_spools_per_rack[undetectedDisk.rackId] == 1:
                            self.max_priority += 1
                    else:
                        if num_failed_spools_per_rack[undetectedDisk.rackId] == 1:
                            self.max_priority += 1
                    undetectedDisk.priority = self.max_priority
            
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)


    
    def pause_spool_repair_time(self, spoolId, priority):
        spool = self.spools[spoolId]
        repaired_time = self.curr_time - spool.repair_start_time
        repaired_percent = repaired_time / spool.repair_time[priority]
        spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
    def compute_spool_priority_percents(self, spool, rackId):
        for i in range(spool.priority):
            priority = i+1
            spool.priority_percents[priority] = mlec_d_c_prio.compute_spool_priority_percent(self.state, self.affected_racks, rackId, priority)
        # logging.info("priority_percents: {}".format(disk.priority_percents))
    
    def resume_spool_repair_time(self, spoolId, priority, rackId):
        spool = self.spools[spoolId]
        if not spool.curr_prio_repair_started:
            priority_percent = spool.priority_percents[priority]
            spool.curr_repair_data_remaining = spool.total_network_repair_data * priority_percent
            spool.curr_prio_repair_started = True
        repair_time = self.calc_spool_repair_time(spool, priority)
        spool.repair_time[priority] = repair_time / 3600 / 24
        spool.repair_start_time = self.state.curr_time
        spool.estimate_repair_time = self.state.curr_time + spool.repair_time[priority]
    
    def calc_spool_repair_time(self, spool, priority):
        total_spool_IO = (self.sys.num_spools - len(self.failed_spools)) * self.sys.diskIO * self.sys.spool_size
        total_repair_bandwidth = min(total_spool_IO, self.total_interrack_bandwidth)

        total_repair_data_readwrite = float(spool.curr_repair_data_remaining) * (self.sys.top_k + 1) 
        # we repair multiple spools concurrently. So rebuild bandwidth is shared
        per_spool_total_repair_bandwidth = total_repair_bandwidth / len(self.priority_queue[priority])
        repair_time = total_repair_data_readwrite / per_spool_total_repair_bandwidth

        return repair_time

    def check_pdl(self):
        if self.sys_failed:
            self.generate_fail_report()
        return mlec_d_c_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        mlec_d_c_repair(self, repair_queue)
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            if not disk.no_need_to_detect:
                heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))
    
    def generate_fail_report(self):
        return
    
    def clean_failures(self):        
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            for diskId in spool.failed_disks:
                disk = self.disks[diskId]
                disk.state = Disk.STATE_NORMAL
                disk.repair_time.clear()
                disk.failure_detection_time = 0
                disk.no_need_to_detect = False

            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            spool.failure_detection_time = 0
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.failed_disks_in_repair.clear()
            spool.repair_time.clear()
            spool.total_network_repair_data = 0
            spool.priority_percents.clear()
            spool.priority = 0
            spool.curr_prio_repair_started = False

        for rackId in self.affected_racks:
            rack = self.racks[rackId]
            rack.failed_spools.clear()
            rack.failed_spools_undetected.clear()
            rack.num_spools_in_repair = 0