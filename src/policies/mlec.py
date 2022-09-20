from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from rack import Rack
import time

class MLEC:
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
        self.mytimer = state.mytimer

        self.failed_racks_per_stripeset = []
        for i in range(self.sys.num_rack_stripesets):
            self.failed_racks_per_stripeset.append({})

    def update_state(self, event_type, diskset):
        for diskId in diskset:
            rackId = diskId // self.sys.num_disks_per_rack
            if event_type == Disk.EVENT_REPAIR:
                self.disks[diskId].state = Disk.STATE_NORMAL
                self.racks[rackId].failed_disks.pop(diskId, None)
                self.failed_disks.pop(diskId, None)
                
                
            if event_type == Disk.EVENT_FAIL:
                self.disks[diskId].state = Disk.STATE_FAILED
                self.racks[rackId].failed_disks[diskId]=1
                self.failed_disks[diskId] = 1


    #----------------------------------------------
    def update_priority(self, event_type, diskset):
        updated_racks = {}
        if event_type == Disk.EVENT_REPAIR:
            for diskId in diskset:
                disk = self.disks[diskId]
                self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.k + 1)
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
                    if self.sys.place_type == 2:
                        for diskId in fail_per_rack:
                            self.update_disk_repair_time(diskId, len(fail_per_rack))
        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                start = time.time()
                rackId = diskId // self.sys.num_disks_per_rack

                if self.racks[rackId].state == Rack.STATE_FAILED:
                    # logging.info("update_priority(): rack {} is failed".format(rackId))
                    continue
                if rackId in updated_racks:
                    continue

                updated_racks[rackId] = 1
                
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                new_failures = set(fail_per_rack).intersection(set(diskset))
                if len(new_failures) > 0:
                    #--------------------------------------------
                    # calculate repair time for mlec cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 2:
                            for diskId in new_failures:
                                self.disks[diskId].repair_start_time = self.curr_time
                            end = time.time()
                            self.mytimer.updatePriorityFailTime += end - start
                            for diskId in fail_per_rack:
                                self.update_disk_repair_time(diskId, len(fail_per_rack))

    def update_disk_repair_time(self, diskId, fail_per_rack):
        start = time.time()
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_rack)

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("calculate repair time for disk {}  repaired time: {} remaining repair time: {} repair_start_time: {}".format(
        #                 diskId, repaired_time, disk.repair_time[0], disk.repair_start_time))

        end = time.time()
        self.mytimer.updateDiskRepairTime += end - start
        logging.info("  update_disk_repair_time for disk {} fail_per_rack {} repair time {}".format(
                        diskId, fail_per_rack, disk.repair_time[0]))



    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_rack_priority(self, event_type, new_failed_racks, rackset):
        failed_racks = self.state.get_failed_racks()
        if event_type == Disk.EVENT_FAIL:
                for rackId in new_failed_racks:
                    self.racks[rackId].repair_start_time = self.curr_time
                    self.racks[rackId].init_repair_start_time = self.curr_time
                for rackId in failed_racks:
                    failed_racks_per_stripeset = self.get_failed_racks_per_stripesetRackId(rackId)
                    self.update_rack_repair_time(rackId, len(failed_racks_per_stripeset))

        if event_type == Rack.EVENT_FAIL:
                for rackId in new_failed_racks:
                    self.racks[rackId].repair_start_time = self.curr_time
                    self.racks[rackId].init_repair_start_time = self.curr_time
                for rackId in failed_racks:
                    failed_racks_per_stripeset = self.get_failed_racks_per_stripesetRackId(rackId)
                    self.update_rack_repair_time(rackId, len(failed_racks_per_stripeset))

        if event_type == Rack.EVENT_REPAIR:
                for rackId in failed_racks:
                    failed_racks_per_stripeset = self.get_failed_racks_per_stripesetRackId(rackId)
                    self.update_rack_repair_time(rackId, len(failed_racks_per_stripeset))
    
    #----------------------------------------------
    # update rack state
    #----------------------------------------------
    def update_rack_state(self, event_type, diskset):
        new_rack_failures = []
        if event_type == Disk.EVENT_FAIL:
            for diskId in diskset:
                rackId = diskId // self.sys.num_disks_per_rack
                rackStripesetId = rackId // self.sys.top_n
                # if rack already fails, we don't need to fail it again.
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    continue
                # otherwise, we need to check if a new rack fails
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                if len(fail_per_rack) > self.sys.m:
                    if rackId not in new_rack_failures:
                        new_rack_failures.append(rackId)
                    self.racks[rackId].state = Rack.STATE_FAILED
                    self.failed_racks[rackId] = 1
                    self.failed_racks_per_stripeset[rackStripesetId][rackId] = 1
                    break
        
        if event_type == Rack.EVENT_FAIL:
            rackset = diskset
            for rackId in rackset:
                rackStripesetId = rackId // self.sys.top_n
                self.racks[rackId].state = Rack.STATE_FAILED
                new_rack_failures.append(rackId)
                self.failed_racks[rackId] = 1
                self.failed_racks_per_stripeset[rackStripesetId][rackId] = 1

        if event_type == Rack.EVENT_REPAIR:
            rackset = diskset
            for rackId in rackset:
                rackStripesetId = rackId // self.sys.top_n
                self.racks[rackId].state = Rack.STATE_NORMAL
                self.failed_racks.pop(rackId, None)
                self.failed_racks_per_stripeset[rackStripesetId].pop(rackId, None)
                
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                for diskId in fail_per_rack:
                    self.failed_disks.pop(diskId, None)
                    
                self.racks[rackId].failed_disks.clear()
                
                for diskId in self.sys.disks_per_rack[rackId]:
                    self.disks[diskId].state = Disk.STATE_NORMAL 
                
                self.sys.metrics.total_net_traffic += self.racks[rackId].repair_data * (self.sys.top_k + 1)
                self.sys.metrics.total_net_repair_time += self.curr_time - self.racks[rackId].init_repair_start_time
                self.sys.metrics.total_net_repair_count += 1

        return new_rack_failures
    
    def update_rack_repair_time(self, rackId, failed_racks_per_stripeset):
        rack = self.racks[rackId]
        repaired_time = self.curr_time - rack.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            rack.curr_repair_data_remaining = rack.repair_data
        else:
            
            repaired_percent = repaired_time / rack.repair_time[0]
            rack.curr_repair_data_remaining = rack.curr_repair_data_remaining * (1 - repaired_percent)
        # repair_time = float(rack.curr_repair_data_remaining)/(self.sys.diskIO * self.n / failed_racks_per_stripeset)
        repair_time = float(rack.curr_repair_data_remaining)/(self.sys.diskIO * (self.sys.m + 1) / failed_racks_per_stripeset)
        rack.repair_time[0] = repair_time / 3600 / 24
        rack.repair_start_time = self.curr_time
        rack.estimate_repair_time = self.curr_time + rack.repair_time[0]
        logging.info("  update_rack_repair_time for rack {} failed_racks_per_stripeset {} repair time {}".format(
                        rackId, failed_racks_per_stripeset, rack.repair_time[0]))

    
    def get_failed_racks_per_stripesetRackId(self, rackId):
        rackStripesetId = rackId // self.sys.top_n
        return list(self.failed_racks_per_stripeset[rackStripesetId].keys())
    
    def get_failed_racks_per_stripeset(self, rackStripesetId):
        return list(self.failed_racks_per_stripeset[rackStripesetId].keys())