from disk import Disk
import operator as op
import numpy as np
import logging
from functools import reduce
from rack import Rack

class RAID:
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
                rackId = diskId // self.sys.num_disks_per_rack
                if rackId in updated_racks:
                    continue
                updated_racks[rackId] = 1
                if self.racks[rackId].state == Rack.STATE_FAILED:
                    logging.info("update_priority(): rack {} is failed. Event type: {}".format(rackId, event_type))
                    continue
                fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
                #  what if there are multiple racks
                if len(fail_per_rack) > 0:
                    if self.sys.place_type == 0:
                        if not self.sys.adapt:
                            for diskId in fail_per_rack:
                                self.update_disk_repair_time(diskId, len(fail_per_rack))

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
                    #--------------------------------------------
                    # calculate repair time for cluster placement
                    #--------------------------------------------
                    if self.sys.place_type == 0:
                        if len(new_failures) > 0:
                            for diskId in new_failures:
                                self.disks[diskId].repair_start_time = self.curr_time
                            if self.sys.adapt:
                                for diskId in new_failures:
                                    self.update_disk_repair_time_adapt(diskId, len(fail_per_rack))
                            else:
                                for diskId in fail_per_rack:
                                    self.update_disk_repair_time(diskId, len(fail_per_rack))
        
        

    def update_disk_repair_time(self, diskId, fail_per_rack):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_rack)
        # if repaired_percent > 0 and (fail_per_rack > 1  or 
        #     disk.repair_time[0] != float(disk.curr_repair_data_remaining)/self.sys.diskIO):
        #     print("fail_per_rack {}  old repair time: {}  old repair time:{}  new repair time: {} new finish time {}".format(
        #         fail_per_rack, disk.repair_time[0], disk.repair_time[0] + disk.repair_start_time, repair_time / 3600 / 24,
        #         repair_time / 3600 / 24 + self.curr_time
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

    
    def update_disk_repair_time_adapt(self, diskId, fail_per_rack):
        disk = self.disks[diskId]
        rackId = diskId // self.sys.num_disks_per_rack
        rack = self.racks[rackId]
        stripesetId = (diskId % self.sys.num_disks_per_rack) // self.n
        repair_time = float(disk.repair_data)/(self.sys.diskIO)
        # if repaired_percent > 0 and (fail_per_rack > 1  or 
        #     disk.repair_time[0] != float(disk.curr_repair_data_remaining)/self.sys.diskIO):
        #     print("fail_per_rack {}  old repair time: {}  old repair time:{}  new repair time: {} new finish time {}".format(
        #         fail_per_rack, disk.repair_time[0], disk.repair_time[0] + disk.repair_start_time, repair_time / 3600 / 24,
        #         repair_time / 3600 / 24 + self.curr_time
        #     ))
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = max(self.curr_time, rack.stripesets_repair_finish[stripesetId])
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        rack.stripesets_repair_finish[stripesetId] = disk.estimate_repair_time
        