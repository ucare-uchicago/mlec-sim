import logging
from components.disk import Disk
from components.rack import Rack
from policies.policy import Policy
from .pdl import flat_cluster_pdl
from .repair import raid_repair

class RAID(Policy):
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

    def update_disk_priority(self, event_type, diskId):
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            rackId = diskId // self.sys.num_disks_per_rack
            if self.racks[rackId].state == Rack.STATE_FAILED:
                # logging.info("update_disk_priority(): rack {} is failed. Event type: {}".format(rackId, event_type))
                return
            fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
            if not self.sys.adapt:
                for dId in fail_per_rack:
                    self.update_disk_repair_time(dId, len(fail_per_rack))

        if event_type == Disk.EVENT_FAIL:
            rackId = diskId // self.sys.num_disks_per_rack
            if self.racks[rackId].state == Rack.STATE_FAILED:
                # logging.info("update_disk_priority(): rack {} is failed".format(rackId))
                return
            fail_per_rack = self.state.get_failed_disks_per_rack(rackId)
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the rebuild IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            self.disks[diskId].repair_start_time = self.curr_time
            if self.sys.adapt:
                self.update_disk_repair_time_adapt(diskId, len(fail_per_rack))
            else:
                for dId in fail_per_rack:
                    self.update_disk_repair_time(dId, len(fail_per_rack))
        
        

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

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = max(self.curr_time, rack.stripesets_repair_finish[stripesetId])
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        rack.stripesets_repair_finish[stripesetId] = disk.estimate_repair_time
    
    def check_pdl(self):
        return flat_cluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        return raid_repair(self.state, repair_queue)
