from disk import Disk
import logging

class NetRAID:
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

    #----------------------------------------------
    # raid net
    #----------------------------------------------

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
        if event_type == Disk.EVENT_REPAIR:
            disk = self.disks[diskId]
            self.sys.metrics.total_net_traffic += disk.repair_data * (self.sys.top_k + 1)
            self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.top_k + 1)
            stripesetId = disk.stripesetId
            failed_disks_per_stripeset = self.state.get_failed_disks_per_stripeset(stripesetId)
            for diskId in failed_disks_per_stripeset:
                self.update_disk_repair_time(diskId)

        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            disk.repair_start_time = self.curr_time

            failed_disks_per_stripeset = self.state.get_failed_disks_per_stripeset(disk.stripesetId)
            logging.info("  update_disk_priority_raid_net event: {} stripesetId: {} failed_disks_per_stripeset: {}".format(
                            event_type, disk.stripesetId, failed_disks_per_stripeset))
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the rebuild IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            for diskId_per_stripeset in failed_disks_per_stripeset:
                self.update_disk_repair_time(diskId_per_stripeset)
    
    
    def update_disk_repair_time(self, diskId):
        disk = self.disks[diskId]
        fail_per_stripeset = len(self.state.get_failed_disks_per_stripeset(disk.stripesetId))

        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_stripeset)
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))
