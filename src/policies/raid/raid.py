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
        super().__init__(state)
        state.failures_per_raidgroup = {}
    

    def update_disk_state(self, event_type: str, diskId: int):
        rackId = diskId // self.sys.num_disks_per_rack
        disk = self.state.disks[diskId]
        if event_type == Disk.EVENT_REPAIR:
            # logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            self.state.failed_disks.pop(diskId, None)

            raidgroupId = diskId // self.state.n
            self.state.failures_per_raidgroup[raidgroupId] -= 1
            if self.state.failures_per_raidgroup[raidgroupId] == 0:
                self.state.failures_per_raidgroup.pop(raidgroupId, None)

            self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
            
            # logging.info("Network bandwidth after replenish: %s", self.state.network.__dict__)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.state.failed_disks[diskId] = 1

            raidgroupId = diskId // self.state.n
            if raidgroupId in self.state.failures_per_raidgroup:
                self.state.failures_per_raidgroup[raidgroupId] += 1
            else:
                self.state.failures_per_raidgroup[raidgroupId] = 1
            # print("disk {} f {}. Now self.state.failed_disks: {}, self.state.failures_per_raidgroup: {}".format(
            #             diskId, self.curr_time, self.state.failed_disks, self.state.failures_per_raidgroup))

            self.disks[diskId].metric_down_start_time = self.curr_time
    

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
        # logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

    
    def update_disk_repair_time_adapt(self, diskId, fail_per_rack):
        disk = self.disks[diskId]
        rackId = diskId // self.sys.num_disks_per_rack
        rack = self.racks[rackId]
        stripesetId = (diskId % self.sys.num_disks_per_rack) // self.n
        repair_time = float(disk.repair_data)/(self.sys.diskIO)

        disk.repair_time[0] = repair_time / 3600 / 24
        # disk.repair_start_time = max(self.curr_time, rack.stripesets_repair_finish[stripesetId])
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]*fail_per_rack
        # rack.stripesets_repair_finish[stripesetId] = disk.estimate_repair_time
    
    def check_pdl(self):
        return flat_cluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        return raid_repair(self.state, repair_queue)

    def clean_failures(self):
        failed_disks = self.state.get_failed_disks()
        for diskId in failed_disks:
            disk = self.state.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}