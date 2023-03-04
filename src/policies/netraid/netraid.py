import logging
import time
import numpy as np
from typing import Optional, List, Dict, Tuple

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack
from components.spool import Spool
from policies.policy import Policy
from .pdl import net_raid_pdl
from .repair import netraid_repair

class NetRAID(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = state.sys.spools
        self.affected_spools_per_rackGroup = state.sys.affected_spools_per_rackGroup
        self.sys_failed = False
        self.affected_spools = {}
        


    def update_disk_state(self, event_type: str, diskId: int) -> None:
        disk = self.state.disks[diskId]
        spoolId = disk.spoolId
        if event_type == Disk.EVENT_REPAIR:
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.spools[spoolId].failed_disks.pop(diskId, None)
            if len(self.spools[spoolId].failed_disks) == 0:
                self.affected_spools_per_rackGroup[disk.rackGroupId].pop(spoolId, None)
                self.affected_spools.pop(spoolId, None)
            self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
            
                        
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.spools[spoolId].failed_disks[diskId] = 1
            self.affected_spools_per_rackGroup[disk.rackGroupId][spoolId] = 1
            self.affected_spools[spoolId] = 1
            self.disks[diskId].metric_down_start_time = self.curr_time



    #----------------------------------------------
    # raid net
    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spoolId = disk.spoolId
        spool = self.spools[spoolId]

        if event_type == Disk.EVENT_REPAIR:
            # self.sys.metrics.total_net_traffic += disk.repair_data * (self.sys.top_k + 1)
            # self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.top_k + 1)
            
            # This is updating the rest of the failed disks for their repair time.
            #  If there is only one failure in the spool, this would not be run
            num_fail_in_spool = len(spool.failed_disks)
            for diskId_per_spool in spool.failed_disks:
                    self.update_disk_repair_time(diskId_per_spool, num_fail_in_spool)

        if event_type == Disk.EVENT_FAIL:
            # Note: the assignment of repair_start_time is moved into update_disk_repair_time()
            #  this is because there is a chance that we might need to delay repair
            disk.repair_start_time = self.curr_time

            num_fail_in_spool = len(spool.failed_disks)
            if num_fail_in_spool > self.sys.top_m:
                self.sys_failed = True
                return

            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the rebuild IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            for diskId_per_spool in spool.failed_disks:
                self.update_disk_repair_time(diskId_per_spool, num_fail_in_spool)
    
    
    def update_disk_repair_time(self, diskId, num_fail_in_spool):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            # This means that the repair is on going, we need to update the remaining data
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_in_spool)
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))


        
    def check_pdl(self):
        return net_raid_pdl(self, self.state)
    
    def update_repair_events(self, repair_queue):
        netraid_repair(self.state, repair_queue)
    
    def clean_failures(self) -> None:
        affected_rack_groups = {}
        for spoolId in self.affected_spools:
            for diskId in self.spools[spoolId].failed_disks:
                disk = self.state.disks[diskId]
                disk.state = Disk.STATE_NORMAL
                disk.repair_time = {}
                self.curr_prio_repair_started = False
            spool = self.spools[spoolId]
            spool.failed_disks = {}
            affected_rack_groups[spool.rackGroupId] = 1
        for rackGroupId in affected_rack_groups:
            self.affected_spools_per_rackGroup[rackGroupId] = {}
        