import logging
import time
from typing import List, Dict, Optional, Tuple

from components.disk import Disk
from components.spool import Spool
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_c_c_pdl
from .repair import mlec_c_c_repair

class MLEC_C_C_RS1(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = state.sys.spools
        self.mpools = state.sys.mpools
        self.rackgroups = state.sys.rackgroups
        
        self.affected_spools = {}
        self.affected_rackgroups = {}
        self.sys_failed = False
        

    def update_disk_state(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            spool.failed_disks.pop(diskId, None)
            if len(spool.failed_disks) == 0:
                self.affected_spools.pop(disk.spoolId, None)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            self.affected_spools[disk.spoolId] = 1


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type in [Disk.EVENT_FAIL]:
            # If the spool is already failing, we do nothing
            if spool.state == Spool.STATE_FAILED:
                # logging.info("Diskgroup already in failed state, ignoring")
                return
            
            num_fail_per_spool = len(spool.failed_disks)
            if num_fail_per_spool > self.sys.m:
                return
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the read IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            disk.repair_start_time = self.curr_time
            for failedDiskId in spool.failed_disks:
                self.update_disk_repair_time(failedDiskId, num_fail_per_spool)


        # if disk repair event
        if event_type == Disk.EVENT_REPAIR:      
            # # It should be impossible to repair a disk in a failed spool   
            # if spool.state == Spool.STATE_FAILED:
            #     return
            num_fail_per_spool = len(spool.failed_disks)
            if num_fail_per_spool == 0:
                return
            for failedDiskId in spool.failed_disks:
                self.update_disk_repair_time(failedDiskId, num_fail_per_spool)


    def update_disk_repair_time(self, diskId, num_fail_per_spool):
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
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / num_fail_per_spool)

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
            # if spool already fails, we don't need to fail it again.
            if spool.state == Spool.STATE_FAILED:
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            if len(spool.failed_disks) > self.sys.m:
                # print('spool failure!!!')
                # logging.error("Diskgroup %s failed due to the disk failure, it has failed disks %s", diskgroupId, self.get_failed_disks_per_diskgroup(diskgroupId))
                spool.state = Spool.STATE_FAILED
                mpool = self.mpools[spool.mpoolId]
                mpool.failed_spools[spool.spoolId] = 1
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools[mpool.mpoolId] = 1
                self.affected_rackgroups[rackgroup.rackgroupId] = 1
                return spool.spoolId

        if event_type == Spool.EVENT_REPAIR:
            spoolId = diskId
            spool = self.spools[spoolId]
            # logging.info("Diskgroup %s is repaired", diskId)
            for failedDiskId in spool.failed_disks:
                self.disks[failedDiskId].state = Disk.STATE_NORMAL
            spool.failed_disks.clear()
            spool.state = Spool.STATE_NORMAL
            self.affected_spools.pop(spool.spoolId, None)
            mpool = self.mpools[spool.mpoolId]
            mpool.failed_spools.pop(spool.spoolId, None)
            if len(mpool.failed_spools) == 0:
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools.pop(mpool.rackgroupId, None)
                if len(rackgroup.affected_mpools) == 0:
                    self.affected_rackgroups.pop(rackgroup.rackgroupId)
            return spoolId
        return None


    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_diskgroup_priority(self, event_type, spoolId, diskId):
        spool = self.spools[spoolId]
        mpool = self.mpools[spool.mpoolId]
        if event_type == Disk.EVENT_FAIL:
            spool.repair_start_time = self.curr_time
            num_failed_spools_per_mpool = len(mpool.failed_spools)
            if num_failed_spools_per_mpool > self.sys.top_m:
                self.sys_failed = True
                return
            
            rackgroup = self.rackgroups[mpool.rackgroupId]
            if num_failed_spools_per_mpool > 1:
                # this mpool is already in repair. So no need to update other mpools' repair time
                mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))
                for failedSpoolId in mpool.failed_spools:
                    self.update_spool_repair_time(failedSpoolId, num_failed_spools_per_mpool, mpool_repair_rate)
            else:
                # this mpool is now in repair, which is goind to steal network bandwidth from other mpools in the same rackgroup
                # therefore, we need to update network bandwidth for all mpools in repair in this rackgroup
                mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))
                for mpoolId in rackgroup.affected_mpools:
                    affected_mpool = self.mpools[mpoolId]
                    for failedSpoolId in affected_mpool.failed_spools:
                        num_failed_spools_in_affected_mpool = len(affected_mpool.failed_spools)
                        self.update_spool_repair_time(failedSpoolId, num_failed_spools_in_affected_mpool, mpool_repair_rate)
                

        if event_type == spool.EVENT_REPAIR:
            num_failed_spools_per_mpool = len(mpool.failed_spools)
            rackgroup = self.rackgroups[mpool.rackgroupId]
            if num_failed_spools_per_mpool > 0:
                mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))
                for failedSpoolId in mpool.failed_spools:
                    self.update_spool_repair_time(failedSpoolId, num_failed_spools_per_mpool, mpool_repair_rate)
            else:
                if len(rackgroup.affected_mpools) > 0:
                    mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))
                    for mpoolId in rackgroup.affected_mpools:
                        affected_mpool = self.mpools[mpoolId]
                        for failedSpoolId in affected_mpool.failed_spools:
                            num_failed_spools_in_affected_mpool = len(affected_mpool.failed_spools)
                            self.update_spool_repair_time(failedSpoolId, num_failed_spools_in_affected_mpool, mpool_repair_rate)
    
    
    def update_spool_repair_time(self, spoolId, num_failed_spools_per_mpool, mpool_repair_rate):
        spool = self.spools[spoolId]
        repaired_time = self.curr_time - spool.repair_start_time
        if repaired_time == 0:            
            repaired_percent = 0
            spool.curr_repair_data_remaining = len(spool.failed_disks) * self.sys.diskSize
        else:
            repaired_percent = repaired_time / spool.repair_time[0]
            spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(spool.curr_repair_data_remaining)/(mpool_repair_rate / num_failed_spools_per_mpool)
        # print(repair_time)
            
        spool.repair_time[0] = repair_time / 3600 / 24
        spool.repair_start_time = self.curr_time
        spool.estimate_repair_time = self.curr_time + spool.repair_time[0]
        # print("spool.repair_time:{}".format(spool.repair_time[0]))


    def check_pdl(self):
        return mlec_c_c_pdl(self)
    
    def update_repair_events(self, repair_queue):
        mlec_c_c_repair(self, repair_queue)


    def clean_failures(self):
        affected_spools = {}
        
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            for diskId in spool.failed_disks:
                disk = self.disks[diskId]
                disk.state = Disk.STATE_NORMAL
                disk.priority = 0
                disk.repair_time = {}
            spool.failed_disks.clear()
    
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                for spoolId in mpool.failed_spools:
                    self.spools[spoolId].state = Spool.STATE_NORMAL
                mpool.failed_spools.clear()
            rackgroup.affected_mpools.clear()
