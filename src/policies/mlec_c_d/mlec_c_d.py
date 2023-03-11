import logging
import time
import math
from typing import List, Dict, Optional, Tuple

from components.disk import Disk
from components.spool import Spool
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_c_d_pdl
from .repair import mlec_c_d_repair

class MLEC_C_D(Policy):
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

        if event_type == Disk.EVENT_FAIL:
            # If the spool is already failing, we do nothing
            if spool.state == Spool.STATE_FAILED:
                return
            #-----------------------------------------------------
            # calculate repairT and update priority for decluster
            #-----------------------------------------------------
            #----------------------------------------------
            fail_num = len(spool.failed_disks)
            good_num = self.sys.spool_size - fail_num
            disk.good_num = good_num
            disk.fail_num = fail_num

            if spool.disk_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_max_priority]:
                    logging.info('pausing for disk {} with priority {}  spool max priority {}'.format(
                            dId, self.disks[diskId], spool.disk_max_priority
                    ))
                    self.pause_disk_repair_time(dId, spool.disk_max_priority)
            #----------------------------------------------
            spool.disk_max_priority += 1
            if spool.disk_max_priority > self.sys.m:
                return
            #----------------------------------------------
            disk.priority = spool.disk_max_priority
            spool.disk_priority_queue[disk.priority][diskId] = 1
            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False
            self.compute_priority_percents(disk)


            for dId in spool.disk_priority_queue[disk.priority]:
                self.resume_repair_time(dId, disk.priority, spool)
                # logging.info("===")
                        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            curr_priority = disk.priority
            del disk.repair_time[curr_priority]

            spool.disk_priority_queue[curr_priority].pop(diskId, None)
            for dId in spool.disk_priority_queue[curr_priority]:
                self.pause_repair_time(dId, curr_priority)
            
            disk.priority -= 1
            if disk.priority > 0:
                spool.disk_priority_queue[disk.priority][diskId] = 1

            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False

            if len(spool.disk_priority_queue[spool.disk_max_priority]) == 0:
                spool.disk_max_priority -= 1      
            
            if spool.disk_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_max_priority]:
                    self.resume_repair_time(dId, spool.disk_max_priority, spool)


    def compute_priority_percents(self, disk):
        good_num = disk.good_num
        fail_num = disk.fail_num
        for i in range(disk.priority):
            priority = i+1
            priority_sets = math.comb(good_num, self.sys.n-priority)*math.comb(fail_num-1, priority-1)
            total_sets = math.comb((good_num+fail_num-1), (self.sys.n-1))
            disk.priority_percents[priority] = float(priority_sets)/total_sets
        
    def pause_disk_repair_time(self, diskId, priority):
        disk = self.state.disks[diskId]
        repaired_time = self.state.curr_time - disk.repair_start_time
        repaired_percent = repaired_time / disk.repair_time[priority]
        disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
    
    def resume_repair_time(self, diskId, priority, spool):
        disk = self.disks[diskId]
        if not disk.curr_prio_repair_started:
            priority_percent = disk.priority_percents[priority]
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            disk.curr_prio_repair_started = True
        good_num = self.sys.spool_size - len(spool.failed_disks)
        repair_time = disk.curr_repair_data_remaining * (self.sys.k+1) / (self.sys.diskIO * good_num / len(spool.disk_priority_queue[priority]))
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.state.curr_time
        disk.estimate_repair_time = self.state.curr_time + disk.repair_time[priority]

    def update_disk_repair_time(self, diskId, priority, fail_per_rack):
        disk = self.disks[diskId]
        good_num = disk.good_num
        fail_num = disk.fail_num
        #----------------------------
        repaired_time = self.curr_time - disk.repair_start_time
        # print("disk {}  priority {}  repair time {}".format(diskId, priority, disk.repair_time))
        if repaired_time == 0:
            priority_sets = math.comb(good_num, self.n-priority)*math.comb(fail_num-1, priority-1)
            total_sets = math.comb((good_num+fail_num-1), (self.n-1)) 
            priority_percent = float(priority_sets)/total_sets
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            if priority > 1:
                self.sys.metrics.total_rebuild_io_per_year -= disk.curr_repair_data_remaining * (priority - 1) * self.sys.k

        else:
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        #----------------------------------------------------
        parallelism = good_num
        #----------------------------------------------------
        amplification = self.sys.k + 1
        if priority < fail_per_rack:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/fail_per_rack)
        else:
            repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
        #----------------------------------------------------
        self.disks[diskId].repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[priority]
        #----------------------------------------------------


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
            if spool.disk_max_priority > self.sys.m:
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
                failed_disk = self.disks[failedDiskId]
                failed_disk.state = Disk.STATE_NORMAL
                failed_disk.priority = 0
            spool.failed_disks.clear()
            spool.state = Spool.STATE_NORMAL
            spool.disk_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()
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
            spool.curr_repair_data_remaining = self.sys.spool_size * self.sys.diskSize
        else:
            repaired_percent = repaired_time / spool.repair_time[0]
            spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(spool.curr_repair_data_remaining)/(mpool_repair_rate / num_failed_spools_per_mpool)
            
        spool.repair_time[0] = repair_time / 3600 / 24
        spool.repair_start_time = self.curr_time
        spool.estimate_repair_time = self.curr_time + spool.repair_time[0]


    def check_pdl(self):
        return mlec_c_d_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        mlec_c_d_repair(self, repair_queue)


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
            spool.disk_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()
    
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                for spoolId in mpool.failed_spools:
                    self.spools[spoolId].state = Spool.STATE_NORMAL
                mpool.failed_spools.clear()
            rackgroup.affected_mpools.clear()
