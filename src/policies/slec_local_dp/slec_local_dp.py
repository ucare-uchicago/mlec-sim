import logging
import math

from components.disk import Disk
from components.rack import Rack
from policies.policy import Policy
from helpers.common_math import ncr
from .pdl import slec_local_dp_pdl
from .repair import slec_local_dp_repair


class SLEC_LOCAL_DP(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.affected_spools = {}
        self.sys_failed = False

    
    def update_disk_state(self, event_type: str, diskId: int):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.failed_disks[diskId] = 1
            spool.failed_disks[diskId] = 1
            self.affected_spools[disk.spoolId] = 1

        if event_type == Disk.EVENT_REPAIR:
            # logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            self.failed_disks.pop(diskId, None)
            spool.failed_disks.pop(diskId, None)
            if len(spool.failed_disks) == 0:
                self.affected_spools.pop(disk.spoolId, None)

    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_FAIL:
            #-----------------------------------------------------
            # calculate repairTime and update priority for decluster
            #-----------------------------------------------------
            fail_num = len(spool.failed_disks)
            good_num = self.sys.spool_size - fail_num
            disk.good_num = good_num
            disk.fail_num = fail_num

            if spool.disk_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_max_priority]:
                    self.pause_disk_repair_time(dId, spool.disk_max_priority)
            #----------------------------------------------
            spool.disk_max_priority += 1
            disk.priority = spool.disk_max_priority
            if disk.priority >= self.sys.num_local_fail_to_report:
                self.sys_failed = True
                return
            #----------------------------------------------
            spool.disk_priority_queue[disk.priority][diskId] = 1
            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False
            self.compute_priority_percents(disk)

            for dId in spool.disk_priority_queue[disk.priority]:
                self.resume_repair_time(dId, disk.priority, spool)
                        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            curr_priority = disk.priority
            assert curr_priority == spool.disk_max_priority, "repair disk priority is not spool disk max priority"
            del disk.repair_time[curr_priority]

            spool.disk_priority_queue[curr_priority].pop(diskId, None)
            for dId in spool.disk_priority_queue[curr_priority]:
                self.pause_disk_repair_time(dId, curr_priority)
            
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

        # logging.info("repair time: {}".format(disk.repair_time[priority]))




    def check_pdl(self):
        return slec_local_dp_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        slec_local_dp_repair(self, repair_queue)
    

    def clean_failures(self):
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