import logging
import math
import json
from heapq import heappush

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
            spool.failed_disks_undetected[diskId] = 1

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

        if event_type == Disk.EVENT_DETECT:
            #-----------------------------------------------------
            # calculate repairTime and update priority for decluster
            #-----------------------------------------------------
            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.pause_disk_repair_time(dId, spool.disk_repair_max_priority)
            

            spool.disk_repair_max_priority = max(spool.disk_repair_max_priority, disk.priority)
            spool.disk_priority_queue[disk.priority][diskId] = 1
            spool.failed_disks_undetected.pop(diskId, None)

            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False
            disk.failure_detection_time = 0
            
            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)

        if event_type == Disk.EVENT_FAIL:
            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.pause_disk_repair_time(dId, spool.disk_repair_max_priority)

            spool.disk_max_priority += 1
            disk.priority = spool.disk_max_priority

            fail_num = len(spool.failed_disks)
            good_num = self.sys.spool_size - fail_num
            disk.good_num = good_num
            disk.fail_num = fail_num
            self.compute_priority_percents(disk)

            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            # logging.info("disk.failure_detection_time: {}".format(disk.failure_detection_time))

            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)
            
            #----------------------------------------------
            if spool.disk_max_priority >= self.sys.num_local_fail_to_report:
                self.sys_failed = True
                sys_survive_prob = 0
                if self.sys.num_local_fail_to_report > self.sys.m:
                    sys_survive_prob = (1-disk.priority_percents[disk.priority])**self.sys.num_chunks_per_disk
                    if sys_survive_prob > 0.1:
                        print("sys_survive_prob is as large as {}.. Please report to Meng!".format(sys_survive_prob))
                if self.sys.collect_fail_reports:
                    fail_report = {'curr_time': self.curr_time, 'disk_infos': []}
                    for failedDiskId in self.failed_disks:
                        failedDisk = self.disks[failedDiskId]
                        
                        fail_report['disk_infos'].append(
                            {
                            'curr_repair_data_remaining': failedDisk.curr_repair_data_remaining,
                            'diskId': int(failedDiskId),
                            'priority': int(failedDisk.priority),
                            'estimate_repair_time': failedDisk.estimate_repair_time,
                            'repair_start_time': failedDisk.repair_start_time,
                            'failure_detection_time': failedDisk.failure_detection_time,
                            'repair_time': json.dumps(failedDisk.repair_time),
                            'priority_percents': json.dumps(failedDisk.priority_percents)
                            })
                    # logging.info('new fail report: {}'.format(fail_report))
                    self.sys.fail_reports.append(fail_report)
                return
            
                        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            curr_priority = disk.priority
            assert curr_priority == spool.disk_repair_max_priority, "repair disk priority is not spool disk repair max priority"
            del disk.repair_time[curr_priority]
            del disk.priority_percents[curr_priority]

            spool.disk_priority_queue[curr_priority].pop(diskId, None)
            for dId in spool.disk_priority_queue[curr_priority]:
                self.pause_disk_repair_time(dId, curr_priority)
            
            disk.priority -= 1
            if disk.priority > 0:
                spool.disk_priority_queue[disk.priority][diskId] = 1

            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False

            if len(spool.disk_priority_queue[spool.disk_repair_max_priority]) == 0:
                spool.disk_repair_max_priority -= 1
                spool.disk_max_priority -= 1
                for dId in spool.failed_disks_undetected:
                    failedDisk = self.disks[dId]
                    failedDisk.priority -= 1
                    # TODO: improve percents computation. Add priority n+1' percents to 1?
            
            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)


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
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

    

    def clean_failures(self):
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            for diskId in spool.failed_disks:
                disk = self.disks[diskId]
                disk.state = Disk.STATE_NORMAL
                disk.priority = 0
                disk.failure_detection_time = 0
                disk.repair_time = {}
                disk.priority_percents = {}
                disk.curr_prio_repair_started = False
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.disk_repair_max_priority = 0
            spool.disk_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()
    
    def manual_inject_failures(self, fail_report, simulate):
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.priority = int(disk_info['priority'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            disk.failure_detection_time = float(disk_info['failure_detection_time'])

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
            
            priority_percents = json.loads(disk_info['priority_percents'])
            for key, value in priority_percents.items():
                disk.priority_percents[int(key)] = float(value)

            self.failed_disks[diskId] = 1
            spool = self.spools[disk.spoolId]
            spool.failed_disks[diskId] = 1
            self.affected_spools[disk.spoolId] = 1

            spool.disk_max_priority = max(disk.priority, spool.disk_max_priority)
            
            if disk.failure_detection_time >= simulate.curr_time:
                disk.curr_prio_repair_started = False
                spool.failed_disks_undetected[diskId] = 1
                # logging.info("found undetected disk {} on spool {}".format(diskId, disk.spoolId))
            else:
                disk.curr_prio_repair_started = True
                spool.disk_priority_queue[disk.priority][diskId] = 1
                spool.disk_repair_max_priority = max(disk.priority, spool.disk_repair_max_priority)

        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            if spool.disk_repair_max_priority > 0:
                for diskId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    disk = self.disks[diskId]
                    if disk.priority > 1:
                        heappush(simulate.repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))
                    if disk.priority == 1:
                        heappush(simulate.repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
                
            for diskId in spool.failed_disks_undetected:
                disk = self.disks[diskId]
                heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))
        #         logging.info("heappush detection for diskId {}".format(diskId))
        # logging.info("failure queue after manual failure injection: {}".format(self.simulation.failure_queue))