import logging
import time
from typing import List, Dict, Optional, Tuple
from heapq import heappush
import json
import ast
import math
from pprint import pformat
import numpy as np
import traceback
import random
import numpy as np

from components.disk import Disk
from components.spool import Spool
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_c_sodp_pdl
from .repair import mlec_c_sodp_repair

class MLEC_C_SODP_RS0(Policy):
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
        self.loss_trigger_diskId = -1

        self.manual_spoolId = -1
        self.manual_spool_fail = False
        self.manual_spool_fail_sample = None
        

    def update_disk_state(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.disks[diskId].no_need_to_detect = False
            spool.failed_disks.pop(diskId, None)
            if len(spool.failed_disks) == 0:
                self.affected_spools.pop(disk.spoolId, None)
                self.mpools[spool.mpoolId].affected_spools.pop(disk.spoolId, None)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            spool.failed_disks_undetected[diskId] = 1
            self.affected_spools[disk.spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[disk.spoolId] = 1
        
        if event_type == Disk.EVENT_DETECT:
            spool.failed_disks_undetected.pop(diskId, None)


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_FAIL:
            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            # If the spool is already failing, we do nothing because it's in reconstruction anyway
            if spool.state == Spool.STATE_FAILED:
                # logging.info("Diskgroup already in failed state, ignoring")
                return

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
            
            mpool = self.mpools[spool.mpoolId]
            if len(mpool.failed_spools) >= self.sys.num_net_fail_to_report and spool.disk_max_priority >= self.sys.num_local_fail_to_report:
                self.loss_trigger_diskId = diskId
                self.sys_failed = True

        if event_type == Disk.EVENT_DETECT:
            # logging.info("detect disk priority: {}".format(disk.priority))
            if spool.state == Spool.STATE_FAILED:
                spool.disk_repair_max_priority = max(spool.disk_repair_max_priority, disk.priority)
                spool.disk_priority_queue[disk.priority][diskId] = 1
            else:
                if spool.disk_repair_max_priority > 0:
                    for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                        self.pause_disk_repair_time(dId, spool.disk_repair_max_priority)
                
                spool.disk_repair_max_priority = max(spool.disk_repair_max_priority, disk.priority)
                spool.disk_priority_queue[disk.priority][diskId] = 1

                disk.repair_start_time = self.curr_time
                disk.curr_prio_repair_started = False
                disk.failure_detection_time = 0
                
                if spool.disk_repair_max_priority > 0:
                    for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                        self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)


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
        if disk.repair_time[priority] == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = 0
        
        elif np.isnan(disk.repair_time[priority]):
            # print("disk {} priority {}  repairtime {}  remain data {}".format(
            #                 diskId, disk.priority, disk.repair_time[priority], disk.curr_repair_data_remaining))
            print('---')
        else:
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

    #----------------------------------------------
    # update diskgroup state
    #----------------------------------------------
    def update_diskgroup_state(self, event_type, diskId):
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            # if spool already fails, we don't need to fail it again.
            if spool.state == Spool.STATE_FAILED:
                disk.no_need_to_detect = True
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            if spool.disk_max_priority > self.sys.m:
                # print('spool failure!!!')
                # logging.error("Diskgroup %s failed due to the disk failure, it has failed disks %s", diskgroupId, self.get_failed_disks_per_diskgroup(diskgroupId))
                spool.state = Spool.STATE_FAILED
                mpool = self.mpools[spool.mpoolId]
                mpool.failed_spools[spool.spoolId] = 1
                mpool.failed_spools_undetected[spool.spoolId] = 1
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools[mpool.mpoolId] = 1
                self.affected_rackgroups[rackgroup.rackgroupId] = 1
                return spool.spoolId
        
        if event_type == Disk.EVENT_DETECT:
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            if spool.disk_repair_max_priority > self.sys.m:
                spool.is_in_repair = True
                mpool = self.mpools[spool.mpoolId]
                mpool.failed_spools_undetected.pop(spool.spoolId, None)
                mpool.failed_spools_in_repair[spool.spoolId] = 1
                self.rackgroups[mpool.rackgroupId].affected_mpools_in_repair[mpool.mpoolId] = 1
                return disk.spoolId


        if event_type == Spool.EVENT_REPAIR:
            spoolId = diskId
            spool = self.spools[spoolId]
            # logging.info("Diskgroup %s is repaired", diskId)
            new_failure_intervals = self.simulation.failure_generator.gen_new_failures(len(spool.failed_disks))
            for i, failedDiskId in enumerate(spool.failed_disks):
                self.disks[failedDiskId].state = Disk.STATE_NORMAL
                self.disks[failedDiskId].failure_detection_time = 0
                self.disks[failedDiskId].no_need_to_detect = False
                self.disks[failedDiskId].priority = 0
                self.disks[failedDiskId].repair_time.clear()
                self.disks[failedDiskId].priority_percents.clear()
                self.disks[failedDiskId].curr_prio_repair_started = False
                disk_fail_time = new_failure_intervals[i] + self.curr_time
                if disk_fail_time < self.simulation.mission_time:
                    heappush(self.simulation.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, failedDiskId))
            
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.disk_max_priority = 0
            spool.disk_repair_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()

            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            self.affected_spools.pop(spool.spoolId, None)
            mpool = self.mpools[spool.mpoolId]
            mpool.affected_spools.pop(spool.spoolId, None)
            mpool.failed_spools.pop(spool.spoolId, None)
            mpool.failed_spools_in_repair.pop(spool.spoolId, None)
            if len(mpool.failed_spools_in_repair) == 0:
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools_in_repair.pop(mpool.mpoolId, None)

            if len(mpool.failed_spools) == 0:
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools.pop(mpool.mpoolId, None)
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
            num_failed_spools_per_mpool = len(mpool.failed_spools)
            spool.failure_detection_time = self.curr_time + self.sys.detection_time
            if num_failed_spools_per_mpool >= self.sys.num_net_fail_to_report:
                if self.sys.num_local_fail_to_report == 0:
                    self.sys_failed = True
                    self.loss_trigger_diskId = diskId
                    return
                for affectedSpoolId in mpool.affected_spools:
                    if affectedSpoolId not in mpool.failed_spools:
                        affected_spool = self.spools[affectedSpoolId]
                        if affected_spool.disk_max_priority >= self.sys.num_local_fail_to_report:
                            self.sys_failed = True
                            self.loss_trigger_diskId = diskId
                

        if event_type == Disk.EVENT_DETECT:
            spool.repair_start_time = self.curr_time
            spool.failure_detection_time = 0
            rackgroup = self.rackgroups[mpool.rackgroupId]
            # logging.info("repairing spool...")


            if len(mpool.failed_spools_in_repair) > 1:
                # this mpool is already in repair. So no need to update other mpools' repair time
                self.update_spool_repair_time(spool.spoolId)
            else:
                # this mpool is now in repair, which is goind to steal network bandwidth from other mpools in the same rackgroup
                # therefore, we need to update network bandwidth for all mpools in repair in this rackgroup
                mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools_in_repair))
                for affected_mpoolId in rackgroup.affected_mpools:
                    affected_mpool = self.mpools[affected_mpoolId]
                    affected_mpool.repair_rate = mpool_repair_rate
                    for failedSpoolId in affected_mpool.failed_spools_in_repair:
                        self.update_spool_repair_time(failedSpoolId)
        


        if event_type == Spool.EVENT_REPAIR:
            num_repair_in_mpool = len(mpool.failed_spools_in_repair)
            rackgroup = self.rackgroups[mpool.rackgroupId]

            if num_repair_in_mpool > 0:
                return
            else:
                if len(rackgroup.affected_mpools_in_repair) > 0:
                    mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                            self.sys.interrack_speed / len(rackgroup.affected_mpools_in_repair))
                    for mpoolId in rackgroup.affected_mpools_in_repair:
                        affected_mpool = self.mpools[mpoolId]
                        affected_mpool.repair_rate = mpool_repair_rate
                        for failedSpoolId in affected_mpool.failed_spools_in_repair:
                            self.update_spool_repair_time(failedSpoolId)

    
    def update_spool_repair_time(self, spoolId):
        spool = self.spools[spoolId]
        mpool = self.mpools[spool.mpoolId]
        repaired_time = self.curr_time - spool.repair_start_time
        if repaired_time == 0:            
            repaired_percent = 0
            spool.curr_repair_data_remaining = self.sys.spool_size * self.sys.diskSize
            if self.sys.distribution == "catas_local_failure":
                self.sys.metrics.total_net_traffic += spool.curr_repair_data_remaining * (self.sys.top_k + 1)
        else:
            repaired_percent = repaired_time / spool.repair_time[0]
            spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(spool.curr_repair_data_remaining)/(mpool.repair_rate)
        if self.sys.distribution == "catas_local_failure":
            self.sys.metrics.total_net_repair_time += repair_time
            
        spool.repair_time[0] = repair_time / 3600 / 24
        spool.repair_start_time = self.curr_time
        spool.estimate_repair_time = self.curr_time + spool.repair_time[0]
        # logging.info("spoolId {} repair time: {} spool.estimate_repair_time: {}".format(spoolId, spool.repair_time[0], spool.estimate_repair_time))
        # print("spool.repair_time:{}".format(spool.repair_time[0]))


    def check_pdl(self):
        if self.sys_failed:
            self.generate_fail_report()
        return mlec_c_sodp_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        mlec_c_sodp_repair(self, repair_queue)
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            if not disk.no_need_to_detect:
                heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

    def generate_fail_report(self):
        if self.sys.collect_fail_reports:
            fail_report = {'curr_time': self.curr_time, 'trigger_disk': int(self.loss_trigger_diskId), 'spool_infos': [], 
                            'disk_infos': [], 'repair_queue': [], 'detect_queue': []}
            for affectedSpoolId in self.affected_spools:
                affectedSpool = self.spools[affectedSpoolId]

                if affectedSpool.is_in_repair:
                    fail_report['spool_infos'].append(
                        {
                        'curr_repair_data_remaining': affectedSpool.curr_repair_data_remaining,
                        'spoolId': int(affectedSpoolId),
                        'estimate_repair_time': affectedSpool.estimate_repair_time,
                        'repair_start_time': affectedSpool.repair_start_time,
                        'failure_detection_time': affectedSpool.failure_detection_time,
                        'is_in_repair': affectedSpool.is_in_repair,
                        'repair_time': json.dumps(affectedSpool.repair_time),
                        'failed_disks': json.dumps({int(k): v for k, v in affectedSpool.failed_disks.items()}),
                        'failed_disks_undetected': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_undetected.items()}),
                        'disk_priority_queue': json.dumps({int(k): {int(kk): vv for kk, vv in v.items()} for k, v in affectedSpool.disk_priority_queue.items()}),
                        'disk_max_priority': int(affectedSpool.disk_max_priority),
                        'disk_repair_max_priority': int(affectedSpool.disk_repair_max_priority)
                        })
                else:
                    spool_failed = False
                    if affectedSpool.state == Spool.STATE_FAILED:
                        spool_failed = True
                    fail_report['spool_infos'].append(
                        {
                        'spoolId': int(affectedSpoolId),
                        'spool_failed': spool_failed,
                        'is_in_repair': affectedSpool.is_in_repair,
                        'failed_disks': json.dumps({int(k): v for k, v in affectedSpool.failed_disks.items()}),
                        'failed_disks_undetected': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_undetected.items()}),
                        'disk_priority_queue': json.dumps({int(k): {int(kk): vv for kk, vv in v.items()} for k, v in affectedSpool.disk_priority_queue.items()}),
                        'disk_max_priority': int(affectedSpool.disk_max_priority),
                        'disk_repair_max_priority': int(affectedSpool.disk_repair_max_priority)
                        })
            
            for affectedSpoolId in self.affected_spools:
                affected_spool = self.spools[affectedSpoolId]
                for failedDiskId in affected_spool.failed_disks:
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
                            'no_need_to_detect': failedDisk.no_need_to_detect,
                            'priority_percents': json.dumps(failedDisk.priority_percents),
                            'curr_prio_repair_started': failedDisk.curr_prio_repair_started
                            })
            # print(self.simulation.repair_queue)
            for (e_time, e_type, e_diskId) in list(self.simulation.repair_queue):
                fail_report['repair_queue'].append(json.dumps((e_time, e_type, int(e_diskId))))
            for (e_time, e_type, e_diskId) in list(self.simulation.failure_queue):
                if e_type == Disk.EVENT_DETECT:
                    fail_report['detect_queue'].append(json.dumps((e_time, e_type, int(e_diskId))))
            # curr_disk = self.disks[diskId]
            # fail_report['detect_queue'].append(json.dumps((curr_disk.failure_detection_time, Disk.EVENT_DETECT, int(diskId))))
            self.sys.fail_reports.append(fail_report)
            # logging.info("generate fail report {}".format(pformat(fail_report)))
        return



    def clean_failures(self):
        affected_spools = {}
        
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            for diskId in spool.failed_disks:
                disk = self.disks[diskId]
                disk.state = Disk.STATE_NORMAL
                disk.repair_time.clear()
                disk.failure_detection_time = 0
                disk.no_need_to_detect = False
                disk.priority = 0
                disk.priority_percents.clear()
                disk.curr_prio_repair_started = False

            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            spool.failure_detection_time = 0
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.disk_repair_max_priority = 0
            spool.disk_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()
            spool.repair_time.clear()
            self.mpools[spool.mpoolId].affected_spools.clear()

        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                mpool.failed_spools.clear()
                mpool.failed_spools_in_repair.clear()
                mpool.failed_spools_undetected.clear()
            rackgroup.affected_mpools.clear()
            rackgroup.affected_mpools_in_repair.clear()

    def manual_inject_failures(self, fail_report, simulate):
        # logging.info("{}".format(pformat(fail_report)))
        for spool_info in fail_report['spool_infos']:
            spoolId = int(spool_info['spoolId'])
            spool = self.sys.spools[spoolId]

            spool.is_in_repair = spool_info['is_in_repair']

            if spool.is_in_repair:
                spool.state = Spool.STATE_FAILED
                spool.curr_repair_data_remaining = float(spool_info['curr_repair_data_remaining'])
                spool.estimate_repair_time = float(spool_info['estimate_repair_time'])
                spool.repair_start_time = float(spool_info['repair_start_time'])
                spool.failure_detection_time = float(spool_info['failure_detection_time'])
                repair_time = json.loads(spool_info['repair_time'])
                for key, value in repair_time.items():
                    spool.repair_time[int(key)] = float(value)
            else:
                spool_failed = spool_info['spool_failed']
                if spool_failed:
                    spool.state = Spool.STATE_FAILED
            
            failed_disks = json.loads(spool_info['failed_disks'])
            for key, value in failed_disks.items():
                spool.failed_disks[int(key)] = int(value)
            
            failed_disks_undetected = json.loads(spool_info['failed_disks_undetected'])
            for key, value in failed_disks_undetected.items():
                spool.failed_disks_undetected[int(key)] = int(value)
            
            disk_priority_queue = json.loads(spool_info['disk_priority_queue'])
            for prio, prio_disks in disk_priority_queue.items():
                spool.disk_priority_queue[int(prio)] = {int(k): 1 for k in prio_disks}
            
            spool.disk_max_priority = int(spool_info['disk_max_priority'])
            spool.disk_repair_max_priority = int(spool_info['disk_repair_max_priority'])
            
            self.affected_spools[spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[spoolId] = 1

            mpool = self.mpools[spool.mpoolId]
            
            rackgroup = self.rackgroups[mpool.rackgroupId]
            

            if spool.state == Spool.STATE_FAILED:
                mpool.failed_spools[spool.spoolId] = 1
                rackgroup.affected_mpools[mpool.mpoolId] = 1
                self.affected_rackgroups[rackgroup.rackgroupId] = 1
                if spool.is_in_repair:
                    mpool.failed_spools_in_repair[spool.spoolId] = 1
                    self.rackgroups[mpool.rackgroupId].affected_mpools_in_repair[mpool.mpoolId] = 1
                else:
                    mpool.failed_spools_undetected[spool.spoolId] = 1
        
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            disk.failure_detection_time = float(disk_info['failure_detection_time'])
            disk.no_need_to_detect = disk_info['no_need_to_detect']
            
            disk.curr_prio_repair_started = disk_info['curr_prio_repair_started']

            disk.priority = int(disk_info['priority'])

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
            
            priority_percents = json.loads(disk_info['priority_percents'])
            for key, value in priority_percents.items():
                disk.priority_percents[int(key)] = float(value)
        
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools_in_repair:
                mpool = self.mpools[mpoolId]
                mpool.repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools_in_repair))

        # if thhis fail report from prev stage already fails in current stage
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            mpool = self.mpools[spool.mpoolId]
            num_failed_spools_per_mpool = len(mpool.failed_spools)
            if num_failed_spools_per_mpool >= self.sys.num_net_fail_to_report:
                if self.sys.num_local_fail_to_report == 0:
                    self.sys_failed = True
                for affectedSpoolId in mpool.affected_spools:
                    if affectedSpoolId not in mpool.failed_spools:
                        affected_spool = self.spools[affectedSpoolId]
                        if affected_spool.disk_max_priority >= self.sys.num_local_fail_to_report:
                            self.sys_failed = True
        
        if self.sys_failed:
            self.sys.fail_reports.append(fail_report)
            return

        frozen_disks = []
        reserved_disks = {}

        if self.manual_spool_fail:
            diskId = fail_report['trigger_disk']
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            mpool = self.mpools[spool.mpoolId]
            manual_spoolId = random.sample(mpool.spoolIds, 1)[0]
            while manual_spoolId in mpool.failed_spools:
                manual_spoolId = random.sample(mpool.spoolIds, 1)[0]
            self.manual_spoolId = manual_spoolId
            manual_spool = self.spools[manual_spoolId]
            for failedDiskId in manual_spool.failed_disks:
                failedDisk = self.disks[failedDiskId]
                failedDisk.state = Disk.STATE_NORMAL
                failedDisk.no_need_to_detect = False
                failedDisk.failure_detection_time = 0
                failedDisk.repair_time.clear()
                failedDisk.priority_percents.clear()
                failedDisk.curr_prio_repair_started = False
                failedDisk.priority = 0
                reserved_disks[int(failedDiskId)] = 1
            manual_spool.failed_disks.clear()
            manual_spool.failed_disks_in_repair.clear()
            manual_spool.failed_disks_undetected.clear()
            manual_spool.disk_max_priority = 0
            manual_spool.disk_repair_max_priority = 0
            for i in range(self.sys.m + 1):
                manual_spool.disk_priority_queue[i + 1].clear()
            self.affected_spools.pop(manual_spool.spoolId, None)
            self.mpools[manual_spool.mpoolId].affected_spools.pop(manual_spool.spoolId, None)

            heappush(self.simulation.failure_queue, (float(self.manual_spool_fail_sample['curr_time']), Spool.EVENT_MANUAL_FAIL, manual_spoolId))
            for frozen_diskId in manual_spool.diskIds:
                frozen_disks.append(frozen_diskId)

        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            for frozen_diskId in spool.failed_disks:
                frozen_disks.append(frozen_diskId)

        for item in fail_report['detect_queue']:
            (e_time, e_type, e_diskId) = ast.literal_eval(item)
            if int(e_diskId) not in reserved_disks:
                heappush(self.simulation.failure_queue, (float(e_time), e_type, int(e_diskId)))

        
        mlec_c_sodp_repair(self, self.simulation.repair_queue)
        # the disk that triggered the system failure in prev stage
        diskId = int(fail_report['trigger_disk'])
        disk = self.disks[diskId]
        heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

        return frozen_disks
        
            # if e_type == Disk.EVENT_DETECT:
            #     print('yes!')
        
        # logging.info('detect queue: {}'.format(self.simulation.failure_queue))
        # logging.info('repair queue: {}'.format(self.simulation.repair_queue))
        # logging.info("affected pools: {}".format(self.affected_spools))
        # for spoolId in self.affected_spools:
        #     spool = self.spools[spoolId]
        #     logging.info("spool {} failed disks {}  failed_disks_in_repair {}".format(
        #                     spoolId, spool.failed_disks, spool.failed_disks_in_repair))
    def generate_manual_spool_fail_id(self):
        self.manual_spoolId = random.sample(self.spools.keys(), 1)[0]
        return self.spools[self.manual_spoolId].diskIds
    
    def manual_inject_spool_failure(self):
        spool = self.spools[self.manual_spoolId]
        # logging.info("spool state{} mpool affected spools{}".format(spool.state, self.mpools[spool.mpoolId].affected_spools))

        # print(self.manual_spool_fail_sample)
        for disk_info in self.manual_spool_fail_sample['disk_infos']:
            diskId = int(disk_info['diskId']) + spool.diskIds[0]
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            disk.failure_detection_time = float(disk_info['failure_detection_time'])
            disk.no_need_to_detect = False

            disk.curr_prio_repair_started = disk_info['curr_prio_repair_started']

            disk.priority = int(disk_info['priority'])

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
            
            priority_percents = json.loads(disk_info['priority_percents'])
            for key, value in priority_percents.items():
                disk.priority_percents[int(key)] = float(value)
            
            spool.disk_max_priority = max(disk.priority, spool.disk_max_priority)
            if disk.failure_detection_time >= self.curr_time:
                spool.failed_disks_undetected[diskId] = 1
                # logging.info("found undetected disk {} on spool {}".format(diskId, disk.spoolId))
            else:
                spool.disk_priority_queue[disk.priority][diskId] = 1
                spool.disk_repair_max_priority = max(disk.priority, spool.disk_repair_max_priority)


            spool.failed_disks[diskId] = 1
            self.affected_spools[disk.spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[disk.spoolId] = 1

        diskId = int(self.manual_spool_fail_sample['trigger_disk'])+ spool.diskIds[0]

        for undetectedDiskId in spool.failed_disks_undetected:
            if int(undetectedDiskId) != diskId:
                undetectedDisk = self.disks[undetectedDiskId]
                heappush(self.simulation.failure_queue, (undetectedDisk.failure_detection_time, Disk.EVENT_DETECT, undetectedDiskId))

        self.update_diskgroup_state(Disk.EVENT_FAIL, diskId)

        # logging.info("spoolId: {} spool state{} mpool affected spools{}".format(spool.spoolId, spool.state, self.mpools[spool.mpoolId].affected_spools))
        self.update_diskgroup_priority(Disk.EVENT_FAIL, spool.spoolId, diskId)

        # for spoolId in self.spools:
        #     logging.info("spoolid: {}  state: {}".format(spoolId, self.spools[spoolId].state))

        diskfailures = self.simulation.failure_generator.gen_new_failures(self.sys.spool_size)
        failure_idxs = np.where(diskfailures < self.simulation.mission_time - self.curr_time)[0]
        for idx in failure_idxs:
            failedDiskId = spool.diskIds[0] + idx
            if failedDiskId not in spool.failed_disks:
                disk_failure_time = diskfailures[idx] + self.curr_time
                heappush(self.simulation.failure_queue, (disk_failure_time, Disk.EVENT_FAIL, failedDiskId))

        return diskId
        
            
        

