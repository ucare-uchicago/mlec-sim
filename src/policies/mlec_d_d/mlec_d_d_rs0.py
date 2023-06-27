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

from helpers import mlec_d_c_prio
from components.disk import Disk
from components.spool import Spool
from constants.Components import Components
from policies.policy import Policy
from .pdl import mlec_d_d_pdl
from .repair import mlec_d_d_repair

class MLEC_D_D_RS0(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = state.sys.spools

        self.affected_racks = {}
        self.priority_queue = {}
        for i in range(self.sys.top_m + 1):
            self.priority_queue[i + 1] = {}
        
        self.max_priority = 0
        self.repair_max_priority = 0
        self.total_interrack_bandwidth = state.sys.interrack_speed * state.sys.num_racks
        self.failed_spools_undetected = []

        self.racks_in_repair = {}
        self.affected_spools = {}
        self.failed_spools = {}
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
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            spool.failed_disks_undetected[diskId] = 1
            self.affected_spools[disk.spoolId] = 1
        
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
            self.compute_disk_priority_percents(disk)

            if spool.disk_repair_max_priority > 0:
                for dId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
                    self.resume_disk_repair_time(dId, spool.disk_repair_max_priority, spool)

        if event_type == Disk.EVENT_DETECT:
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
                        self.resume_disk_repair_time(dId, spool.disk_repair_max_priority, spool)

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
                    self.resume_disk_repair_time(dId, spool.disk_repair_max_priority, spool)
    
    def compute_disk_priority_percents(self, disk):
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
    
    def resume_disk_repair_time(self, diskId, priority, spool):
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
            rackId = disk.rackId
            # if spool already fails, we don't need to fail it again.
            if spool.state == Spool.STATE_FAILED:
                disk.no_need_to_detect = True
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            # logging.info("disk id: {}  spoolid: {} spool failed disks: {}  sys.m: {}".format(
            #                 diskId, disk.spoolId, spool.failed_disks, self.sys.m))
            if spool.disk_max_priority > self.sys.m:
                spool.state = Spool.STATE_FAILED
                spool.total_network_repair_data = self.sys.diskSize * self.sys.spool_size
                self.racks[rackId].failed_spools[spool.spoolId] = 1
                self.affected_racks[rackId] = 1
                self.failed_spools[spool.spoolId] = 1
                self.failed_spools_undetected.append(spool.spoolId)
                self.racks[rackId].failed_spools_undetected.append(spool.spoolId)
                return spool.spoolId
        
        if event_type == Disk.EVENT_DETECT:
            disk = self.disks[diskId]
            spool = self.spools[disk.spoolId]
            if spool.disk_repair_max_priority > self.sys.m:
                spool.is_in_repair = True
                return disk.spoolId
        
        if event_type == Spool.EVENT_REPAIR:
            spoolId = diskId
            spool = self.spools[spoolId]
            rackId = spool.rackId
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
            
            spool.total_network_repair_data = 0
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.disk_max_priority = 0
            spool.disk_repair_max_priority = 0
            for i in range(self.sys.m + 1):
                spool.disk_priority_queue[i + 1].clear()

            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            self.affected_spools.pop(spool.spoolId, None)
            
            rack = self.racks[rackId]
            rack.failed_spools.pop(spoolId, None)
            if len(self.racks[rackId].failed_spools) == 0:
                self.affected_racks.pop(rackId, None)
            self.failed_spools.pop(spoolId, None)
            rack.num_spools_in_repair -= 1
            if rack.num_spools_in_repair == 0:
                self.racks_in_repair.pop(rackId, None)
            return spoolId
        
        if event_type == Spool.EVENT_FASTREBUILD:
            spoolId = diskId
            return spoolId
        return None

    def update_diskgroup_priority(self, event_type, spoolId, diskId):
        spool = self.spools[spoolId]
        rackId = spool.rackId
        if event_type == Disk.EVENT_FAIL:
            spool.failure_detection_time = self.curr_time + self.sys.detection_time
            rack = self.racks[rackId]
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.pause_spool_repair_time(spId, self.repair_max_priority)
            
            # Imagine there are 2 affected racks. And a new spool fails.
            # If the current failure happens in a brand new rack, we need to increment max_priority
            # If the current failure happens in an already affected rack, but the current max-priority is 1. 
            #     it means previous 2-chunk-failure have been repaired. But the new spool failure will lead to new 2-chunk-failure stripes.
            #     so we still need to increment max_priority
            # Otherwise, we don't increase max-priority.
            if self.repair_max_priority < len(self.racks_in_repair):
                if len(rack.failed_spools_undetected) == 1:
                    self.max_priority += 1
            else:
                if len(rack.failed_spools) == 1:
                    self.max_priority += 1
            
            spool.priority = self.max_priority

            self.compute_spool_priority_percents(spool, rackId)

            spool.failure_detection_time = self.curr_time + self.sys.detection_time

            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)

            if self.max_priority >= self.sys.num_net_fail_to_report:
                self.sys_failed = True
                self.loss_trigger_diskId = diskId
                return
        
        if event_type == Disk.EVENT_DETECT:
            rack = self.racks[rackId]

            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.pause_spool_repair_time(spId, self.repair_max_priority)
            
            self.repair_max_priority = max(self.repair_max_priority, spool.priority)
            self.priority_queue[spool.priority][spoolId] = 1
            assert self.failed_spools_undetected[0] == spoolId, "the detected spool should be the first element in the list!"
            self.failed_spools_undetected.pop(0)
            rack.failed_spools_undetected.pop(0)
            rack.num_spools_in_repair += 1
            if rack.num_spools_in_repair == 1:
                self.racks_in_repair[rackId] = 1

            spool.repair_start_time = self.curr_time
            spool.curr_prio_repair_started = False
            spool.failure_detection_time = 0
            
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)
        
        if event_type == Spool.EVENT_FASTREBUILD or event_type == Spool.EVENT_REPAIR:
            logging.info("event {}  spool {}  disk {}".format(event_type, spoolId, diskId))
            # ----------------
            # Remove the priority repair time and reduce priority because the spool is already repaired
            # Also remove it from the priority queue, and pause the repair of other spools in the queue.
            curr_priority = spool.priority
            assert curr_priority == self.repair_max_priority, "repair spool priority is not system spool repair max priority"
            del spool.repair_time[curr_priority]
            del spool.priority_percents[curr_priority]
            
            self.priority_queue[curr_priority].pop(spoolId, None)
            for spId in self.priority_queue[curr_priority]:
                self.pause_spool_repair_time(spId, curr_priority)
            
            # ----------------
            # Reduce priority because the spool has been repaired
            spool.priority -= 1
            if spool.priority > 0:
                self.priority_queue[spool.priority][spoolId] = 1

            spool.repair_start_time = self.state.curr_time
            spool.curr_prio_repair_started = False
            
            rack = self.racks[rackId]
            # update max priority when its queue is empty
            if len(self.priority_queue[self.repair_max_priority]) == 0:
                self.repair_max_priority -= 1
                self.max_priority = self.repair_max_priority

                num_racks_in_repair = len(self.racks_in_repair)
                num_failed_spools_per_rack = [0] * self.sys.num_racks
                for inrepairRackId in self.racks_in_repair:
                    num_failed_spools_per_rack[inrepairRackId] = self.racks[inrepairRackId].num_spools_in_repair

                num_undetected_spools_per_rack = [0] * self.sys.num_racks

                for undetectedDiskId in self.failed_spools_undetected:
                    undetectedDisk = self.spools[undetectedDiskId]
                    num_undetected_spools_per_rack[undetectedDisk.rackId] += 1
                    num_failed_spools_per_rack[undetectedDisk.rackId] += 1
                    if self.repair_max_priority < num_racks_in_repair:
                        if num_undetected_spools_per_rack[undetectedDisk.rackId] == 1:
                            self.max_priority += 1
                    else:
                        if num_failed_spools_per_rack[undetectedDisk.rackId] == 1:
                            self.max_priority += 1
                    undetectedDisk.priority = self.max_priority
            
            if self.repair_max_priority > 0:
                for spId in self.priority_queue[self.repair_max_priority]:
                    self.resume_spool_repair_time(spId, self.spools[spId].priority, rackId)


    
    def pause_spool_repair_time(self, spoolId, priority):
        spool = self.spools[spoolId]
        repaired_time = self.curr_time - spool.repair_start_time
        repaired_percent = repaired_time / spool.repair_time[priority]
        spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
    def compute_spool_priority_percents(self, spool, rackId):
        for i in range(spool.priority):
            priority = i+1
            spool.priority_percents[priority] = mlec_d_c_prio.compute_spool_priority_percent(self.state, self.affected_racks, rackId, priority)
        # logging.info("priority_percents: {}".format(disk.priority_percents))
    
    def resume_spool_repair_time(self, spoolId, priority, rackId):
        spool = self.spools[spoolId]
        if not spool.curr_prio_repair_started:
            priority_percent = spool.priority_percents[priority]
            spool.curr_repair_data_remaining = spool.total_network_repair_data * priority_percent
            spool.curr_prio_repair_started = True
            if self.sys.distribution == "catas_local_failure":
                self.sys.metrics.total_net_traffic += spool.curr_repair_data_remaining * (self.sys.top_k + 1)
        repair_time = self.calc_spool_repair_time(spool, priority)
        if self.sys.distribution == "catas_local_failure":
            self.sys.metrics.total_net_repair_time += repair_time
            
        spool.repair_time[priority] = repair_time / 3600 / 24
        spool.repair_start_time = self.state.curr_time
        spool.estimate_repair_time = self.state.curr_time + spool.repair_time[priority]
    
    def calc_spool_repair_time(self, spool, priority):
        total_spool_IO = (self.sys.num_spools - len(self.failed_spools)) * self.sys.diskIO * self.sys.spool_size
        total_repair_bandwidth = min(total_spool_IO, self.total_interrack_bandwidth)

        total_repair_data_readwrite = float(spool.curr_repair_data_remaining) * (self.sys.top_k + 1) 
        # we repair multiple spools concurrently. So rebuild bandwidth is shared
        per_spool_total_repair_bandwidth = total_repair_bandwidth / len(self.priority_queue[priority])
        repair_time = total_repair_data_readwrite / per_spool_total_repair_bandwidth

        return repair_time

    def check_pdl(self):
        if self.sys_failed:
            self.generate_fail_report()
        return mlec_d_d_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        mlec_d_d_repair(self, repair_queue)
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            if not disk.no_need_to_detect:
                heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))
    
    def generate_fail_report(self):
        return
    
    def clean_failures(self):        
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
            spool.total_network_repair_data = 0
            spool.priority_percents.clear()
            spool.priority = 0
            spool.curr_prio_repair_started = False

        for rackId in self.affected_racks:
            rack = self.racks[rackId]
            rack.failed_spools.clear()
            rack.failed_spools_undetected.clear()
            rack.num_spools_in_repair = 0
    
    def generate_fail_report(self):
        if self.sys.collect_fail_reports:
            fail_report = {'curr_time': self.curr_time, 'trigger_disk': int(self.loss_trigger_diskId), 'spool_infos': [], 
                            'disk_infos': [], 'rack_infos': [], 'detect_queue': []}
            for affectedSpoolId in self.affected_spools:
                affectedSpool = self.spools[affectedSpoolId]

                fail_report['spool_infos'].append(
                    {
                    'curr_repair_data_remaining': affectedSpool.curr_repair_data_remaining,
                    'spoolId': int(affectedSpoolId),
                    'state': affectedSpool.state,
                    'priority': affectedSpool.priority,
                    'estimate_repair_time': affectedSpool.estimate_repair_time,
                    'repair_start_time': affectedSpool.repair_start_time,
                    'failure_detection_time': affectedSpool.failure_detection_time,
                    'is_in_repair': affectedSpool.is_in_repair,
                    'repair_time': json.dumps(affectedSpool.repair_time),
                    'failed_disks': json.dumps({int(k): v for k, v in affectedSpool.failed_disks.items()}),
                    'failed_disks_undetected': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_undetected.items()}),
                    'disk_priority_queue': json.dumps({int(k): {int(kk): vv for kk, vv in v.items()} for k, v in affectedSpool.disk_priority_queue.items()}),
                    'disk_max_priority': int(affectedSpool.disk_max_priority),
                    'disk_repair_max_priority': int(affectedSpool.disk_repair_max_priority),
                    'total_network_repair_data': affectedSpool.total_network_repair_data,
                    'curr_prio_repair_started': affectedSpool.curr_prio_repair_started,
                    'priority_percents': json.dumps({int(k): v for k, v in affectedSpool.priority_percents.items()}),
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
            
            for affectedRackId in self.affected_racks:
                affectedRack = self.racks[affectedRackId]
                fail_report['rack_infos'].append({
                    'rackId': int(affectedRack.rackId),
                    'failed_spools': json.dumps({int(k): v for k, v in affectedRack.failed_spools.items()}),
                    'failed_spools_undetected': json.dumps([int(k) for k in affectedRack.failed_spools_undetected]),
                    'num_spools_in_repair': int(affectedRack.num_spools_in_repair)
                })

            fail_report['priority_queue'] = json.dumps({int(k): v for k, v in self.priority_queue.items()})
            fail_report['max_priority'] = self.max_priority
            fail_report['repair_max_priority'] = self.repair_max_priority
            fail_report['failed_spools_undetected'] = json.dumps([int(k) for k in self.failed_spools_undetected])
            fail_report['racks_in_repair'] = json.dumps({int(k): v for k, v in self.racks_in_repair.items()})
            fail_report['affected_racks'] = json.dumps({int(k): v for k, v in self.affected_racks.items()})
            fail_report['failed_spools'] = json.dumps({int(k): v for k, v in self.failed_spools.items()})


            for (e_time, e_type, e_diskId) in list(self.simulation.failure_queue):
                if e_type == Disk.EVENT_DETECT:
                    fail_report['detect_queue'].append(json.dumps((e_time, e_type, int(e_diskId))))
            # curr_disk = self.disks[diskId]
            # fail_report['detect_queue'].append(json.dumps((curr_disk.failure_detection_time, Disk.EVENT_DETECT, int(diskId))))
            self.sys.fail_reports.append(fail_report)
            # logging.info("generate fail report {}".format(pformat(fail_report)))
        return

    def manual_inject_failures(self, fail_report, simulate):
        logging.info("{}".format(pformat(fail_report)))
        for spool_info in fail_report['spool_infos']:
            spoolId = int(spool_info['spoolId'])
            spool = self.sys.spools[spoolId]

            spool.total_network_repair_data = float(spool_info['total_network_repair_data'])
            spool.curr_prio_repair_started = spool_info['curr_prio_repair_started']
            spool.priority = int(spool_info['priority'])
            spool.is_in_repair = spool_info['is_in_repair']

            spool.state = spool_info['state']
            spool.curr_repair_data_remaining = float(spool_info['curr_repair_data_remaining'])
            spool.estimate_repair_time = float(spool_info['estimate_repair_time'])
            spool.repair_start_time = float(spool_info['repair_start_time'])
            spool.failure_detection_time = float(spool_info['failure_detection_time'])
            repair_time = json.loads(spool_info['repair_time'])
            for key, value in repair_time.items():
                spool.repair_time[int(key)] = float(value)
            priority_percents = json.loads(spool_info['priority_percents'])
            for key, value in priority_percents.items():
                spool.priority_percents[int(key)] = float(value)
                
            
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
        
        for rack_info in fail_report['rack_infos']:
            rackId = int(rack_info['rackId'])
            rack = self.racks[rackId]
            for key, value in json.loads(rack_info['failed_spools']).items():
                rack.failed_spools[int(key)] = float(value)
            for key in json.loads(rack_info['failed_spools_undetected']):
                rack.failed_spools_undetected.append(int(key))
            rack.num_spools_in_repair = int(rack_info['num_spools_in_repair'])
            
        
        priority_queue = json.loads(fail_report['priority_queue'])
        for pri, q in priority_queue.items():
            que = {}
            for k, v in q.items():
                que[int(k)] = float(v)
            self.priority_queue[int(pri)] = que
            
        
        self.max_priority = int(fail_report['max_priority'])
        self.repair_max_priority = int(fail_report['repair_max_priority'])
        failed_spools_undetected = json.loads(fail_report['failed_spools_undetected'])
        for k in failed_spools_undetected:
            self.failed_spools_undetected.append(int(k))
        
        for key, value in json.loads(fail_report['racks_in_repair']).items():
            self.racks_in_repair[int(key)] = float(value)
        for key, value in json.loads(fail_report['affected_racks']).items():
            self.affected_racks[int(key)] = float(value)
        for key, value in json.loads(fail_report['failed_spools']).items():
            self.failed_spools[int(key)] = float(value)


        # if thhis fail report from prev stage already fails in current stage
        if self.max_priority >= self.sys.num_net_fail_to_report:
            self.sys_failed = True
        
        if self.sys_failed:
            self.sys.fail_reports.append(fail_report)
            return
        
        frozen_disks = []
        reserved_disks = {}
        
        if self.manual_spool_fail:
            manual_spoolId = random.sample(self.spools.keys(), 1)[0]
            while manual_spoolId in self.failed_spools:
                manual_spoolId = random.sample(self.spools.keys(), 1)[0]
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
            manual_spool.curr_prio_repair_started = False
            manual_spool.total_network_repair_data = 0
            assert manual_spool.priority == 0, "this manual spool should have priority 0"
            self.affected_spools.pop(manual_spool.spoolId, None)

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
            # if e_type == Disk.EVENT_DETECT:
            #     print('yes!')
        mlec_d_d_repair(self, self.simulation.repair_queue)

        diskId = int(fail_report['trigger_disk'])
        disk = self.disks[diskId]
        heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

        return frozen_disks

    def generate_manual_spool_fail_id(self):
        self.manual_spoolId = random.sample(self.spools.keys(), 1)[0]
        return self.spools[self.manual_spoolId].diskIds

    def manual_inject_spool_failure(self):
        spool = self.spools[self.manual_spoolId]

        logging.info("{}".format(pformat(self.manual_spool_fail_sample)))
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
