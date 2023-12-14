import logging
import math
import json
from heapq import heappush

from components.disk import Disk
from components.rack import Rack
from policies.policy import Policy
from helpers.common_math import ncr
from .pdl import slec_local_sodp_pdl
from .repair import slec_local_sodp_repair


class SLEC_LOCAL_SODP(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        # key is stripesetId, value is priority
        self.affected_stripesets = {}
        
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
                spool.next_repair_disk = -1

    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_DETECT:
            #-----------------------------------------------------
            # calculate repairTime and update priority for decluster
            #-----------------------------------------------------
            if spool.disk_repair_max_priority > 0:
                dId = spool.next_repair_disk
                self.pause_disk_repair_time(dId, spool.disk_repair_max_priority)
            

            spool.disk_repair_max_priority = max(spool.disk_repair_max_priority, disk.priority)
            if disk.priority not in spool.disk_priority_queue:
                self.simulation.log.append('disk.priority {} not in spool.disk_priority_queue: {}'.format(
                    disk.priority, spool.disk_priority_queue
                ))
                self.simulation.print_log()
            spool.disk_priority_queue[disk.priority][diskId] = 1
            spool.failed_disks_undetected.pop(diskId, None)

            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False
            disk.failure_detection_time = 0
            
            if spool.disk_repair_max_priority > 0:
                dId = next(iter(spool.disk_priority_queue[spool.disk_repair_max_priority]))
                spool.next_repair_disk = dId
                self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)

        if event_type == Disk.EVENT_FAIL:
            if spool.disk_repair_max_priority > 0:
                dId = spool.next_repair_disk
                self.pause_disk_repair_time(dId, spool.disk_repair_max_priority)

            disk_priority = 0
            # print('disk.stripesets: {}'.format(disk.stripesets))
            for stripesetId in disk.stripesets:
                stripeset_prio = self.affected_stripesets.get(stripesetId, 0) + 1                    
                self.affected_stripesets[stripesetId] = stripeset_prio
                
                disk_priority = max(disk_priority, stripeset_prio)
                # print('stripeset_prio: {} disk_priority: {}'.format(stripeset_prio, disk_priority))
            disk.priority = disk_priority
            # print('disk.priority: {} disk_priority: {}'.format(disk.priority, disk_priority))

            spool.disk_max_priority = max(spool.disk_max_priority, disk.priority)

            fail_num = len(spool.failed_disks)
            good_num = self.sys.spool_size - fail_num
            disk.good_num = good_num
            disk.fail_num = fail_num
            # self.compute_priority_percents(disk)

            disk.curr_prio_repair_started = False
            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            # logging.info("disk.failure_detection_time: {}".format(disk.failure_detection_time))

            if spool.disk_repair_max_priority > 0:
                dId = next(iter(spool.disk_priority_queue[spool.disk_repair_max_priority]))
                spool.next_repair_disk = dId
                self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)
            
            #----------------------------------------------
            if spool.disk_max_priority >= self.sys.num_local_fail_to_report:
                self.sys_failed = True
                sys_survive_prob = 0
                if self.sys.collect_fail_reports:
                    fail_report = {'curr_time': self.curr_time, 'disk_infos': [], 'trigger_disk': int(diskId), 
                                    'affected_stripesets': json.dumps({int(k): v for k, v in self.affected_stripesets.items()}),
                                    'spool_infos': []}
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
                            'priority_percents': json.dumps(failedDisk.priority_percents),
                            'curr_prio_repair_started': failedDisk.curr_prio_repair_started
                            })
                    for affectedSpoolId in self.affected_spools:
                        affectedSpool = self.spools[affectedSpoolId]
                        fail_report['spool_infos'].append(
                            {
                            'spoolId': int(affectedSpoolId),
                            'next_repair_disk': int(affectedSpool.next_repair_disk),
                            'disk_priority_queue': json.dumps({int(k): {int(kk): vv for kk, vv in v.items()} for k, v in affectedSpool.disk_priority_queue.items()}),
                            })
                    # logging.info('new fail report: {}'.format(fail_report))
                    self.sys.fail_reports.append(fail_report)
                return
            
                        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            curr_priority = disk.priority
            assert curr_priority == spool.disk_repair_max_priority, "repair disk priority is not spool disk repair max priority"
            if curr_priority not in disk.repair_time:
                self.simulation.log.append('curr prioirity {} not in disk.repair_time'.format(curr_priority))
                self.simulation.print_log()
            del disk.repair_time[curr_priority]
            if curr_priority not in disk.priority_percents:
                self.simulation.log.append('affected_stripesets: {}'.format(self.affected_stripesets))
                self.simulation.log.append('curr prioirity {} not in disk {} priority_percents: {} stripesets: {}'.format(
                            curr_priority, diskId, disk.priority_percents, disk.stripesets))
                self.simulation.print_log()
            del disk.priority_percents[curr_priority]

            spool.disk_priority_queue[curr_priority].pop(diskId, None)
            # self.pause_disk_repair_time(diskId, curr_priority)
            

            updated_stripesets = set()
            for stripesetId in disk.stripesets:
                if stripesetId in self.affected_stripesets:
                    if self.affected_stripesets[stripesetId] == curr_priority:
                        self.affected_stripesets[stripesetId] -= 1
                    if self.affected_stripesets[stripesetId] == 0:
                        del self.affected_stripesets[stripesetId]
                    updated_stripesets.add(stripesetId)
            for dId in spool.failed_disks_undetected:
                failedDisk = self.disks[dId]
                if updated_stripesets & failedDisk.stripesets:
                    failedDisk.priority = 0
                    for stripesetId in failedDisk.stripesets:
                        stripeset_prio = self.affected_stripesets.get(stripesetId, 0) + 1                    
                        failedDisk.priority = max(failedDisk.priority, stripeset_prio)

            disk.priority -= 1
            if disk.priority > 0:
                spool.disk_priority_queue[disk.priority][diskId] = 1

            spool.disk_max_priority = 0
            for dId in spool.failed_disks:
                failedDisk = self.disks[dId]
                spool.disk_max_priority = max(spool.disk_max_priority, failedDisk.priority)


            disk.repair_start_time = self.curr_time
            disk.curr_prio_repair_started = False

            if len(spool.disk_priority_queue[spool.disk_repair_max_priority]) == 0:
                spool.disk_repair_max_priority -= 1

            if spool.disk_repair_max_priority > 0:
                dId = next(iter(spool.disk_priority_queue[spool.disk_repair_max_priority]))
                self.simulation.log.append('next repair disk: {} priority: {}'.format(dId, spool.disk_repair_max_priority))
                spool.next_repair_disk = dId
                self.resume_repair_time(dId, spool.disk_repair_max_priority, spool)


    def compute_priority_percents(self, failed_disk):
        max_priority = failed_disk.priority
        
        priority_num = {}

        for priority in range(1, max_priority+1):
            priority_num[priority] = 0

        for stripesetId in failed_disk.stripesets:
            if stripesetId in self.affected_stripesets:
                priority = self.affected_stripesets[stripesetId]
                # if priority not in priority_num:
                #     self.simulation.log.append(self.affected_stripesets)
                #     spool = self.sys.spools[failed_disk.spoolId]
                #     for diskId in spool.failed_disks:
                #         self.simulation.log.append('disk {} stripesets: {}'.format(diskId, failed_disk.stripesets))
                #     self.simulation.log.append('priority {} of stripeset{} not in disk priority_num {}. Disk priority: {}'.format(
                #         priority, stripesetId, failed_disk.diskId, failed_disk.priority
                #     ))
                #     self.simulation.print_log()
                # TODO: optimize this. Use repair priority and priority for stripeset to deal with detection?
                if priority in priority_num:
                    priority_num[priority] += 1
        
        for priority in range(1, max_priority+1):
            failed_disk.priority_percents[priority] = priority_num[priority] / len(failed_disk.stripesets)
        
        # logging.info("    Priority percent is {}".format(failed_disk.priority_percents))
        return failed_disk.priority_percents[max_priority]

    def pause_disk_repair_time(self, diskId, priority):
        disk = self.state.disks[diskId]
        repaired_time = self.state.curr_time - disk.repair_start_time
        # if priority not in disk.repair_time:
        #     self.simulation.log.append('disk {}. repair time: {}  priority {} not in'.format(
        #         diskId, disk.repair_time, priority
        #     ))
        #     self.simulation.print_log()
        repaired_percent = repaired_time / disk.repair_time[priority]
        disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
    

    # def update_rebuild_width(self, spool):
    #     rebuild_width = 0
    #     read_disks = set()
    #     for repairDiskId in spool.disk_priority_queue[spool.disk_repair_max_priority]:
    #         repairDisk = self.disks[repairDiskId]
    #         for stripesetId in repairDisk.stripesets:
    #             if self.affected_stripesets.get(stripesetId, 0) == spool.disk_repair_max_priority:
    #                 for availDiskId in self.sys.stripesets[stripesetId]:
    #                     availDisk = self.disks[availDiskId]
    #                     if availDisk.state == Disk.STATE_NORMAL:
    #                         read_disks.add(availDiskId)
    #     rebuild_width = len(read_disks) / self.sys.k * (self.sys.k + 1)
    #     rebuild_width = min(rebuild_width, self.sys.spool_size - len(spool.failed_disks))
    #     spool.rebuild_width = rebuild_width
    #     if spool.rebuild_width == 0:
    #         for dId in spool.failed_disks:
    #             disk = self.disks[dId]
    #             self.simulation.log.append('did: {} prioirty: {} stripesets: {}'.format(
    #                     dId, disk.priority, disk.stripesets))
    #         self.simulation.log.append('self.affected_stripesets: {}'.format(self.affected_stripesets))
    #         self.simulation.log.append('spool.disk_repair_max_priority: {}  rebuild_width: {}'.format(
    #                 spool.disk_repair_max_priority, rebuild_width))
    #         self.simulation.print_log()
    #     assert spool.rebuild_width > 0
    #     logging.info('rebuild_width: {}'.format(rebuild_width))

    def resume_repair_time(self, diskId, priority, spool):
        disk = self.disks[diskId]
        self.simulation.log.append('resume repair disk {} curr_prio_repair_started: {}'.format(
            diskId, disk.curr_prio_repair_started
        ))
        if not disk.curr_prio_repair_started:
            self.compute_priority_percents(disk)
            priority_percent = disk.priority_percents[priority]
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            disk.curr_prio_repair_started = True
        
        avail_disks = set()
        for stripesetId in disk.stripesets:
            if stripesetId in self.affected_stripesets:
                if self.affected_stripesets[stripesetId] == priority:
                    for availDiskId in self.sys.stripesets[stripesetId]:
                        if self.disks[availDiskId].state == Disk.STATE_NORMAL:
                            avail_disks.add(availDiskId)
        rebuild_width = len(avail_disks) / self.sys.k * (self.sys.k + 1)
        rebuild_width = min(rebuild_width, self.sys.spool_size - len(spool.failed_disks))

        if rebuild_width == 0 or disk.curr_repair_data_remaining == 0:
            self.simulation.log.append('  disk {} rebuild width: {}  disk.curr_repair_data_remaining {}'.format(
                diskId, rebuild_width, disk.curr_repair_data_remaining
            ))
            repair_time = 0
        else:
            repair_time = disk.curr_repair_data_remaining * (self.sys.k+1) / (self.sys.diskIO * rebuild_width / len(spool.disk_priority_queue[priority]))
 
        
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.state.curr_time
        disk.estimate_repair_time = self.state.curr_time + disk.repair_time[priority]

        # self.simulation.log.append(' resume_repair_time. disk {} repair time: {}'.format(
        #     diskId, disk.repair_time
        # ))

        # logging.info("curr_repair_data_remaining: {} repair time: {}".format(
        #     disk.curr_repair_data_remaining, disk.repair_time[priority]))




    def check_pdl(self):
        return slec_local_sodp_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        slec_local_sodp_repair(self, repair_queue)
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
            spool.next_repair_disk = -1
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
            
            disk.curr_prio_repair_started = disk_info['curr_prio_repair_started']
            
            if disk.failure_detection_time >= simulate.curr_time:
                spool.failed_disks_undetected[diskId] = 1
                # logging.info("found undetected disk {} on spool {}".format(diskId, disk.spoolId))
            else:
                spool.disk_repair_max_priority = max(disk.priority, spool.disk_repair_max_priority)
        
        for spool_info in fail_report['spool_infos']:
            spoolId = int(spool_info['spoolId'])
            assert self.affected_spools[spoolId] == 1
            spool = self.sys.spools[spoolId]
            spool.next_repair_disk = int(spool_info['next_repair_disk'])
            disk_priority_queue = json.loads(spool_info['disk_priority_queue'])
            for prio, prio_disks in disk_priority_queue.items():
                spool.disk_priority_queue[int(prio)] = {int(k): 1 for k in prio_disks}
        
        affected_stripesets = json.loads(fail_report['affected_stripesets'])
        for key, value in affected_stripesets.items():
            self.affected_stripesets[int(key)] = int(value)

        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            if spool.disk_repair_max_priority > 0:
                diskId = spool.next_repair_disk
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