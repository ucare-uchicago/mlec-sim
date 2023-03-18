import logging
import time
from typing import List, Dict, Optional, Tuple
from heapq import heappush
import json
import ast
from pprint import pformat

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
        self.loss_trigger_diskId = -1
        

    def update_disk_state(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        
        if event_type == Disk.EVENT_REPAIR:
            self.disks[diskId].state = Disk.STATE_NORMAL
            self.disks[diskId].no_need_to_detect = False
            spool.failed_disks.pop(diskId, None)
            spool.failed_disks_in_repair.pop(diskId, None)
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
            spool.failed_disks_in_repair[diskId] = 1


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_FAIL:
            # If the spool is already failing, we do nothing because it's in reconstruction anyway
            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            if spool.state == Spool.STATE_FAILED:
                # logging.info("Diskgroup already in failed state, ignoring")
                return
            
            mpool = self.mpools[spool.mpoolId]
            if len(mpool.failed_spools) >= self.sys.num_net_fail_to_report and len(spool.failed_disks) >= self.sys.num_local_fail_to_report:
                self.loss_trigger_diskId = diskId
                self.sys_failed = True

        if event_type == Disk.EVENT_DETECT:
            disk.repair_start_time = self.curr_time
            disk.failure_detection_time = 0
            self.update_disk_repair_time(diskId)

        if event_type == Disk.EVENT_REPAIR:
            return


    def update_disk_repair_time(self, diskId):
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
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO)

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
                disk.no_need_to_detect = True
                return None
            
            # otherwise, we need to check if a new diskgroup fails
            if len(spool.failed_disks) > self.sys.m:
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
            if spool.state == Spool.STATE_FAILED:
                spool.is_in_repair = True
                for failedDiskId in spool.failed_disks_in_repair:
                    spool.failed_disks_network_repair[failedDiskId] = 1
                mpool = self.mpools[spool.mpoolId]
                mpool.failed_spools_undetected.pop(spool.spoolId, None)
                mpool.failed_spools_in_repair[spool.spoolId] = 1
                self.rackgroups[mpool.rackgroupId].affected_mpools_in_repair[mpool.mpoolId] = 1
                return disk.spoolId


        if event_type == Spool.EVENT_REPAIR:
            spoolId = diskId
            spool = self.spools[spoolId]
            # logging.info("Diskgroup %s is repaired", diskId)
            new_failure_intervals = self.simulation.failure_generator.gen_new_failures(len(spool.failed_disks_network_repair))
            for i, failedDiskId in enumerate(spool.failed_disks_network_repair):
                self.disks[failedDiskId].state = Disk.STATE_NORMAL
                self.disks[failedDiskId].failure_detection_time = 0
                self.disks[failedDiskId].no_need_to_detect = False
                spool.failed_disks.pop(failedDiskId, None)
                spool.failed_disks_in_repair.pop(failedDiskId, None)
                disk_fail_time = new_failure_intervals[i] + self.curr_time
                if disk_fail_time < self.simulation.mission_time:
                    heappush(self.simulation.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, failedDiskId))
            spool.failed_disks_network_repair.clear()
            
            # we only repair p_l+1 disks via network parity.
            # if during the repair, some other disks in the spool fail, we repair them locally after the network repair finishes.
            assert len(spool.failed_disks_in_repair) == 0, "spool.failed_disks_in_repair should be empty!"
            detect_count = 0
            for failedDiskId in spool.failed_disks_undetected:
                failed_disk = self.disks[failedDiskId]
                failed_disk.no_need_to_detect = False
                if failed_disk.failure_detection_time <= self.curr_time:
                    heappush(self.simulation.failure_queue, (self.curr_time, Disk.EVENT_DETECT, failedDiskId))
                    detect_count += 1
                    # print(detect_count)
                    if detect_count > self.sys.m:
                        break

            spool.is_in_repair = False
            mpool = self.mpools[spool.mpoolId]
            mpool.failed_spools_in_repair.pop(spool.spoolId, None)
            if len(mpool.failed_spools_in_repair) == 0:
                rackgroup = self.rackgroups[mpool.rackgroupId]
                rackgroup.affected_mpools_in_repair.pop(mpool.mpoolId, None)

            if len(spool.failed_disks) <= self.sys.m:
                spool.state = Spool.STATE_NORMAL
                if len(spool.failed_disks) == 0:
                    self.affected_spools.pop(spool.spoolId, None)
                    mpool.affected_spools.pop(spool.spoolId, None)

                mpool.failed_spools.pop(spool.spoolId, None)

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
                        if len(affected_spool.failed_disks) >= self.sys.num_local_fail_to_report:
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
            spool.curr_repair_data_remaining = len(spool.failed_disks_network_repair) * self.sys.diskSize
        else:
            repaired_percent = repaired_time / spool.repair_time[0]
            spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(spool.curr_repair_data_remaining)/(mpool.repair_rate)
            
        spool.repair_time[0] = repair_time / 3600 / 24
        spool.repair_start_time = self.curr_time
        spool.estimate_repair_time = self.curr_time + spool.repair_time[0]
        # logging.info("spoolId {} spool.failed_disks_network_repair{} curr_repair_data_remaining: {} mpool.repair_rate: {}".format(
        #                 spoolId, spool.failed_disks_network_repair, spool.curr_repair_data_remaining, mpool.repair_rate))
        # logging.info("spoolId {} repair time: {} spool.estimate_repair_time: {}".format(spoolId, spool.repair_time[0], spool.estimate_repair_time))
        # print("spool.repair_time:{}".format(spool.repair_time[0]))


    def check_pdl(self):
        if self.sys_failed:
            self.generate_fail_report()
        return mlec_c_c_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        mlec_c_c_repair(self, repair_queue)
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
                        'failed_disks_in_repair': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_in_repair.items()}),
                        'failed_disks_network_repair': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_network_repair.items()})
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
                        'failed_disks_in_repair': json.dumps({int(k): v for k, v in affectedSpool.failed_disks_in_repair.items()})
                        })
            
            for affectedSpoolId in self.affected_spools:
                affected_spool = self.spools[affectedSpoolId]
                for failedDiskId in affected_spool.failed_disks:
                    failedDisk = self.disks[failedDiskId]
                    fail_report['disk_infos'].append(
                            {
                            'curr_repair_data_remaining': failedDisk.curr_repair_data_remaining,
                            'diskId': int(failedDiskId),
                            'estimate_repair_time': failedDisk.estimate_repair_time,
                            'repair_start_time': failedDisk.repair_start_time,
                            'failure_detection_time': failedDisk.failure_detection_time,
                            'repair_time': json.dumps(failedDisk.repair_time),
                            'no_need_to_detect': failedDisk.no_need_to_detect
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

            spool.state = Spool.STATE_NORMAL
            spool.is_in_repair = False
            spool.failure_detection_time = 0
            spool.failed_disks.clear()
            spool.failed_disks_undetected.clear()
            spool.failed_disks_in_repair.clear()
            spool.failed_disks_network_repair.clear()
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
                failed_disks_network_repair = json.loads(spool_info['failed_disks_network_repair'])
                for key, value in failed_disks_network_repair.items():
                    spool.failed_disks_network_repair[int(key)] = int(value)
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
            
            failed_disks_in_repair = json.loads(spool_info['failed_disks_in_repair'])
            for key, value in failed_disks_in_repair.items():
                spool.failed_disks_in_repair[int(key)] = int(value)
            
            self.affected_spools[spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[spoolId] = 1

            mpool = self.mpools[spool.mpoolId]
            
            rackgroup = self.rackgroups[mpool.rackgroupId]
            rackgroup.affected_mpools[mpool.mpoolId] = 1
            self.affected_rackgroups[rackgroup.rackgroupId] = 1

            if spool.state == Spool.STATE_FAILED:
                mpool.failed_spools[spool.spoolId] = 1
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

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
        
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
                        if len(affected_spool.failed_disks) >= self.sys.num_local_fail_to_report:
                            self.sys_failed = True
        
        if self.sys_failed:
            self.sys.fail_reports.append(fail_report)
            return

        # the disk that triggered the system failure in prev stage
        diskId = int(fail_report['trigger_disk'])
        disk = self.disks[diskId]
        # disk.state = Disk.STATE_FAILED
        # spool = self.spools[disk.spoolId]
            
        # # we need to check if this spool fails
        # if len(spool.failed_disks) > self.sys.m:
        #     spool.state = Spool.STATE_FAILED
        #     mpool = self.mpools[spool.mpoolId]
        #     mpool.failed_spools[spool.spoolId] = 1
        #     mpool.failed_spools_undetected[spool.spoolId] = 1
        #     rackgroup = self.rackgroups[mpool.rackgroupId]
        #     rackgroup.affected_mpools[mpool.mpoolId] = 1
        #     self.affected_rackgroups[rackgroup.rackgroupId] = 1
        #     spool.failure_detection_time = disk.failure_detection_time
        heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

        for item in fail_report['repair_queue']:
            (e_time, e_type, e_diskId) = ast.literal_eval(item)
            heappush(self.simulation.repair_queue, (float(e_time), e_type, int(e_diskId)))
        for item in fail_report['detect_queue']:
            (e_time, e_type, e_diskId) = ast.literal_eval(item)
            heappush(self.simulation.failure_queue, (float(e_time), e_type, int(e_diskId)))
            # if e_type == Disk.EVENT_DETECT:
            #     print('yes!')
        
        # logging.info('detect queue: {}'.format(self.simulation.failure_queue))
        # logging.info('repair queue: {}'.format(self.simulation.repair_queue))
        # logging.info("affected pools: {}".format(self.affected_spools))
        # for spoolId in self.affected_spools:
        #     spool = self.spools[spoolId]
        #     logging.info("spool {} failed disks {}  failed_disks_in_repair {}".format(
        #                     spoolId, spool.failed_disks, spool.failed_disks_in_repair))
        
            
        

