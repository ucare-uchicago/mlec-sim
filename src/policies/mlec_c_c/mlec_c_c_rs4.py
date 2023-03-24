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

class MLEC_C_C_RS4(Policy):
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
            spool.failed_disks.pop(diskId, None)
            if len(spool.failed_disks) == 0:
                self.affected_spools.pop(disk.spoolId, None)
                self.mpools[spool.mpoolId].affected_spools.pop(disk.spoolId, None)
            
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            self.affected_spools[disk.spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[disk.spoolId] = 1


    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]

        if event_type == Disk.EVENT_FAIL:
            # If the spool is already failing, we do nothing because it's in reconstruction anyway
            if spool.state == Spool.STATE_FAILED:
                # logging.info("Diskgroup already in failed state, ignoring")
                return
            mpool = self.mpools[spool.mpoolId]
            disk.repair_start_time = self.curr_time
            self.update_disk_repair_time(diskId)

            if len(mpool.failed_spools) >= self.sys.num_net_fail_to_report and len(spool.failed_disks) >= self.sys.num_local_fail_to_report:
                self.loss_trigger_diskId = diskId
                self.sys_failed = True

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
            new_failure_intervals = self.simulation.failure_generator.gen_new_failures(len(spool.failed_disks))
            for i, failedDiskId in enumerate(spool.failed_disks):
                self.disks[failedDiskId].state = Disk.STATE_NORMAL
                disk_fail_time = new_failure_intervals[i] + self.curr_time
                if disk_fail_time < self.simulation.mission_time:
                    heappush(self.simulation.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, failedDiskId))
            
            spool.failed_disks.clear()
            spool.state = Spool.STATE_NORMAL
            self.affected_spools.pop(spool.spoolId, None)
            mpool = self.mpools[spool.mpoolId]
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
            if num_failed_spools_per_mpool >= self.sys.num_net_fail_to_report:
                if self.sys.num_local_fail_to_report == 0:
                    self.sys_failed = True
                    self.loss_trigger_diskId = diskId
                for affectedSpoolId in mpool.affected_spools:
                    if affectedSpoolId not in mpool.failed_spools:
                        affected_spool = self.spools[affectedSpoolId]
                        if len(affected_spool.failed_disks) >= self.sys.num_local_fail_to_report:
                            self.sys_failed = True
                            self.loss_trigger_diskId = diskId
                
            spool.repair_start_time = self.curr_time
            rackgroup = self.rackgroups[mpool.rackgroupId]
            # logging.info("repairing spool...")


            if len(mpool.failed_spools) > 1:
                # this mpool is already in repair. So no need to update other mpools' repair time
                self.update_spool_repair_time(spool.spoolId)
            else:
                # this mpool is now in repair, which is goind to steal network bandwidth from other mpools in the same rackgroup
                # therefore, we need to update network bandwidth for all mpools in repair in this rackgroup
                mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))
                for affected_mpoolId in rackgroup.affected_mpools:
                    affected_mpool = self.mpools[affected_mpoolId]
                    affected_mpool.repair_rate = mpool_repair_rate
                    for failedSpoolId in affected_mpool.failed_spools:
                        self.update_spool_repair_time(failedSpoolId)
        


        if event_type == Spool.EVENT_REPAIR:
            num_repair_in_mpool = len(mpool.failed_spools)
            rackgroup = self.rackgroups[mpool.rackgroupId]

            if num_repair_in_mpool > 0:
                return
            else:
                if len(rackgroup.affected_mpools) > 0:
                    mpool_repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                            self.sys.interrack_speed / len(rackgroup.affected_mpools))
                    for mpoolId in rackgroup.affected_mpools:
                        affected_mpool = self.mpools[mpoolId]
                        affected_mpool.repair_rate = mpool_repair_rate
                        for failedSpoolId in affected_mpool.failed_spools:
                            self.update_spool_repair_time(failedSpoolId)

    
    def update_spool_repair_time(self, spoolId):
        spool = self.spools[spoolId]
        mpool = self.mpools[spool.mpoolId]
        repaired_time = self.curr_time - spool.repair_start_time
        if repaired_time == 0:            
            repaired_percent = 0
            spool.curr_repair_data_remaining = self.sys.spool_size * self.sys.diskSize/5
        else:
            repaired_percent = repaired_time / spool.repair_time[0]
            spool.curr_repair_data_remaining = spool.curr_repair_data_remaining * (1 - repaired_percent)
    
        repair_time = float(spool.curr_repair_data_remaining)/(mpool.repair_rate)
        # repair_time = float(spool.curr_repair_data_remaining)/(self.sys.interrack_speed*2)

        # logging.info("curr_repair_data_remaining {}  repair rate {}".format(spool.curr_repair_data_remaining, mpool.repair_rate))

        spool.repair_time[0] = repair_time / 3600 / 24
        spool.repair_start_time = self.curr_time
        spool.estimate_repair_time = self.curr_time + spool.repair_time[0]
        # logging.info("spoolId {} repair time: {} spool.estimate_repair_time: {}".format(spoolId, spool.repair_time[0], spool.estimate_repair_time))
        # print("spool.repair_time:{}".format(spool.repair_time[0]))


    def check_pdl(self):
        if self.sys_failed:
            self.generate_fail_report()
        return mlec_c_c_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        repair_queue.clear()
        for spoolId in self.affected_spools:
            spool = self.spools[spoolId]
            if len(spool.failed_disks) <= self.sys.m:
                for diskId in spool.failed_disks:
                    heappush(repair_queue, (self.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
        
        # logging.info("affected rack groups: {}".format(mlec_c_c.affected_rackgroups.keys()))
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            # logging.info("    rackgroup.affected_mpools_in_repair: {}".format(rackgroup.affected_mpools_in_repair.keys()))
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                # logging.info("        mpool.failed_spools_in_repair: {}".format(mpool.failed_spools_in_repair.keys()))
                for spoolId in mpool.failed_spools:
                    spool = self.spools[spoolId]
                    heappush(repair_queue, (spool.estimate_repair_time, Spool.EVENT_REPAIR, spoolId))

    def generate_fail_report(self):
        if self.sys.collect_fail_reports:
            fail_report = {'curr_time': self.curr_time, 'trigger_disk': int(self.loss_trigger_diskId), 'spool_infos': [], 
                            'disk_infos': [], 'repair_queue': [], 'detect_queue': []}
            for affectedSpoolId in self.affected_spools:
                affectedSpool = self.spools[affectedSpoolId]

                fail_report['spool_infos'].append(
                    {
                    'curr_repair_data_remaining': affectedSpool.curr_repair_data_remaining,
                    'spoolId': int(affectedSpoolId),
                    'state': affectedSpool.state,
                    'estimate_repair_time': affectedSpool.estimate_repair_time,
                    'repair_start_time': affectedSpool.repair_start_time,
                    'repair_time': json.dumps(affectedSpool.repair_time),
                    'failed_disks': json.dumps({int(k): v for k, v in affectedSpool.failed_disks.items()}),
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
                            'repair_time': json.dumps(failedDisk.repair_time),
                            })
            for (e_time, e_type, e_diskId) in list(self.simulation.repair_queue):
                fail_report['repair_queue'].append(json.dumps((e_time, e_type, int(e_diskId))))
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

            spool.state = Spool.STATE_NORMAL
            spool.failed_disks.clear()
            spool.repair_time.clear()
            self.mpools[spool.mpoolId].affected_spools.clear()

        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                mpool.failed_spools.clear()
            rackgroup.affected_mpools.clear()

    def manual_inject_failures(self, fail_report, simulate):
        # logging.info("{}".format(pformat(fail_report)))
        for spool_info in fail_report['spool_infos']:
            spoolId = int(spool_info['spoolId'])
            spool = self.sys.spools[spoolId]

            spool.state = spool_info['state']
            spool.curr_repair_data_remaining = float(spool_info['curr_repair_data_remaining'])
            spool.estimate_repair_time = float(spool_info['estimate_repair_time'])
            spool.repair_start_time = float(spool_info['repair_start_time'])
            repair_time = json.loads(spool_info['repair_time'])
            for key, value in repair_time.items():
                spool.repair_time[int(key)] = float(value)
            
            failed_disks = json.loads(spool_info['failed_disks'])
            for key, value in failed_disks.items():
                spool.failed_disks[int(key)] = int(value)
            
            self.affected_spools[spoolId] = 1
            self.mpools[spool.mpoolId].affected_spools[spoolId] = 1

            mpool = self.mpools[spool.mpoolId]
            
            rackgroup = self.rackgroups[mpool.rackgroupId]
            

            if spool.state == Spool.STATE_FAILED:
                rackgroup.affected_mpools[mpool.mpoolId] = 1
                self.affected_rackgroups[rackgroup.rackgroupId] = 1
                mpool.failed_spools[spool.spoolId] = 1
        
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_start_time = float(disk_info['repair_start_time'])

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
        
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            for mpoolId in rackgroup.affected_mpools:
                mpool = self.mpools[mpoolId]
                mpool.repair_rate = min(self.sys.diskIO * self.sys.spool_size, 
                                        self.sys.interrack_speed / len(rackgroup.affected_mpools))

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
        

        for item in fail_report['repair_queue']:
            (e_time, e_type, e_diskId) = ast.literal_eval(item)
            heappush(self.simulation.repair_queue, (float(e_time), e_type, int(e_diskId)))
            #     print('yes!')
        
        self.update_repair_events(Disk.EVENT_FAIL, int(fail_report['trigger_disk']), self.simulation.repair_queue)

        # logging.info('detect queue: {}'.format(self.simulation.failure_queue))
        # logging.info('repair queue: {}'.format(self.simulation.repair_queue))
        # logging.info("affected pools: {}".format(self.affected_spools))
        # for spoolId in self.affected_spools:
        #     spool = self.spools[spoolId]
        #     logging.info("spool {} failed disks {}  failed_disks_in_repair {}".format(
        #                     spoolId, spool.failed_disks, spool.failed_disks_in_repair))
        
            
        

