import logging
import time
import numpy as np
import json
from typing import Optional, List, Dict, Tuple
from heapq import heappush

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack
from components.spool import Spool
from policies.policy import Policy
from .pdl import slec_net_cp_pdl
from .repair import slec_net_cp_repair

class SLEC_NET_CP_RS0(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = state.sys.spools
        self.rackgroups = state.sys.rackgroups

        self.sys_failed = False
        self.affected_rackgroups = {}
        


    def update_disk_state(self, event_type: str, diskId: int) -> None:
        disk = self.state.disks[diskId]
        spoolId = disk.spoolId
        spool = self.spools[spoolId]

        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            spool.failed_disks[diskId] = 1
            spool.failed_disks_undetected[diskId] = 1
            self.affected_rackgroups[spool.rackgroupId] = 1
            self.rackgroups[spool.rackgroupId].affected_spools[spoolId] = 1
        
        if event_type == Disk.EVENT_DETECT:
            spool.failed_disks_undetected.pop(diskId, None)
            spool.failed_disks_in_repair[diskId] = 1
            self.rackgroups[spool.rackgroupId].affected_spools_in_repair[spoolId] = 1
        
        if event_type == Disk.EVENT_REPAIR:
            disk.state = Disk.STATE_NORMAL
            spool.failed_disks.pop(diskId, None)
            spool.failed_disks_in_repair.pop(diskId)

            rackgroup = self.rackgroups[spool.rackgroupId]
            if len(spool.failed_disks_in_repair) == 0:
                rackgroup.affected_spools_in_repair.pop(spoolId, None)
            if len(spool.failed_disks) == 0:
                rackgroup.affected_spools.pop(spoolId, None)
                if len(rackgroup.affected_spools) == 0:
                    self.affected_rackgroups.pop(rackgroup.rackgroupId, None)


    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spoolId = disk.spoolId
        spool = self.spools[spoolId]

        if event_type == Disk.EVENT_FAIL:
            disk.failure_detection_time = self.curr_time + self.sys.detection_time
            if len(spool.failed_disks) >= self.sys.num_net_fail_to_report:
                self.sys_failed = True
                if self.sys.collect_fail_reports:
                    fail_report = {'curr_time': self.curr_time, 'disk_infos': []}
                    for rackgroupId in self.affected_rackgroups:
                        for spoolId in self.rackgroups[rackgroupId].affected_spools:
                            for failedDiskId in self.spools[spoolId].failed_disks:
                                failedDisk = self.disks[failedDiskId]
                                
                                fail_report['disk_infos'].append(
                                    {
                                    'curr_repair_data_remaining': failedDisk.curr_repair_data_remaining,
                                    'diskId': int(failedDiskId),
                                    'estimate_repair_time': failedDisk.estimate_repair_time,
                                    'repair_start_time': failedDisk.repair_start_time,
                                    'failure_detection_time': failedDisk.failure_detection_time,
                                    'repair_time': json.dumps(failedDisk.repair_time),
                                    })
                            # logging.info('new fail report: {}'.format(fail_report))
                            self.sys.fail_reports.append(fail_report)
                return
        
        if event_type == Disk.EVENT_DETECT:
            disk.repair_start_time = self.curr_time
            disk.failure_detection_time = 0
            
            num_repair_in_spool = len(spool.failed_disks_in_repair)
            if num_repair_in_spool > 1:
                # If this pool is already repairing some disks, then the pool already received network bandwidth share.
                # Therefore, there is no need to update the repair time in other pool.
                self.update_disk_repair_time(diskId)
            else:
                # If it's the first disk failure in this pool, then the pool is going to share network bandwidth.
                # Therefore, other affected pools' network bandwidth share could decrease
                # In this case, we need to update repair time for all the affected pools in this rack group
                rackgroup = self.rackgroups[spool.rackgroupId]
                for affected_spool_id in rackgroup.affected_spools_in_repair:
                    affected_spool = self.spools[affected_spool_id]
                    affected_spool.repair_rate = min(self.sys.diskIO, self.sys.interrack_speed/len(rackgroup.affected_spools_in_repair))

                    for diskId_per_spool in affected_spool.failed_disks_in_repair:
                        self.update_disk_repair_time(diskId_per_spool)


        if event_type == Disk.EVENT_REPAIR:
            num_repair_in_spool = len(spool.failed_disks_in_repair)
            if num_repair_in_spool > 0:
                # If this pool is still reparing failed disks, then this pool still share network bandwidth
                # Therefore, there is no need to update the repair time in other pool.
                return
            else:
                # If this pool recovers, then other affected pools' network bandwidth share could increase
                # In this case, we need to update repair time for all the affected pools in this rack group
                rackgroup = self.rackgroups[disk.rackgroupId]
                for affected_spool_id in rackgroup.affected_spools_in_repair:
                    affected_spool = self.spools[affected_spool_id]
                    affected_spool.repair_rate = min(self.sys.diskIO, self.sys.interrack_speed/len(rackgroup.affected_spools_in_repair))

                    for diskId_per_spool in affected_spool.failed_disks_in_repair:
                        self.update_disk_repair_time(diskId_per_spool)
    
    
    def update_disk_repair_time(self, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            # This means that the repair is on going, we need to update the remaining data
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        repair_time = float(disk.curr_repair_data_remaining) / spool.repair_rate
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        # logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))


        
    def check_pdl(self):
        return slec_net_cp_pdl(self, self.state)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        slec_net_cp_repair(self, repair_queue)
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))
    
    def clean_failures(self) -> None:
        for rackgroupId in self.affected_rackgroups:
            rackgroup = self.rackgroups[rackgroupId]
            
            for spoolId in rackgroup.affected_spools:
                for diskId in self.spools[spoolId].failed_disks:
                    disk = self.state.disks[diskId]
                    disk.state = Disk.STATE_NORMAL
                    disk.repair_time = {}
                    disk.failure_detection_time = 0
                    disk.priority_percents = {}
                    disk.curr_prio_repair_started = False
                spool = self.spools[spoolId]
                spool.failed_disks.clear()
                spool.failed_disks_undetected.clear()
                spool.failed_disks_in_repair.clear()

            rackgroup.affected_spools.clear()
            rackgroup.affected_spools_in_repair.clear()
    

    def manual_inject_failures(self, fail_report, simulate):
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            disk.failure_detection_time = float(disk_info['failure_detection_time'])

            repair_time = json.loads(disk_info['repair_time'])
            for key, value in repair_time.items():
                disk.repair_time[int(key)] = float(value)
            
            spoolId = disk.spoolId
            spool = self.spools[spoolId]
            spool.failed_disks[diskId] = 1
            spool.failed_disks_undetected[diskId] = 1
            self.affected_rackgroups[spool.rackgroupId] = 1
            self.rackgroups[spool.rackgroupId].affected_spools[spoolId] = 1

            # for failures that are already detected
            if disk.failure_detection_time < self.simulation.curr_time:
                spool.failed_disks_undetected.pop(diskId, None)
                spool.failed_disks_in_repair[diskId] = 1
                self.rackgroups[spool.rackgroupId].affected_spools_in_repair[spoolId] = 1
        
        for rackgroupId in self.affected_rackgroups:
            for spoolId in self.rackgroups[rackgroupId].affected_spools:
                spool = self.spools[spoolId]
                # logging.info("rackgroupId: {}  spoolid: {} failed disks{} in-repair disks{} undetected:{}".format(
                #                 rackgroupId, spoolId, spool.failed_disks, spool.failed_disks_in_repair, spool.failed_disks_undetected))
                for diskId in spool.failed_disks_in_repair:
                    disk = self.disks[diskId]
                    heappush(self.simulation.repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
                for diskId in spool.failed_disks_undetected:
                    disk = self.disks[diskId]
                    heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))
        # logging.info("failure queuue: {}".format(self.simulation.failure_queue))
        