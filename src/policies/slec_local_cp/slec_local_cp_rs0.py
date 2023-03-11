import logging
from components.disk import Disk
from policies.policy import Policy
from .pdl import slec_local_cp_pdl
from heapq import heappush

# Repair scheme 0
# For a failed stripe with 2 failed chunks, we assume the repairer can read the stripe once,
# reconstruct the 2 lost chunks together, and then write to the two spare disks in parallel.
class SLEC_LOCAL_CP_RS0(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.spools = self.sys.spools
        self.sys_failed = False

    def update_disk_state(self, event_type: str, diskId: int):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            disk.fail_time = self.curr_time
            self.failed_disks[diskId] = 1
            spool.failed_disks[diskId] = 1

        if event_type == Disk.EVENT_REPAIR:
            # logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            self.failed_disks.pop(diskId, None)
            spool.failed_disks.pop(diskId, None)
        
    

    def update_disk_priority(self, event_type, diskId):
        disk = self.disks[diskId]
        spool = self.spools[disk.spoolId]
        if event_type == Disk.EVENT_FAIL:
            self.update_disk_repair_time(disk)
            
            if len(spool.failed_disks) >= self.sys.num_local_fail_to_report:
                self.sys_failed = True
                if self.sys.collect_fail_reports:
                    fail_report = {'curr_time': self.curr_time, 'disk_infos': []}
                    for failedDiskId in self.failed_disks:
                        failedDisk = self.disks[failedDiskId]
                        fail_report['disk_infos'].append(
                            {
                            'diskId': int(failedDiskId),
                            'estimate_repair_time': failedDisk.estimate_repair_time,
                            'repair_time': {
                                0: failedDisk.repair_time[0]
                                },
                            'repair_start_time': failedDisk.repair_start_time
                            })
                    # logging.info('new fail report: {}'.format(fail_report))
                    self.sys.fail_reports.append(fail_report)
                return

        if event_type == Disk.EVENT_REPAIR:
            return

        
        
        

    def update_disk_repair_time(self, disk):
        repair_time = float(self.sys.diskSize)/float(self.sys.diskIO)

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        
    
    def check_pdl(self):
        return slec_local_cp_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            heappush(repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))

    def clean_failures(self):
        affected_spools = {}
        for diskId in self.failed_disks:
            disk = self.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.repair_time = {}

            affected_spools[disk.spoolId] = 1
        
        for spoolId in affected_spools:
            spool = self.spools[spoolId]
            spool.failed_disks.clear()
    
    def manual_inject_failures(self, fail_report, simulate):
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_time[0] = float(disk_info['repair_time']['0'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            self.failed_disks[diskId] = 1

            spool = self.spools[disk.spoolId]
            spool.failed_disks[diskId] = 1

            heappush(simulate.repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
        
