import logging
from components.disk import Disk
from components.rack import Rack
from policies.policy import Policy
from .pdl import slec_local_cp_pdl
from .repair import slec_local_cp_repair

class SLEC_LOCAL_CP_RS1(Policy):
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
            disk.repair_start_time = self.curr_time
            for dId in spool.failed_disks:
                self.update_disk_repair_time(dId, len(spool.failed_disks))
            
            if len(spool.failed_disks) >= self.sys.num_local_fail_to_report:
                self.sys_failed = True
                if self.sys.collect_fail_reports:
                    fail_report = {'curr_time': self.curr_time, 'disk_infos': []}
                    for failedDiskId in self.failed_disks:
                        failedDisk = self.disks[failedDiskId]
                        fail_report['disk_infos'].append(
                            {
                            'curr_repair_data_remaining': failedDisk.curr_repair_data_remaining,
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
            for dId in spool.failed_disks:
                self.update_disk_repair_time(dId, len(spool.failed_disks))

        
        
        

    def update_disk_repair_time(self, diskId, fail_per_rack):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
            
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_rack)

        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        
    
    def check_pdl(self):
        return slec_local_cp_pdl(self)
    
    def update_repair_events(self, repair_queue):
        return slec_local_cp_repair(self, repair_queue)

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
    
    def manual_inject_failures(self, fail_report):
        for disk_info in fail_report['disk_infos']:
            diskId = int(disk_info['diskId'])
            disk = self.sys.disks[diskId]
            disk.state = Disk.STATE_FAILED
            disk.curr_repair_data_remaining = float(disk_info['curr_repair_data_remaining'])
            disk.estimate_repair_time = float(disk_info['estimate_repair_time'])
            disk.repair_time[0] = float(disk_info['repair_time']['0'])
            disk.repair_start_time = float(disk_info['repair_start_time'])
            self.failed_disks[diskId] = 1

            # logging.info('disk: {}'.format(disk))

            spool = self.spools[disk.spoolId]
            spool.failed_disks[diskId] = 1
        
