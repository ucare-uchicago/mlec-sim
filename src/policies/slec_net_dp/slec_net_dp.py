import logging

from components.disk import Disk
from policies.policy import Policy

from helpers import netdp_prio
from .pdl import slec_net_dp_pdl
from .repair import slec_net_dp_repair
import random
from heapq import heappush
import json

class SLEC_NET_DP(Policy):
    
    def __init__(self, state):
        super().__init__(state)
        self.affected_racks = {}
        self.priority_queue = {}
        for i in range(self.sys.top_m + 1):
            self.priority_queue[i + 1] = {}
        self.max_priority = 0
        self.repair_max_priority = 0
        self.total_interrack_bandwidth = state.sys.interrack_speed * state.sys.num_racks
        self.failed_disks_undetected = {}
        self.sys_failed = False

    def update_disk_state(self, event_type: str, diskId: int) -> None:
        disk = self.state.disks[diskId]
        rackId = disk.rackId

        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.racks[rackId].failed_disks[diskId] = 1
            self.affected_racks[rackId] = 1
            self.failed_disks[diskId] = 1
            self.failed_disks_undetected[diskId] = 1

        if event_type == Disk.EVENT_REPAIR:
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.racks[rackId].failed_disks.pop(diskId, None)
            if len(self.racks[rackId].failed_disks) == 0:
                self.affected_racks.pop(rackId, None)
            self.failed_disks.pop(diskId, None)

    
    def update_disk_priority(self, event_type, diskId: int):        
        disk = self.disks[diskId]
        rackId = disk.rackId

        if event_type == Disk.EVENT_DETECT:
            if self.repair_max_priority > 0:
                for dId in self.priority_queue[self.repair_max_priority]:
                    self.pause_repair_time(dId, self.repair_max_priority)
            
            self.repair_max_priority = max(self.repair_max_priority, disk.priority)
            self.priority_queue[disk.priority][diskId] = 1
            self.failed_disks_undetected.pop(diskId, None)

            disk.repair_start_time = self.state.curr_time
            disk.curr_prio_repair_started = False
            disk.failure_detection_time = 0
            
            if self.repair_max_priority > 0:
                for dId in self.priority_queue[self.repair_max_priority]:
                    self.resume_repair_time(dId, self.disks[dId].priority, rackId)


        if event_type == Disk.EVENT_FAIL:
            if self.repair_max_priority > 0:
                for dId in self.priority_queue[self.repair_max_priority]:
                    self.pause_repair_time(dId, self.repair_max_priority)
            
            # Imagine there are 2 affected racks. And a new disk fails.
            # If the current failure happens in a brand new rack, we need to increment max_priority
            # If the current failure happens in an already affected rack, but the current max-priority is 1. 
            #     it means previous 2-chunk-failure have been repaired. But the new disk failure will lead to new 2-chunk-failure stripes.
            #     so we still need to increment max_priority
            # Otherwise, we don't increase max-priority.
            if (len(self.state.racks[rackId].failed_disks) == 1) or self.max_priority < len(self.affected_racks):
                self.max_priority += 1
            
            disk.priority = self.max_priority

            good_num = self.sys.num_disks - len(self.failed_disks)
            fail_num = len(self.failed_disks)
            disk.good_num = good_num
            disk.fail_num = fail_num
            self.compute_priority_percents(disk, rackId)

            disk.failure_detection_time = self.curr_time + self.sys.detection_time

            if self.repair_max_priority > 0:
                for dId in self.priority_queue[self.repair_max_priority]:
                    self.resume_repair_time(dId, self.disks[dId].priority, rackId)

            if self.max_priority >= self.sys.num_net_fail_to_report:
                # if a disk has infinite chunks, then there must be some stripe that have max_priority
                if self.sys.infinite_chunks:
                    self.sys_failed = True

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
                # otherwise it's possible that the system is lucky and have no max_priority stripe
                # we need to compute the probability for the system to be lucky
                else:
                    stripe_survival_prob = 1-netdp_prio.compute_priority_percent(self.state, self.affected_racks, rackId, self.max_priority)
                    all_disk_stripe_survival_prob = stripe_survival_prob ** self.sys.num_chunks_per_disk
                    sample = random.choices([0,1], [all_disk_stripe_survival_prob, 1-all_disk_stripe_survival_prob])
                    # print("stripe_survival_prob: {} all_disk_stripe_survival_prob: {} self.sys.num_chunks_per_disk: {}  sample:{}".format(
                    #     stripe_survival_prob, all_disk_stripe_survival_prob, self.sys.num_chunks_per_disk, sample
                    # ))
                    if sample[0] == 1:
                        self.sys_failed = True
                        return
                    else:
                        self.max_priority -= 1



        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            # ----------------
            # Remove the priority repair time and reduce priority because the disk is already repaired
            # Also remove it from the priority queue, and pause the repair of other disks in the queue.
            curr_priority = disk.priority
            assert curr_priority == self.repair_max_priority, "repair disk priority is not system disk repair max priority"
            del disk.repair_time[curr_priority]
            del disk.priority_percents[curr_priority]
            
            self.priority_queue[curr_priority].pop(diskId, None)
            for dId in self.priority_queue[curr_priority]:
                self.pause_repair_time(dId, curr_priority)
            
            # ----------------
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            if disk.priority > 0:
                self.priority_queue[disk.priority][diskId] = 1

            disk.repair_start_time = self.state.curr_time
            disk.curr_prio_repair_started = False
            
            # update max priority when its queue is empty
            if len(self.priority_queue[self.repair_max_priority]) == 0:
                self.repair_max_priority -= 1
                self.max_priority -= 1       
                for dId in self.failed_disks_undetected:
                    failedDisk = self.disks[dId]
                    failedDisk.priority -= 1

            if self.repair_max_priority > 0:
                for dId in self.priority_queue[self.repair_max_priority]:
                    self.resume_repair_time(dId, self.disks[dId].priority, rackId)
                

    
    
    def compute_priority_percents(self, disk, rackId):
        for i in range(disk.priority):
            priority = i+1
            disk.priority_percents[priority] = netdp_prio.compute_priority_percent(self.state, self.affected_racks, rackId, priority)
        # logging.info("priority_percents: {}".format(disk.priority_percents))
        

    def pause_repair_time(self, diskId, priority):
        disk = self.state.disks[diskId]
        repaired_time = self.state.curr_time - disk.repair_start_time
        repaired_percent = repaired_time / disk.repair_time[priority]
        disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
    
    def resume_repair_time(self, diskId, priority, rackId):
        disk = self.state.disks[diskId]
        if not disk.curr_prio_repair_started:
            priority_percent = disk.priority_percents[priority]
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            disk.curr_prio_repair_started = True
        repair_time = self.calc_repair_time(disk, priority)
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.state.curr_time
        disk.estimate_repair_time = self.state.curr_time + disk.repair_time[priority]

        
    def calc_repair_time(self, disk, priority):
        total_disk_IO = (self.sys.num_disks - len(self.failed_disks)) * self.sys.diskIO
        total_repair_bandwidth = min(total_disk_IO, self.total_interrack_bandwidth)

        total_repair_data_readwrite = float(disk.curr_repair_data_remaining) * (self.sys.top_k + 1) 
        # we repair multiple disks concurrently. So rebuild bandwidth is shared
        per_disk_total_repair_bandwidth = total_repair_bandwidth / len(self.priority_queue[priority])
        repair_time = total_repair_data_readwrite / per_disk_total_repair_bandwidth
        
        return repair_time
    
    def check_pdl(self):
        return slec_net_dp_pdl(self)
    
    def update_repair_events(self, event_type, diskId, repair_queue):
        slec_net_dp_repair(self, repair_queue)
        if event_type == Disk.EVENT_FAIL:
            disk = self.disks[diskId]
            heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))

    def clean_failures(self) -> None:
        failed_disks = self.state.get_failed_disks()
        for diskId in failed_disks:
            disk = self.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.failure_detection_time = 0
            disk.repair_time.clear()
            disk.priority_percents.clear()
            self.curr_prio_repair_started = False
        for rackId in self.affected_racks:
            self.racks[rackId].failed_disks.clear()
    
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

            rackId = disk.rackId
            self.failed_disks[diskId] = 1
            self.racks[rackId].failed_disks[diskId] = 1
            self.affected_racks[rackId] = 1

            self.max_priority = max(disk.priority, self.max_priority)
            
            if disk.failure_detection_time >= simulate.curr_time:
                disk.curr_prio_repair_started = False
                self.failed_disks_undetected[diskId] = 1
            else:
                disk.curr_prio_repair_started = True
                self.priority_queue[disk.priority][diskId] = 1
                self.repair_max_priority = max(disk.priority, self.repair_max_priority)

        if self.repair_max_priority > 0:
            for diskId in self.priority_queue[self.repair_max_priority]:
                disk = self.disks[diskId]
                if disk.priority > 1:
                    heappush(simulate.repair_queue, (disk.estimate_repair_time, Disk.EVENT_FASTREBUILD, diskId))
                if disk.priority == 1:
                    heappush(simulate.repair_queue, (disk.estimate_repair_time, Disk.EVENT_REPAIR, diskId))
            
        for diskId in self.failed_disks_undetected:
            disk = self.disks[diskId]
            heappush(self.simulation.failure_queue, (disk.failure_detection_time, Disk.EVENT_DETECT, diskId))