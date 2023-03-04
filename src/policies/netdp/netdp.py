import logging

from components.disk import Disk
from policies.policy import Policy

from helpers import netdp_prio
from .pdl import network_decluster_pdl
from .repair import netdp_repair
import random

class NetDP(Policy):
    
    def __init__(self, state):
        super().__init__(state)
        self.affected_racks = {}
        self.priority_queue = {}
        for i in range(self.sys.m+1):
            self.priority_queue[i+1] = {}
        self.max_priority = 0
        self.total_interrack_bandwidth = state.sys.interrack_speed * state.sys.num_racks
        

    def update_disk_state(self, event_type: str, diskId: int) -> None:
        rackId = diskId // self.sys.num_disks_per_rack
        disk = self.state.disks[diskId]
        if event_type == Disk.EVENT_REPAIR:
            # logging.info("Repair event, updating disk %s to be STATE_NORMAL", diskId)
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            if len(self.state.racks[rackId].failed_disks) == 0:
                self.affected_racks.pop(rackId, None)
            self.state.failed_disks.pop(diskId, None)
            # self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
                            
        if event_type == Disk.EVENT_FAIL:
            # logging.info("Fail event for disk {} with priority".format(diskId))
            disk.state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.affected_racks[rackId] = 1
            self.state.failed_disks[diskId] = 1
            # self.disks[diskId].metric_down_start_time = self.curr_time

    
    def update_disk_priority(self, event_type, diskId: int):        
        disk = self.state.disks[diskId]
        rackId = diskId // self.sys.num_disks_per_rack
                        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            # ----------------
            # Remove the priority repair time and reduce priority because the disk is already repaired
            # Also remove it from the priority queue, and pause the repair of other disks in the queue.
            curr_priority = disk.priority
            del disk.repair_time[curr_priority]
            self.priority_queue[curr_priority].pop(diskId, None)
            for dId in self.priority_queue[curr_priority]:
                self.pause_repair_time(dId, curr_priority)
            
            # ----------------
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            if disk.priority > 0:
                self.priority_queue[disk.priority][diskId] = 1
            # QUESTION: Why mark the repair start time when its a repair event?
            # Meng: Because you now start the next repair period, which repairs a different priority with different amounot of data. 
            disk.repair_start_time = self.state.curr_time
            # Since the disk's priority has changed, we will need to compute a new priority percent.
            disk.curr_prio_repair_started = False
            
            # update max priority when its queue is empty
            if len(self.priority_queue[self.max_priority]) == 0:
                self.max_priority -= 1            

            if self.max_priority > 0:
                for dId in self.priority_queue[self.max_priority]:
                    self.resume_repair_time(dId, self.state.disks[dId].priority, rackId)
                
        if event_type == Disk.EVENT_FAIL:
            # Good num / failed num
            # Good num and failed num does not directly get involved in priority percent calculation
            # It it used to determine which formula to use to caluclate pp
            good_num = self.state.sys.num_disks - len(self.failed_disks)
            fail_num = self.state.sys.num_disks - good_num
            disk.good_num = good_num
            disk.fail_num = fail_num

            if self.max_priority > 0:
                for dId in self.priority_queue[self.max_priority]:
                    self.pause_repair_time(dId, self.max_priority)
            
            # Imagine there are 2 affected racks. And a new disk fails.
            # If the current failure happens in a brand new rack, we need to increment max_priority
            # If the current failure happens in an already affected rack, but the current max-priority is 1. 
            #     it means previous 2-chunk-failure have been repaired. But the new disk failure will lead to new 2-chunk-failure stripes.
            #     so we still need to increment max_priority
            # Otherwise, we don't increase max-priority.
            if (len(self.state.racks[rackId].failed_disks) == 1) or self.max_priority < len(self.affected_racks):
                self.max_priority += 1
            
            if self.max_priority > self.sys.m:
                # if a disk has infinite chunks, then there must be some stripe that have max_priority
                if self.sys.infinite_chunks:
                    return 1
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
                        return 1
                    else:
                        self.max_priority -= 1
            
            disk.priority = self.max_priority

            
            self.priority_queue[disk.priority][diskId] = 1
            disk.repair_start_time = self.state.curr_time
            disk.curr_prio_repair_started = False
            self.compute_priority_percents(disk, rackId)
            
            # Ignore ADAPT for now
            for dId in self.priority_queue[self.max_priority]:
                self.resume_repair_time(dId, self.state.disks[dId].priority, rackId)
                # logging.info("===")
    
    
    def compute_priority_percents(self, disk, rackId):
        for i in range(disk.priority):
            priority = i+1
            # disk.priority_percents[priority] = netdp_prio.priority_percent(self.state, disk, failed_disk_per_rack, self.max_priority, priority)
            disk.priority_percents[priority] = netdp_prio.compute_priority_percent(self.state, self.affected_racks, rackId, priority)
        

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
        total_disk_IO = disk.good_num * self.sys.diskIO
        total_repair_bandwidth = min(total_disk_IO, self.total_interrack_bandwidth)

        total_repair_data_readwrite = float(disk.curr_repair_data_remaining) * (self.sys.k + 1) 
        # we repair multiple disks concurrently. So rebuild bandwidth is shared
        per_disk_total_repair_bandwidth = total_repair_bandwidth / len(self.priority_queue[priority])
        repair_time = total_repair_data_readwrite / per_disk_total_repair_bandwidth
        
        return repair_time
    
    def check_pdl(self):
        return network_decluster_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        return netdp_repair(self.state, repair_queue)

    def clean_failures(self) -> None:
        failed_disks = self.state.get_failed_disks()
        for diskId in failed_disks:
            disk = self.state.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}
            self.curr_prio_repair_started = False
        for rackId in self.affected_racks:
            self.state.racks[rackId].failed_disks = {}