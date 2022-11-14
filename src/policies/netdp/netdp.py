from disk import Disk
from policies.policy import Policy

import logging
from helpers import netdp_prio
from .pdl import network_decluster_pdl

class NetDP(Policy):
    
    def __init__(self, state):
        super().__init__(state)
    
    def update_disk_priority(self, event_type, diskId: int):
        
        failed_disk_system_tuple = self.state.get_failed_disks_each_rack()
        failed_disk_per_rack = failed_disk_system_tuple[0]
        failed_racks = failed_disk_system_tuple[1]
        
        disk = self.state.disks[diskId]
        
        logging.info("Event %s, dID %s, time: %s", event_type, diskId, self.state.curr_time)
        logging.info("Failed disk system: %s", failed_disk_per_rack)
        priorities = {}
        for dId in self.state.failed_disks:
            priorities[dId] = self.state.disks[dId].priority
                
        logging.info("Priorities: %s", priorities)
        
        if event_type == Disk.EVENT_FASTREBUILD or event_type == Disk.EVENT_REPAIR:
            # Get the current priority of the disk
            curr_priority = disk.priority
            # Remove the priority repair time and reduce priority because the disk is already repaired
            del disk.repair_time[curr_priority]
            # Reduce priority because the disk has been repaired
            disk.priority -= 1
            # QUESTION: Why mark the repair start time when its a repair event?
            disk.repair_start_time = self.state.curr_time
            
            max_priority = 0
            for dId in self.state.failed_disks:
                max_priority = max(max_priority, self.state.disks[dId].priority)
            
            logging.info("Disk priority: %s", disk.priority)
            
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            for dId in all_failed_disks:
                # Do not do ADAPT for now
                self.update_disk_repair_time(dId, self.state.disks[dId].priority, max_priority, failed_disk_system_tuple)
                logging.info("===")
                
        if event_type == Disk.EVENT_FAIL:
            logging.info("Max prio: %s", failed_racks)
            all_failed_disks = list(dId for v in failed_disk_per_rack.values() for dId in v)
            
            # Good num / failed num
            # Good num and failed num does not directly get involved in priority percent calculation
            # It it used to determine which formula to use to caluclate pp
            good_num = self.state.sys.num_disks
            for value in failed_disk_per_rack.values():
                good_num -= len(value)
            fail_num = self.state.sys.num_disks - good_num
            disk.good_num = good_num
            disk.fail_num = fail_num
            
            # We need to check if the disk failed in a previously unimpacted rack
            # If so, the priority is failed_racks + 1
            # Otherwise, the priority remains failed_racks because failures within an impacted rack does not affect priotiy 
            rackId = diskId // self.sys.num_disks_per_rack
            max_priority = 0
            for dId in self.state.failed_disks:
                max_priority = max(max_priority, self.state.disks[dId].priority)
                
            logging.info("Max priority before: %s", max_priority)
            
            # If the current failure happens in a brand new rack, we need to increment max_priority
            #   otherwise, if the failure happened within the same rack, there will be no priority increase
            if (len(failed_disk_per_rack[rackId]) == 1):
                max_priority += 1
            
            logging.info("Max priority after: %s", max_priority)
            
            disk.priority = max_priority
            
            # if (len(failed_disk_per_rack[rackId]) == 1):
            #     disk.priority = failed_racks
            # else:
            #     disk.priority = failed_racks - 1
            
            
            disk.repair_start_time = self.state.curr_time
            
            # Ignore ADAPT for now
            for dId in all_failed_disks:
                self.update_disk_repair_time(dId, self.state.disks[dId].priority, max_priority, failed_disk_system_tuple)
                logging.info("===")
        
        logging.info("-----")
    
    def update_disk_repair_time(self, diskId, priority, max_priority, failed_disk_system_tuple):
        failed_disk_per_rack = failed_disk_system_tuple[0]
        failed_racks = failed_disk_system_tuple[1]
        logging.info("Updating repair time for diskId %d, prio %d, max prio: %s", diskId, priority, max_priority)
        logging.info("Disk %s", str(self.state.disks[diskId]))
        disk = self.state.disks[diskId]
        
        repaired_time = self.state.curr_time - disk.repair_start_time
        if repaired_time == 0:
            # This means that the repair just started on a Disk.EVENT_FAIL
            # We calculate how many stripes that store data on this disk is impacted by this priority
            priority_percent = netdp_prio.priority_percent(self.state, disk, failed_disk_per_rack, max_priority, priority)
            logging.info("Priority percent: %s for racks: %s and disk prio: %s", priority_percent, failed_disk_per_rack, disk.priority)
            repaired_percent = 0
            # Disk capacity * percentage of chunks/data in this disk that we need to repair (for this priority)
            disk.curr_repair_data_remaining = disk.repair_data * priority_percent
            
            # QUESTION: why minus the data amount?
            # Data remaining amplified by reading from k chunks, and writing to (priority - 1) chunks
            # if priority > 1:
            #     self.sys.metrics.total_rebuild_io_per_year -= disk.curr_repair_data_remaining * (priority - 1) * self.sys.k
                
        else:
            # We calculate the repair data remaining from itself because we iteratively 
            #  "restart" the repair every iteration by setting new repair start time to curr_time
            # print(disk.repair_time)
            repaired_percent = repaired_time / disk.repair_time[priority]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)

        
        repair_time = self.calc_repair_time(disk, failed_racks, priority)
        
        disk.repair_time[priority] = repair_time / 3600 / 24
        disk.repair_start_time = self.state.curr_time
        disk.estimate_repair_time = self.state.curr_time + disk.repair_time[priority]
        
        logging.info("Time needed for repair %s d, will be repaired at day %s", (repair_time / 3600 / 24), disk.estimate_repair_time)
        
    def calc_repair_time(self, disk, failed_racks, priority):
        # For read parallelism, we need to sum all the surviving disks outside of the impacted racks
        # Note that this is an assumption made to reduce the complexity of the simulation
        # parallelism = disk.good_num / (self.sys.k + 1)
        # parallelism = self.calc_parallelism(failed_racks)
        
        # amplification = self.sys.k + priority        
        
        # if priority < failed_racks:
        #     # This means that we need to yield bandwidth to the disks with higher priority
        #     repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism/failed_racks)
        # else:
        #     repair_time = disk.curr_repair_data_remaining*amplification/(self.sys.diskIO*parallelism)
            
        # logging.info("Failed racks %s", failed_racks)
        # logging.info("Parallelism: %s, amplification: %s, data: %s, diskIO: %s", parallelism, amplification, disk.curr_repair_data_remaining, self.sys.diskIO)
        # return repair_time
        
        # We calculate the repair time for RAID, and divide that by speed up brought by net dp
        repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO)
        
        participating_disks = disk.good_num
        speed_up = participating_disks // (self.sys.k + 1)
        
        return repair_time / speed_up
    
    def calc_parallelism(self, failed_racks, exp=False):
        # return (len(self.racks) - failed_racks) * self.sys.num_disks_per_rack
        return self.sys.num_disks - len(self.state.failed_disks)
    
    def check_pdl(self):
        return network_decluster_pdl(self.state)