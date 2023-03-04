import logging
import time
import numpy as np
from typing import Optional, List, Dict, Tuple

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack
from components.network import NetworkUsage
from policies.policy import Policy
from .pdl import net_raid_pdl
from .repair import netraid_repair

class NetRAID(Policy):
    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, state):
        super().__init__(state)
        self.max_prio = 0
        self.num_rack_groups = self.sys.num_racks // self.sys.top_n
        # we divide the system into multiple rack groups
        # each rack group contains multiple netraid pools
        # In a rack group, each racks' network bandwidth would be the same,
        # because a disk's repair will need to read data from all other racks and then write to the target rack.
        # Here singerack_interrack_bandwidth_per_rackgroup records the remaining interrack-bandwidth of a single rack in a rack group. 
        self.singerack_avail_interrack_bandwidth_per_rackgroup = [self.sys.interrack_speed] * self.num_rack_groups
        self.singerack_avail_intrarack_bandwidth_per_rackgroup = [self.sys.intrarack_speed] * self.num_rack_groups


    def update_disk_state(self, event_type: str, diskId: int) -> None:
        rackId = diskId // self.sys.num_disks_per_rack
        disk = self.state.disks[diskId]
        if event_type == Disk.EVENT_REPAIR:
            disk.state = Disk.STATE_NORMAL
            # This is removing the disk from the failed disk array
            self.state.racks[rackId].failed_disks.pop(diskId, None)
            self.state.failed_disks.pop(diskId, None)
            self.sys.metrics.disks_aggregate_down_time += self.curr_time - self.disks[diskId].metric_down_start_time
            
                        
        if event_type == Disk.EVENT_FAIL:
            disk.state = Disk.STATE_FAILED
            self.state.racks[rackId].failed_disks[diskId] = 1
            self.state.failed_disks[diskId] = 1
            self.disks[diskId].metric_down_start_time = self.curr_time



    #----------------------------------------------
    # raid net
    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        if event_type == Disk.EVENT_REPAIR:
            disk = self.disks[diskId]
            
            self.sys.metrics.total_net_traffic += disk.repair_data * (self.sys.top_k + 1)
            self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.top_k + 1)
            
            spoolId = disk.spoolId
            failed_disks_per_spool = self.state.get_failed_disks_per_spool(spoolId)
            self.max_prio = max(self.max_prio, len(failed_disks_per_spool))
            logging.info("Repair event of disk %s on stripe %s", diskId, spoolId)
            # logging.info("Failed spools: %s", self.state.get_failed_disks_each_spool())
            
            # This is updating the rest of the failed disks for their repair time.
            #  If there is only one failure in the spool, this would not be run
            for diskId in failed_disks_per_spool:
                # If the disk is being delayed for repair, we do not update its time
                if not self.state.simulation.delay_repair_queue[Components.DISK].get(diskId, False):
                    self.update_disk_repair_time(diskId, failed_disks_per_spool)

        if event_type in [Disk.EVENT_FAIL, Disk.EVENT_DELAYED_FAIL]:
            disk = self.disks[diskId]
            # Note: the assignment of repair_start_time is moved into update_disk_repair_time()
            #  this is because there is a chance that we might need to delay repair
            disk.repair_start_time = self.curr_time

            failed_disks_per_spool = self.state.get_failed_disks_per_spool(disk.spoolId)
            self.max_prio = max(self.max_prio, len(failed_disks_per_spool))
            # logging.info("Failed spools: %s", self.state.get_failed_disks_each_spool())
            # logging.info("  update_disk_priority_raid_net event: {} spoolId: {} failed_disks_per_spool: {}".format(
            #                 event_type, disk.spoolId, failed_disks_per_spool))
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the rebuild IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            for diskId_per_spool in failed_disks_per_spool:
                self.update_disk_repair_time(diskId_per_spool, failed_disks_per_spool)
    
    
    def update_disk_repair_time(self, diskId, num_fail_in_spool, num_fail_in_rackgroup):
        disk = self.disks[diskId]
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            # This means that the repair is on going, we need to update the remaining data
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
        
        rebuild_rate = min(self.sys.diskIO / num_fail_in_spool, self.sys.interrack_speed / num_fail_in_rackgroup)
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / fail_per_spool)
        logging.info("Repaired percent %s, Repair time %s", repaired_percent, repair_time)
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

    
    def update_bandwidth(self, disk, disk_to_read_from: List[int]) -> Optional[NetworkUsage]:
        # Calculate intrarack, from k randomly selected drives from the spool
        intra_rack = {}
        for diskId in disk_to_read_from:
            rackId = self.state.disks[diskId].rackId
            if self.sys.diskIO <= self.state.network.intra_rack_avail[rackId]:
                # We have more bandwidth than disk upload
                intra_rack[rackId] = self.sys.diskIO
                self.state.network.intra_rack_avail[rackId] -= self.sys.diskIO
                # logging.info("Using %s net bandwidth from rack %s", self.sys.diskIO, rackId)
            else:
                # We have less bandwidth than the diskIO, we then take all
                intra_rack[rackId] = self.state.network.intra_rack_avail[rackId]
                self.state.network.intra_rack_avail[rackId] = 0
                # logging.info("Using %s net bandwidth from rack %s", self.state.network.intra_rack_avail[rackId], rackId)
        
        # If the total upload from racks are larger than the avail inter-rack, we use all
        # TODO: if this is the case, we also need to adjust intra-rack bandwidth of each rack
        total_upload_bandwidth = np.sum(list(intra_rack.values()))
        if total_upload_bandwidth > self.state.network.inter_rack_avail:
            inter_rack = self.state.network.inter_rack_avail
            self.state.network.inter_rack_avail = 0
        else:
            inter_rack = total_upload_bandwidth
            self.state.network.inter_rack_avail -= total_upload_bandwidth
        
        return NetworkUsage(inter_rack, intra_rack) 
        
    def check_pdl(self):
        return net_raid_pdl(self, self.state)
    
    def update_repair_events(self, repair_queue):
        netraid_repair(self.state, repair_queue)
        
    def intercept_next_event(self, prev_event) -> Optional[Tuple[float, str, int]]:
        logging.info("Trying to intercept event with delay repair queue length of %s", len(self.state.simulation.delay_repair_queue[Components.DISK]))
        # Check whether there are delayed repaired disks that satisfy the requirement
        if len(self.state.simulation.delay_repair_queue[Components.DISK]) == 0 \
                or self.state.network.inter_rack_avail == 0:
            return None
        
        # We check all the disks that have been delayed for repairs
        for diskId in self.state.simulation.delay_repair_queue[Components.DISK].keys():
            disk = self.state.disks[diskId]
            # This means that we have enough bandwidth to carry out the repair
            disk_to_read_from = self.disks_to_read_for_repair(disk)
            logging.info("Trying to initiate delayed repair for disk %s with inter-rack of %s and avail peer of %s (k=%s)", diskId, self.state.network.inter_rack_avail, len(disk_to_read_from), self.sys.top_k)
            if self.state.network.inter_rack_avail != 0 and len(disk_to_read_from) >= self.sys.top_k:
                logging.info("Delayed disk %s now has enough bandwidth, repairing", diskId)
                del self.state.simulation.delay_repair_queue[Components.DISK][diskId]
                return (prev_event[0], Disk.EVENT_DELAYED_FAIL, diskId)
        
        return None