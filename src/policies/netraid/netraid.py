import logging
import time
import numpy as np
from typing import Optional, List, Dict, Tuple

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
        self.sys = state.sys
        self.n = state.n
        self.racks = state.racks
        self.disks = state.disks
        self.curr_time = state.curr_time
        self.failed_disks = state.failed_disks
        self.failed_racks = state.failed_racks

    #----------------------------------------------
    # raid net
    #----------------------------------------------
    def update_disk_priority(self, event_type, diskId):
        if event_type == Disk.EVENT_REPAIR:
            disk = self.disks[diskId]
            
            self.sys.metrics.total_net_traffic += disk.repair_data * (self.sys.top_k + 1)
            self.sys.metrics.total_rebuild_io_per_year += disk.repair_data * (self.sys.top_k + 1)
            
            stripesetId = disk.stripesetId
            failed_disks_per_stripeset = self.state.get_failed_disks_per_stripeset(stripesetId)
            logging.info("Repair event of disk %s on stripe %s", diskId, stripesetId)
            # logging.info("Failed stripesets: %s", self.state.get_failed_disks_each_stripeset())
            
            # This is updating the rest of the failed disks for their repair time.
            #  If there is only one failure in the stripeset, this would not be run
            for diskId in failed_disks_per_stripeset:
                # If the disk is being delayed for repair, we do not update its time
                if diskId not in self.state.simulation.delay_repair_queue:
                    self.update_disk_repair_time(diskId, failed_disks_per_stripeset)

        if event_type in [Disk.EVENT_FAIL, Disk.EVENT_DELAYED_FAIL]:
            disk = self.disks[diskId]
            # Note: the assignment of repair_start_time is moved into update_disk_repair_time()
            #  this is because there is a chance that we might need to delay repair
            disk.repair_start_time = self.curr_time

            failed_disks_per_stripeset = self.state.get_failed_disks_per_stripeset(disk.stripesetId)
            # logging.info("Failed stripesets: %s", self.state.get_failed_disks_each_stripeset())
            logging.info("  update_disk_priority_raid_net event: {} stripesetId: {} failed_disks_per_stripeset: {}".format(
                            event_type, disk.stripesetId, failed_disks_per_stripeset))
            #--------------------------------------------
            # calculate repair time for disk failures
            # all the failed disks need to read data from other surviving disks in the group to rebuild data
            # so the rebuild IO is shared by all failed disks
            # we need to update the repair rate for all failed disks, because every failed disk gets less share now
            #--------------------------------------------
            for diskId_per_stripeset in failed_disks_per_stripeset:
                if diskId_per_stripeset not in self.state.simulation.delay_repair_queue:
                    self.update_disk_repair_time(diskId_per_stripeset, failed_disks_per_stripeset)
    
    
    def update_disk_repair_time(self, diskId, failed_disks_in_stripeset):
        logging.info("Updating repair time for disk %s", diskId)
        disk = self.disks[diskId]
        fail_per_stripeset = len(failed_disks_in_stripeset)
        logging.info("Bandwidth before usage - inter: %s, intra: %s", self.sys.network.inter_rack_avail, self.sys.network.intra_rack_avail)
        logging.info("Disk detail %s", disk)
        repaired_time = self.curr_time - disk.repair_start_time
        if repaired_time == 0:
            # This means that the repair just got started
            
            #------------
            # Network: since we currently assume that we do the construction in a dedicated rack
            #   we remove the bandwidth from intrarack, as well as interrack because
            #   the traffic has to travel from node to TOR
            #------------
            disk_to_read_from = self.disks_to_read_for_repair(disk)
            logging.info("Reading from %s disks to complete repair", disk_to_read_from)
            if self.sys.network.inter_rack_avail != 0 and len(disk_to_read_from) >= self.sys.top_k:
                # This means that we begin repair
                # 1. Subtract the bandwidth away from network
                network_usage = self.update_bandwidth(disk, disk_to_read_from)
                logging.info("Network usage: %s", network_usage.__dict__)
                logging.info("Bandwidth after usage - inter: %s, intra: %s", self.sys.network.inter_rack_avail, self.sys.network.intra_rack_avail)
                # 2. Assign the network usage to the repairing disk
                disk.network_usage = network_usage
            else:
                # If we do not have enough bandwidth to carry out repair, we delay the repair
                # logging.warning("Not enough bandwidth, delaying repair")
                self.state.simulation.delay_repair_queue.append(diskId)
                return
            
            repaired_percent = 0
            disk.curr_repair_data_remaining = disk.repair_data
        else:
            # This means that the repair is on going, we need to update the remaining data
            
            # Also note that we do not check available bandwidth here as we currently assume the network repair is non-interruptable
            repaired_percent = repaired_time / disk.repair_time[0]
            disk.curr_repair_data_remaining = disk.curr_repair_data_remaining * (1 - repaired_percent)
            
            # We pull the network usage from the disk
            network_usage = disk.network_usage

        assert network_usage is not None
        # logging.info("Repairing with network bandwidth of %s", network_usage.inter_rack)
        repair_time = float(disk.curr_repair_data_remaining) / (self.sys.diskIO / fail_per_stripeset)
        # repair_time = float(disk.curr_repair_data_remaining)/(self.sys.diskIO/fail_per_stripeset)
        logging.info("Repaired percent %s, Repair time %s", repaired_percent, repair_time)
        disk.repair_time[0] = repair_time / 3600 / 24
        disk.repair_start_time = self.curr_time
        disk.estimate_repair_time = self.curr_time + disk.repair_time[0]
        logging.info("  curr time: {}  repair time: {}  finish time: {}".format(self.curr_time, disk.repair_time[0], disk.estimate_repair_time))

    # TODO: combine this with update_bandwidth to save time
    def disks_to_read_for_repair(self, disk: Disk) -> List[int]:
        stripeset = self.sys.net_raid_stripesets_layout[disk.stripesetId]
        disk_to_read_from = []
        for diskId in stripeset:
            if len(disk_to_read_from) < self.sys.top_k \
                and self.state.disks[diskId].state == Disk.STATE_NORMAL \
                and self.state.racks[disk.rackId].state == Rack.STATE_NORMAL \
                and self.sys.network.intra_rack_avail[disk.rackId] != 0:
                disk_to_read_from.append(diskId)
        
        return disk_to_read_from
    
    def update_bandwidth(self, disk, disk_to_read_from: List[int]) -> Optional[NetworkUsage]:
        # Calculate intrarack, from k randomly selected drives from the stripeset
        intra_rack = {}
        for diskId in disk_to_read_from:
            rackId = self.state.disks[diskId].rackId
            if self.sys.diskIO <= self.sys.network.intra_rack_avail[rackId]:
                # We have more bandwidth than disk upload
                intra_rack[rackId] = self.sys.diskIO
                self.sys.network.intra_rack_avail[rackId] -= self.sys.diskIO
                # logging.info("Using %s net bandwidth from rack %s", self.sys.diskIO, rackId)
            else:
                # We have less bandwidth than the diskIO, we then take all
                intra_rack[rackId] = self.sys.network.intra_rack_avail[rackId]
                self.sys.network.intra_rack_avail[rackId] = 0
                # logging.info("Using %s net bandwidth from rack %s", self.sys.network.intra_rack_avail[rackId], rackId)
        
        # If the total upload from racks are larger than the avail inter-rack, we use all
        # TODO: if this is the case, we also need to adjust intra-rack bandwidth of each rack
        total_upload_bandwidth = np.sum(list(intra_rack.values()))
        if total_upload_bandwidth > self.sys.network.inter_rack_avail:
            inter_rack = self.sys.network.inter_rack_avail
            self.sys.network.inter_rack_avail = 0
        else:
            inter_rack = total_upload_bandwidth
            self.sys.network.inter_rack_avail -= total_upload_bandwidth
        
        return NetworkUsage(inter_rack, intra_rack) 
        
    def check_pdl(self):
        return net_raid_pdl(self.state)
    
    def update_repair_events(self, repair_queue):
        netraid_repair(self.state, repair_queue)
        
    def intercept_next_event(self, prev_event) -> Optional[Tuple[float, str, int]]:
        logging.info("Trying to intercept event with delay repair queue length of %s", len(self.state.simulation.delay_repair_queue))
        # Check whether there are delayed repaired disks that satisfy the requirement
        if len(self.state.simulation.delay_repair_queue) == 0 or self.sys.network.inter_rack_avail == 0:
            return None
        
        # We check all the disks that have been delayed for repairs
        for diskId in self.state.simulation.delay_repair_queue:
            disk = self.state.disks[diskId]
            # This means that we have enough bandwidth to carry out the repair
            disk_to_read_from = self.disks_to_read_for_repair(disk)
            logging.info("Trying to initiate delayed repair for disk %s with inter-rack of %s and avail peer of %s (k=%s)", diskId, self.sys.network.inter_rack_avail, len(disk_to_read_from), self.sys.top_k)
            if self.sys.network.inter_rack_avail != 0 and len(disk_to_read_from) >= self.sys.top_k:
                logging.info("Delayed disk %s now has enough bandwidth, repairing", diskId)
                self.state.simulation.delay_repair_queue.remove(diskId)
                return (prev_event[0], Disk.EVENT_DELAYED_FAIL, diskId)
        
        return None