from __future__ import annotations
from typing import List, Dict
import logging
import numpy as np
import typing
if typing.TYPE_CHECKING:
    from policies.mlec.mlec import MLEC

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack
from components.diskgroup import Diskgroup
from components.network import NetworkUsage

def disks_to_read_for_repair(disk: Disk, mlec: MLEC) -> List[int]:
    diskgroupId = disk.diskId // mlec.n
    disks_to_read = []
    for diskId in mlec.diskgroups[diskgroupId].disks:
        # Check whether this disk is NORMAL state
        if len(disks_to_read) >= mlec.sys.k:
            break
        
        if mlec.disks[diskId].state == Disk.STATE_NORMAL \
            and mlec.state.racks[disk.rackId].state == Rack.STATE_NORMAL \
            and mlec.state.network.intra_rack_avail[disk.rackId] != 0:
            disks_to_read.append(diskId)
            
    return disks_to_read

def diskgroup_to_read_for_repair(stripesetId: int, mlec: MLEC) -> List[int]:
    diskgroup_to_read = []
    for diskgroupId in mlec.state.sys.diskgroup_stripesets[stripesetId]:
        if len(diskgroup_to_read) >= mlec.sys.top_k:
            break
        
        if mlec.diskgroups[diskgroupId].state == Diskgroup.STATE_NORMAL \
            and mlec.state.network.intra_rack_avail[mlec.diskgroups[diskgroupId].rackId] != 0:
                diskgroup_to_read.append(diskgroupId)
                
    return diskgroup_to_read
    
def initial_repair(disk: Disk, disk_to_read_from: List[int], mlec: MLEC) -> NetworkUsage:
    # Calculate intrarack, from k randomly selected drives from the stripeset
    intra_rack = {}
    for diskId in disk_to_read_from:
        rackId = mlec.state.disks[diskId].rackId
        if mlec.sys.diskIO <= mlec.state.network.intra_rack_avail[rackId]:
            # We have more bandwidth than disk upload
            intra_rack[rackId] = mlec.sys.diskIO
            mlec.state.network.intra_rack_avail[rackId] -= mlec.sys.diskIO
            # logging.info("Using %s net bandwidth from rack %s", self.sys.diskIO, rackId)
        else:
            # We have less bandwidth than the diskIO, we then take all
            intra_rack[rackId] = mlec.state.network.intra_rack_avail[rackId]
            mlec.state.network.intra_rack_avail[rackId] = 0
            # logging.info("Using %s net bandwidth from rack %s", self.state.network.intra_rack_avail[rackId], rackId)
    
    # If the total upload from racks are larger than the avail inter-rack, we use all
    # TODO: if this is the case, we also need to adjust intra-rack bandwidth of each rack
    total_upload_bandwidth = np.sum(list(intra_rack.values()))
    if total_upload_bandwidth > mlec.state.network.inter_rack_avail:
        inter_rack = mlec.state.network.inter_rack_avail
        mlec.state.network.inter_rack_avail = 0
    else:
        inter_rack = total_upload_bandwidth
        mlec.state.network.inter_rack_avail -= total_upload_bandwidth
    
    return NetworkUsage(inter_rack, intra_rack)

def initial_repair_diskgroup(diskgroups_to_read_from: List[int], mlec: MLEC):
    intra_rack = {}
    for diskgroupId in diskgroups_to_read_from:
        rackId = mlec.diskgroups[diskgroupId].rackId
        if (mlec.sys.diskIO * mlec.sys.n) <= mlec.state.network.intra_rack_avail[rackId]:
            intra_rack[rackId] = mlec.sys.diskIO * mlec.sys.n
            mlec.state.network.intra_rack_avail[rackId] -= mlec.sys.diskIO * mlec.sys.n
        else:
            intra_rack[rackId] = mlec.state.network.intra_rack_avail[rackId]
            mlec.state.network.intra_rack_avail[rackId] = 0
            
    # If the total upload from racks are larger than the avail inter-rack, we use all
    # TODO: if this is the case, we also need to adjust intra-rack bandwidth of each rack
    total_upload_bandwidth = np.sum(list(intra_rack.values()))
    if total_upload_bandwidth > mlec.state.network.inter_rack_avail:
        inter_rack = mlec.state.network.inter_rack_avail
        mlec.state.network.inter_rack_avail = 0
    else:
        inter_rack = total_upload_bandwidth
        mlec.state.network.inter_rack_avail -= total_upload_bandwidth
    
    return NetworkUsage(inter_rack, intra_rack)
        
def update_network_state(disk: Disk, fail_per_diskgroup: List[int], mlec: MLEC) -> bool:
    # The disk being passed in MUST be just failed disk
    rackId = disk.rackId
    num_fail_per_diskgroup = len(fail_per_diskgroup)
    if len(fail_per_diskgroup) == 1:
        logging.info("First failure in disk group, initial repair")
        # If there is only one failure in the disk group, we need to check whether there
        #   is enough bandwidth and surviving chunks to carry out the repair
        disk_to_read = disks_to_read_for_repair(disk, mlec)
        if mlec.state.network.inter_rack_avail != 0 \
            and len(disk_to_read) >= mlec.state.sys.k:
                disk.network_usage = initial_repair(disk, disk_to_read, mlec)
        else:
            # This means that we do not have enough bandwidth to start the repair
            mlec.state.simulation.delay_repair_queue[Components.DISK].append(disk.diskId)
            logging.warn("Disk %s repair is being delayed due to insufficient sibling or bandwidth", disk.diskId)
            return False
        
    elif len(fail_per_diskgroup) <= mlec.sys.m:
        # If there is no Diskgroup failure, we use intra-rack bandwidth for repair
        #   We also append the network usage to the disk 
        # We remove network usage from all previously repairing disks
        #    And then update the new network usage
        # This step does not require additional bandwidth, as we are only redistributing
        #    bandwidth being used by previously repairing disks
        logging.info("Multiple failures in disk group: %s", str(fail_per_diskgroup))
        usage_aggregator = NetworkUsage(0, {})
        for diskId in fail_per_diskgroup:
            usage_aggregator.join(mlec.state.disks[diskId].network_usage)
            logging.info("disk %s has intra-rack usage of %s", diskId, mlec.state.disks[diskId].network_usage)
        
        # Note that despite that we do not use new bandwidth, it is possible that there are siblings
        #    in the disk group that are in delay repair queue. Repair should not happen in this case
        if (usage_aggregator.has_intra_rack()):
            for diskId in fail_per_diskgroup:
                mlec.state.disks[diskId].network_usage = NetworkUsage(0, {rackId: usage_aggregator.intra_rack[rackId] / num_fail_per_diskgroup})
        else:
            logging.warn("Disk %s repair is being delayed due to insufficient sibling or bandwidth", disk.diskId)
            mlec.state.simulation.delay_repair_queue[Components.DISK].append(disk.diskId)
            return False
            
    elif len(fail_per_diskgroup) > mlec.sys.m:
        logging.info("More than bottom_m failures, resort to diskgroup repair")
        # This means that the local diskgroup has failed. We need to repair the disk group
        #  this is going to be handled in the diskgroup network state update
        # But we need to return false so that we do not assign repair time for the disk
        return False
    
    return True

def update_network_state_diskgroup(diskgroup: Diskgroup, fail_per_stripeset: List[int], mlec: MLEC) -> bool:
    rackId = diskgroup.rackId
    num_fail_per_stripeset = len(fail_per_stripeset)
    if num_fail_per_stripeset == 1:
        # If there is only one failure in the disk group, we need to check whether there
        #   is enough bandwidth and surviving chunks to carry out the repair
        diskgroups_to_read = diskgroup_to_read_for_repair(diskgroup.diskgroupStripesetId, mlec)
        if mlec.state.network.inter_rack_avail != 0 \
            and len(diskgroups_to_read) >= mlec.state.sys.top_k:
                diskgroup.network_usage = initial_repair_diskgroup(diskgroups_to_read, mlec)
        else:
            logging.warn("Diskgroup %s repair is being delayed due to insufficient sibling or bandwidth", diskgroup.diskgroupId)
            mlec.state.simulation.delay_repair_queue[Components.DISKGROUP].append(diskgroup.diskgroupId)
            return False
    
    elif len(fail_per_stripeset) <= mlec.sys.m:
        # This means that we have diskgroup stripeset failures, we need cross-rack repair
        #    We append the network usage to the disk groups under going repair
        # We split the previously used bandwidth, similar to the disk repair
        usage_aggregator = NetworkUsage(0, {})
        for diskgroupId in fail_per_stripeset:
            usage_aggregator.join(mlec.diskgroups[diskgroupId].network_usage)
        
        if (usage_aggregator.has_intra_rack()):
            for diskgroupId in fail_per_stripeset:
                mlec.diskgroups[diskgroupId].network_usage = usage_aggregator.split(num_fail_per_stripeset)
        else:
            logging.warn("Diskgroup %s repair is being delayed due to insufficient sibling or bandwidth", diskgroup.diskgroupId)
            mlec.state.simulation.delay_repair_queue[Components.DISKGROUP].append(diskgroup.diskgroupId)
            return False
            
    elif len(fail_per_stripeset) > mlec.sys.m:
        # Do nothing. Check PDL will determine system fail
        return False
    
    return True