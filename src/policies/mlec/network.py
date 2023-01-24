from __future__ import annotations
from typing import List, Dict, Union
import logging
import numpy as np
import typing
if typing.TYPE_CHECKING:
    from policies.mlec.mlec import MLEC

from constants.Components import Components
from constants.RepairResult import RepairResult
from components.disk import Disk
from components.rack import Rack
from components.diskgroup import Diskgroup
from components.network import NetworkUsage

def used_for_repair_top_level(mlec: MLEC, disk: Disk):
    # TODO: Optimize this
    # We delay all bottom layer repairs that compete resources with repairing top layer stripes
    #  -> if the rack of this failing disk is being used for repairing a top layer diskgroup or
    #     is needed for a diskgroup in delay queue, we do not start the repair
    stripeset_to_check = []
    stripeset_to_check += mlec.repairing_stripeset
    for delayedDiskgroupId in mlec.state.simulation.delay_repair_queue[Components.DISKGROUP]:
        stripeset_to_check += [mlec.diskgroups[delayedDiskgroupId].diskgroupStripesetId]
    
    logging.info("Stripeset to check %s", stripeset_to_check)
    logging.info("Stripesets %s", mlec.sys.diskgroup_stripesets)
    for stripesetId in stripeset_to_check:
        repairing_rack = []
        for diskgroupId_ in mlec.sys.diskgroup_stripesets[stripesetId]:
            repairing_rack.append(mlec.diskgroups[diskgroupId_].rackId)
        
        if disk.rackId in repairing_rack:
            # This means that to repair the current bottom layer diskgroup, we need bandwidth from repairing top layer stripe
            #   we will delay the repair of this disk
            #logging.warn("This disk's rack is being used for top level repair, delaying repair")
            #logging.warn("Repairing stripeset %s", mlec.repairing_stripeset)
            return True
    
    return False

def disks_to_read_for_repair(disk: Disk, mlec: MLEC) -> List[int]:
    diskgroupId = disk.diskId // mlec.n
    disks_to_read = []
    
    for diskId in mlec.diskgroups[diskgroupId].disks:
        # logging.info("Scanning through disk %s", diskId)
        # Check whether this disk is NORMAL state
        if len(disks_to_read) >= mlec.sys.k:
            break
        
        # No longer consider inter-rack bandwidth
        if mlec.disks[diskId].state == Disk.STATE_NORMAL \
            and mlec.state.racks[disk.rackId].state == Rack.STATE_NORMAL:
            disks_to_read.append(diskId)
            
    return disks_to_read

def diskgroup_to_read_for_repair(stripesetId: int, mlec: MLEC) -> List[int]:
    logging.info("Trying to get readable sibling for stripeset %s", stripesetId)
    logging.info("Stripeset %s", mlec.sys.diskgroup_stripesets[stripesetId])
    diskgroup_to_read = []
    for diskgroupId in mlec.state.sys.diskgroup_stripesets[stripesetId]:
        if len(diskgroup_to_read) >= mlec.sys.top_k:
            break
        
        if mlec.diskgroups[diskgroupId].state == Diskgroup.STATE_NORMAL \
            and mlec.state.network.intra_rack_avail[mlec.diskgroups[diskgroupId].rackId] != 0:
                diskgroup_to_read.append(diskgroupId)
                
    return diskgroup_to_read

# If dry run is true, we will only produce the network usage, without really taking it away from the system
def initial_repair_diskgroup(diskgroups_to_read_from: List[int], mlec: MLEC, dry_run = False):
    logging.info("Initial repairing diskgroup with diskgroups to read from %s", diskgroups_to_read_from)
    intra_rack = {}
    for diskgroupId in diskgroups_to_read_from:
        # They might be of the same rack!
        rackId = mlec.diskgroups[diskgroupId].rackId
        logging.info("Diskgroup %s is on rack %s", diskgroupId, mlec.diskgroups[diskgroupId].rackId)
        if (mlec.sys.diskIO * mlec.sys.k) <= mlec.state.network.intra_rack_avail[rackId]:
            intra_rack[rackId] = intra_rack.get(rackId, 0) + mlec.sys.diskIO * mlec.sys.n
            if not dry_run:
                mlec.state.network.intra_rack_avail[rackId] -= mlec.sys.diskIO * mlec.sys.n
        else:
            intra_rack[rackId] = intra_rack.get(rackId, 0) + mlec.state.network.intra_rack_avail[rackId]
            if not dry_run:
                mlec.state.network.intra_rack_avail[rackId] = 0
    
    logging.info("Network usage: %s", intra_rack)
    # How much bw does diskgroup repair uses
    return NetworkUsage(0, intra_rack)
        
def update_network_state(disk: Disk, fail_per_diskgroup: List[int], mlec: MLEC) -> RepairResult:
    # The disk being passed in MUST be just failed disk
    rackId = disk.rackId
    diskgroupId = disk.diskId // mlec.sys.n
    num_fail_per_diskgroup = len(fail_per_diskgroup)
    
    # Get how many disks are repairing in the diskgroup 
    #  -> used to check whether this diskgroup just had its first failure, or failed, or in degraded state
    num_repairing = 0
    for diskId in fail_per_diskgroup:
        if mlec.disks[diskId].repair_start_time != 0:
            num_repairing += 1
    
    # Note: due to intra-rack repair no longer requiring bandwidth, lower-level repairs can always take place
    # If this disk's repair would require network bandwidth from an on-gonig top-layer repair, or a delayed top-layer repair, we delay its repair
    # if used_for_repair_top_level(mlec, disk):
    #     mlec.sys.metrics.total_delayed_disks += 1
    #     mlec.state.simulation.delay_repair_queue[Components.DISK][disk.diskId] = True
    #     return RepairResult(False, 0)
    
    if num_repairing == 0:
        logging.info("First failure in disk group, initial repair")
        # If there is only one failure in the disk group, we need to check whether there
        #   is enough surviving chunks to carry out the repair
        disk_to_read = disks_to_read_for_repair(disk, mlec)
        logging.info("Surviving sibling num %s (need %s)", len(disk_to_read), mlec.state.sys.k)
        if len(disk_to_read) >= mlec.state.sys.k:
            # Carry out repair
            logging.info("Beginning repair")
            return RepairResult(True, mlec.sys.diskIO)
        else:
            # This means that we do not have enough surviving subling to start the repair
            mlec.sys.metrics.total_delayed_disks += 1
            mlec.state.simulation.delay_repair_queue[Components.DISK][disk.diskId] = True
            #logging.warn("Disk %s repair is being delayed due to insufficient sibling or bandwidth", disk.diskId)
            return RepairResult(False, 0)
        
    elif len(fail_per_diskgroup) <= mlec.sys.m:
        # If there is no Diskgroup failure, we split the diskIO between the siblings
        return RepairResult(True, mlec.sys.diskIO)
            
    elif len(fail_per_diskgroup) > mlec.sys.m:
        logging.info("More than bottom_m failures, resort to diskgroup repair")
        
        for diskId_ in fail_per_diskgroup:
            logging.info("Network usage for disk %s is %s", diskId_, mlec.disks[diskId_].network_usage)
            # We give back the network bandwidth to the system
            
        # This means that the local diskgroup has failed. We need to repair the disk group
        #  this is going to be handled in the diskgroup network state update
        # But we need to return false so that we do not assign repair time for the disk
        return RepairResult(False, 0)

    else:
        raise Exception("Should not get here")

def update_network_state_diskgroup(diskgroup: Diskgroup, fail_per_stripeset: List[int], mlec: MLEC) -> bool:
    rackId = diskgroup.rackId
    num_fail_per_stripeset = len(fail_per_stripeset)
    
    num_repairing = 0
    for diskgroupId in fail_per_stripeset:
        if mlec.diskgroups[diskgroupId].repair_start_time != 0:
            logging.info("Diskgroup %s with repair_start_time of %s", diskgroupId, mlec.diskgroups[diskgroupId].repair_start_time)
            num_repairing += 1
    
    logging.info("Num repairing %s", num_repairing)
    if num_repairing == 1:
        # we start the repair if there is enough diskgroups to read from (aka enough cross-rack bandwidth)
        diskgroups_to_read = diskgroup_to_read_for_repair(diskgroup.diskgroupStripesetId, mlec)
        if len(diskgroups_to_read) >= mlec.state.sys.top_k:
                # If there is, we start the repair.
                diskgroup.network_usage = initial_repair_diskgroup(diskgroups_to_read, mlec)
                logging.info("Network after use %s", mlec.state.network)
                return True
        # If there is not enough cross-rack bandwidth, we can only delay the repair, so no need for the pause of repair
        # elif mlec.state.network.inter_rack_avail != 0:
        #     return handle_pause_bottom_layer_repair(mlec, diskgroup, diskgroups_to_read)
        else:
            #logging.warn("Delaying diskgroup %s repair", diskgroup.diskgroupId)
            mlec.state.simulation.delay_repair_queue[Components.DISKGROUP][diskgroup.diskgroupId] = True
            return False
            
    elif len(fail_per_stripeset) <= mlec.sys.m:
        # This means that we have diskgroup stripeset failures, we need cross-rack repair
        #    We append the network usage to the disk groups under going repair
        # We split the previously used bandwidth, similar to the disk repair
        usage_aggregator = NetworkUsage(0, {})
        for diskgroupId in fail_per_stripeset:
            usage_aggregator.join(mlec.diskgroups[diskgroupId].network_usage)
        
        for diskgroupId in fail_per_stripeset:
            mlec.diskgroups[diskgroupId].network_usage = usage_aggregator.split(num_fail_per_stripeset)
        
        return True
        # else:
        #     #logging.warn("Diskgroup %s repair is being delayed due to insufficient sibling or bandwidth", diskgroup.diskgroupId)
        #     mlec.state.simulation.delay_repair_queue[Components.DISKGROUP][diskgroup.diskgroupId] = True
        #     return False
            
    elif len(fail_per_stripeset) > mlec.sys.m:
        # Do nothing. Check PDL will determine system fail
        return False
    
    else:
        raise Exception("Should not get here")