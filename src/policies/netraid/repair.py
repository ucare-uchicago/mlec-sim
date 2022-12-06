from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from state import State

from constants.Components import Components
from components.disk import Disk
from components.rack import Rack

from heapq import heappush

def netraid_repair(state: State, repair_queue):
    for diskId in state.get_failed_disks():
        #logging.info("Trying to update repair for disk %s", diskId)
        rackId = diskId // state.sys.num_disks_per_rack
        # If the rack is normal AND the disk is not awaiting repair
        if state.racks[rackId].state == Rack.STATE_NORMAL and (diskId not in state.simulation.delay_repair_queue[Components.DISK]):
            #logging.info("Disk has healthy parent rack, and is not awaiting repair")
            # logging.info("  update_repair_event. diskId: {}".format(diskId))
            repair_time = state.disks[diskId].repair_time[0]
            #-----------------------------------------------------
            estimate_time = state.disks[diskId].repair_start_time
            estimate_time  += repair_time
            # Generate repair event
            heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))