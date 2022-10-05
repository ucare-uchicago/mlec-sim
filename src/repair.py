from asyncore import write
from failure_generator import FailureGenerator

import heapq
import math
import numpy as np
from collections import OrderedDict
from system import System
from disk import Disk
import operator as op
from heapq import *
import logging
from rack import Rack

class Repair:
    def __init__(self, sys, place_type):
        #---------------------------------------
        # Initialize System Storage System
        #---------------------------------------
        self.sys = sys
        self.place_type = place_type
        #---------------------------------------



    def update_repair_event(self, state, curr_time, repair_queue):
        # logging.debug("updating repair",diskset)
        repair_queue.clear()
        checked_racks = {}
        for rackId in state.get_failed_racks():
            if self.place_type in [2,4]:
                repair_time = state.racks[rackId].repair_time[0]
                #-----------------------------------------------------
                heappush(repair_queue, (state.racks[rackId].estimate_repair_time, Rack.EVENT_REPAIR, rackId))
        for diskId in state.get_failed_disks():
            rackId = diskId // self.sys.num_disks_per_rack
            if state.racks[rackId].state == Rack.STATE_NORMAL:
                if self.place_type == 0:
                    #-----------------------------------------------------
                    # FIFO reconstruct, utilize the hot spares
                    #-----------------------------------------------------
                    repair_time = state.disks[diskId].repair_time[0]
                    #-----------------------------------------------------
                    estimate_time = state.disks[diskId].repair_start_time
                    estimate_time  += repair_time
                    heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                    # logging.debug("--------> push ", repair_time, estimate_time, Disk.EVENT_REPAIR, 
                    #             "D-",diskId,"-", "S-",diskId/84, "R-",diskId/504)
                elif self.place_type == 1:
                    disk = state.disks[diskId]
                    estimate_time = disk.repair_start_time
                    priority = disk.priority
                    estimate_time  += disk.repair_time[priority]
                    if priority > 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                        # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                        #           Disk.EVENT_FASTREBUILD, diskId))
                    if priority == 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                        # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                        #           Disk.EVENT_REPAIR, diskId))
                elif self.place_type == 2:
                    #-----------------------------------------------------
                    # FIFO reconstruct, utilize the hot spares
                    #-----------------------------------------------------
                    heappush(repair_queue, (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))
                elif self.place_type == 3:
                    # logging.info("  update_repair_event. diskId: {}".format(diskId))
                    repair_time = state.disks[diskId].repair_time[0]
                    #-----------------------------------------------------
                    estimate_time = state.disks[diskId].repair_start_time
                    estimate_time  += repair_time
                    heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                elif self.place_type == 4:
                    #-----------------------------------------------------
                    # FIFO reconstruct, utilize the hot spares
                    #-----------------------------------------------------
                    disk = state.disks[diskId]
                    priority = disk.priority
                    estimate_time = disk.estimate_repair_time
                    if priority > 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                        # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                        #           Disk.EVENT_FASTREBUILD, diskId))
                    if priority == 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                elif self.place_type == 5:
                    # This should be the same as flat decluster
                    disk = state.disks[diskId]
                    estimate_time = disk.repair_start_time
                    priority = disk.priority
                    estimate_time  += disk.repair_time[priority]
                    if priority > 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                        # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                        #           Disk.EVENT_FASTREBUILD, diskId))
                    if priority == 1:
                        heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                        # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                        #           Disk.EVENT_REPAIR, diskId))
                else:
                    raise NotImplementedError("The placement type does not have a repair strategy")
        
        if len(repair_queue) > 0:
            if not state.repairing:
                state.repairing = True
                state.repair_start_time = curr_time
        else:
            if state.repairing:
                state.repairing = False
                state.sys.metrics.total_rebuild_time += curr_time - state.repair_start_time