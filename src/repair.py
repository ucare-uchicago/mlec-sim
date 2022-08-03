from asyncore import write
from sys_state import SysState
from constants import debug, YEAR

import heapq
import math
import numpy as np
from collections import OrderedDict
from trinity import Trinity
from disk import Disk
import operator as op
from heapq import *
import logging
from server import Server

class Repair:
    def __init__(self, sys, place_type):
        #---------------------------------------
        # Initialize Trinity Storage System
        #---------------------------------------
        self.sys = sys
        self.place_type = place_type
        #---------------------------------------



    
    def generate_repair_event(self, diskset, state, curr_time, events_queue):
        logging.debug("generate repair",diskset)
        for diskId in diskset:
            if self.place_type == 1 or self.place_type == 2 or self.place_type == 3:
                #-----------------------------------------------------
                # priority reconstruct, p is larger, schedule it faster
                #------------------------------------------------------
                sorted_time = sorted(state.disks[diskId].repair_time.items(),reverse=True,key=lambda x: x[0])
                logging.debug("sorted", sorted_time)
                estimate_time = curr_time
                for (priority, repair_time) in sorted_time:
                    estimate_time  += repair_time
                    if priority > 1:
                        heappush(events_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                        # logging.info(priority,"--------> push ", repair_time, estimate_time, Disk.EVENT_FASTREBUILD, "D-",diskId,"-", "S-", diskId/84, "R-",diskId/504)
                        logging.info("--->repair_time" + "(" + str(diskId) + ")" + ":" + str(repair_time) +  " estimated_time: " + str(estimate_time) + " type: FASTERBUILD")
                    if priority == 1:
                        heappush(events_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                        # logging.info(priority,"--------> push ", repair_time, estimate_time, DiskEVENT_REPAIR, "D-",diskId,"-", "S-",diskId/84, "R-",diskId/504)
                        logging.info("--->repair_time" + "(" + str(diskId) + ")" + ":" + str(repair_time) +  " estimated_time: " + str(estimate_time) + " type: REPAIR")
                    #print "diskId",diskId, "> priority", priority, "repair", repair_time, "> ",estimate_time, "curr-time", curr_time
                    #print "    >>>> priority", priority
                    #------------------------------
                    # remove from the time dict
                    #------------------------------
                    del state.disks[diskId].repair_time[priority]
            if self.place_type == 0:
                #-----------------------------------------------------
                # FIFO reconstruct, utilize the hot spares
                #-----------------------------------------------------
                repair_time = state.disks[diskId].repair_time[0]
                #-----------------------------------------------------
                estimate_time = curr_time
                estimate_time  += repair_time
                heappush(events_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                logging.debug("--------> push ", repair_time, estimate_time, Disk.EVENT_REPAIR, "D-",diskId,"-", "S-",diskId/84, "R-",diskId/504)


    def update_repair_event(self, diskset, state, curr_time, repair_queue):
        logging.debug("updating repair",diskset)
        repair_queue.clear()
        checked_servers = {}
        for serverId in state.get_failed_servers():
            if self.place_type == 2:
                repair_time = state.servers[serverId].repair_time[0]
                #-----------------------------------------------------
                heappush(repair_queue, (state.servers[serverId].estimate_repair_time, Server.EVENT_REPAIR, serverId))
        for diskId in state.get_failed_disks():
            serverId = diskId // self.sys.num_disks_per_server
            if state.servers[serverId].state == Server.STATE_NORMAL:
                if self.place_type == 0:
                    #-----------------------------------------------------
                    # FIFO reconstruct, utilize the hot spares
                    #-----------------------------------------------------
                    repair_time = state.disks[diskId].repair_time[0]
                    #-----------------------------------------------------
                    estimate_time = state.disks[diskId].repair_start_time
                    estimate_time  += repair_time
                    heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))
                    logging.debug("--------> push ", repair_time, estimate_time, Disk.EVENT_REPAIR, 
                                "D-",diskId,"-", "S-",diskId/84, "R-",diskId/504)
                if self.place_type == 1:
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
                if self.place_type == 2:
                    #-----------------------------------------------------
                    # FIFO reconstruct, utilize the hot spares
                    #-----------------------------------------------------
                    heappush(repair_queue, (state.disks[diskId].estimate_repair_time, Disk.EVENT_REPAIR, diskId))