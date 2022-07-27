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



# this is repairing each definitive stripe
def calc_raid_repair_time(stripe_failed_count, state: SysState):
    # disks_to_read = state.drive_args.total_shards - stripe_failed_count
    # disks_to_write = stripe_failed_count

    # read_time = disks_to_read * state.drive_args.drive_cap * 1024 * 1024 / state.drive_args.rec_speed
    # write_time = disks_to_write * state.drive_args.drive_cap * 1024 * 1024 / state.drive_args.rec_speed

    write_time = state.drive_args.drive_cap * 1024 * 1024 / state.drive_args.rec_speed

    return write_time / 3600 / 24

# this is repair all the stripes that have this failure count
def calc_dp_repair_time(stripe_failed_count, state: SysState):
    # Stripes that contain parity_shards - 1 failures
    #  the most dangerous critical path, should be given priority
    #state.print(["Stripe failed: " + str(stripe_failed_count)])
    
    if debug: print("c({}, {}) * c({} {})".format(state.good_cnt, state.drive_args.total_shards - stripe_failed_count, state.fail_cnt, stripe_failed_count), flush=True)
    # selecting stripe good ones from the remaining good ones
    prio_stripes = math.comb(state.good_cnt, state.drive_args.total_shards - stripe_failed_count) * math.comb(state.fail_cnt, stripe_failed_count)
    total_stripes = math.comb(state.good_cnt + state.fail_cnt, state.drive_args.total_shards)
    prio_percent = float(prio_stripes) / total_stripes
    if debug: print("Priority stats - prio_stripes: {} total_stripes: {} prio: {} prio_percent: {:.6f}".format(prio_stripes, total_stripes, stripe_failed_count, prio_percent), flush=True)

    # how many disks that we can read/write from at the same time
    parallelism = state.good_cnt
    
    # how many disks worth of DATA do we need to read 
    # (well, all the good data shards worth of data would be needed for reconstruction)
    # if (debug): state.drive_args.print()
    disks_read = state.drive_args.data_shards
    disks_write = stripe_failed_count
    
    # total "amplification (term adopted from SOLSim)", because in DP, read and write occur sequentially
    amplification = disks_read + disks_write

    # WARNING: assuming that all disk data is subject to repair (no repair_data in SOLSim)
    repair_time = state.drive_args.drive_cap * 1024 * 1024 * amplification / (state.drive_args.rec_speed * parallelism)
    if debug: print("Repair stats - parallelism: {} amplification: {} calculated_time(hr): {}".format(parallelism, amplification, repair_time/3600), flush=True)
    return repair_time / 3600 / 24

def calc_dp_repair_time_serkay(stripe_failed_count, state: SysState):
    # Stripes that contain parity_shards - 1 failures
    #  the most dangerous critical path, should be given priority
    #state.print(["Stripe failed: " + str(stripe_failed_count)])

    mu = state.dirve_args.drive_cap * 1024 * 1024 / state.drive_args.rec_speed
    
    num  = 1
    for i in range(1, stripe_failed_count):
        num *= state.drive_args.total_shards - i
    num  *= (state.drive_args.total_shards - stripe_failed_count + 1)

    denom = 1
    for i in range(1, stripe_failed_count + 1):
        denom *= state.total_drives - i

    estimated_hours = mu * (num / denom) / 3600

    if debug: print("mu: <{}>, num: <{}>, denom: <{}>".format(mu, num, denom))
    if debug: print("Repair stripe with {} damage,  estimated time {}hr".format(stripe_failed_count, estimated_hours))

    return estimated_hours / 24