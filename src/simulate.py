from inspect import trace
from multiprocessing.pool import ThreadPool
from placement import Placement
from repair import Repair
from state import State
from disk import Disk
from rack import Rack
from heapq import *
import logging
import time
import sys
from constants import debug, YEAR
import numpy as np
import time
import os
from mytimer import Mytimer
import random
#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    def __init__(self, mission_time, num_disks, sys = None, repair = None, placement = None):
        self.mission_time = mission_time
        #---------------------------------------
        self.sys = sys
        self.repair = repair
        self.placement = placement
        #---------------------------------------
        self.num_disks = num_disks


    #------------------------------------------
    # initiate failure queue and repair queue
    # failure queue: the queue of failure events (including rack failure and disk failure)
    # repair queue: the queue of repair events (including rack repair and disk repair)
    # element in the queue follows this format: (event_time, event_type, disk/rack ID)
    #------------------------------------------
    def reset(self, failureGenerator, mytimer):
        self.failure_queue = []
        self.repair_queue = []

        self.sys.priority_per_set = {}

        self.state = State(self.sys, mytimer)

        if failureGenerator.is_burst:
            failures = failureGenerator.gen_failure_burst(self.sys.num_disks_per_rack, self.sys.num_racks)
            # failures = failureGenerator.gen_failure_burst(50, 50)
        else:
            initialFailures = failureGenerator.gen_failure_times(self.sys.num_disks)
            failure_times = initialFailures[initialFailures < self.mission_time]
            failure_idxs = np.where(initialFailures < self.mission_time)[0]
            failures = list(zip(failure_times, failure_idxs))
        #-----------------------------------------------------
        # generate disks failures events from failure traces
        #-----------------------------------------------------
        # if self.use_trace:
        # heappush(self.failure_queue, (0, Disk.EVENT_FAIL, 0))
        # heappush(self.failure_queue, (0.001, Disk.EVENT_FAIL, 1))
        # return

        # logging.info("initialFailures: {}".format(len(initialFailures)))


        for disk_fail_time, diskId in failures:
            heappush(self.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
            logging.info("    >>>>> reset {} {} {}".format(disk_fail_time, Disk.EVENT_FAIL, diskId))
            self.sys.metrics.failure_count += 1
            
        
        if self.sys.rack_fail > 0:
            for diskId in range(self.sys.rack_fail):
                disk_fail_time = random.random() * YEAR
                heappush(self.failure_queue, (disk_fail_time, Rack.EVENT_FAIL, diskId))
                # logging.info("    >>>>> reset {} {} {}".format(disk_fail_time, Rack.EVENT_FAIL, diskId))


        #-----------------------------------------------------


    #------------------------------------------
    
    #------------------------------------------
    def get_next_event(self):
        if self.failure_queue or self.repair_queue:
            if len(self.repair_queue) == 0:
                next_event = heappop(self.failure_queue)
            elif len(self.failure_queue) == 0:
                next_event = heappop(self.repair_queue)
            else:
                first_event_time = self.failure_queue[0][0]
                first_repair_time = self.repair_queue[0][0]
                if first_event_time < first_repair_time:
                    next_event = heappop(self.failure_queue)
                else:
                    next_event = heappop(self.repair_queue)
            return next_event
        return (None, None, None)
        



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, failureGenerator, mytimer):
        logging.info("---------")

        self.sys.metrics.iter_count += 1
        self.mytimer = mytimer

        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        
        
        self.reset(failureGenerator, mytimer)

        curr_time = 0
        prob = 0
        loss_events = 0
        while True:
            #---------------------------
            # extract the next event
            #---------------------------
            (event_time, event_type, diskId) = self.get_next_event()
            getEventEndTime = time.time()

            # logging.info("----record----  {} {} {}".format(event_time, event_type, diskId))
            
            if event_time == None:
                break

            #--------------------------------------
            # update all disks state/priority
            #--------------------------------------
            curr_time = event_time
            self.state.update_curr_time(curr_time)

            self.state.update_disk_state(event_type, diskId)

            self.state.update_disk_priority(event_type, diskId)

            #--------------------------------------
            # In MLEC-RAID, each disk group contains n=k+m disks. A rack can have multiple diskgroups
            # If this disk group has m+1 or more disk failures, then we need to repair the 
            # disk group using network erasure.
            # Note that other disk groups in this rack can be healthy and don't need repair
            #--------------------------------------
            if self.sys.place_type in [2]:
                new_diskgroup_failure = self.state.update_diskgroup_state(event_type, diskId)
                if new_diskgroup_failure != None:
                    self.state.update_diskgroup_priority(event_type, new_diskgroup_failure, diskId)

            #--------------------------------------
            # In MLEC-DP, a rack can have more disks
            # If the rack has m+1 or more disk failures, then we need to repair the rack
            #--------------------------------------
            if self.sys.place_type in [4]:
                new_rack_failure = self.state.update_rack_state(event_type, diskId)
                if new_rack_failure != None:
                    self.state.update_rack_priority(event_type, new_rack_failure, diskId)

            #---------------------------
            # exceed mission-time, exit
            #---------------------------
            if curr_time > self.mission_time:
                break
            #---------------------------
            # new failure should be generated
            #---------------------------
            if event_type == Disk.EVENT_FAIL and not failureGenerator.is_burst:
                new_failure_intervals = failureGenerator.gen_new_failures(1)
                disk_fail_time = new_failure_intervals[0] + curr_time
                if disk_fail_time < self.mission_time:
                    heappush(self.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
                    # logging.info("    >>>>> reset {} {}".format(diskId, disk_fail_time))


            #---------------------------
            # failure event, check PDL
            #---------------------------
            if event_type == Disk.EVENT_FAIL or event_type == Rack.EVENT_FAIL:
                #curr_failures = self.state.get_failed_disks()
                if self.placement.check_data_loss_prob(self.state):
                    prob = 1
                    logging.info("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    # loss_events = self.placement.check_data_loss_events(self.state)
                    return prob
                    #------------------------------------------
                else:
                    prob = 0
                    #print "  >>>>>>> no data loss >>>>>>>  ", curr_failures
                
                    #------------------------------------------

            self.repair.update_repair_event(self.state, curr_time, self.repair_queue)
        return prob


    

