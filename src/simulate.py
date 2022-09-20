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
    def reset(self, initialFailures, mytimer):
        self.failure_queue = []
        self.repair_queue = []

        self.sys.priority_per_set = {}

        temp = time.time()
        self.state = State(self.sys, mytimer)
        resetStateEndTime = time.time()
        mytimer.resetStateInitTime += resetStateEndTime - temp

        temp = time.time()
        failure_times = initialFailures[initialFailures < self.mission_time]
        failure_idxs = np.where(initialFailures < self.mission_time)[0]
        failures = list(zip(failure_times, failure_idxs))
        resetGenFailEndTime = time.time()
        mytimer.resetGenFailTime += resetGenFailEndTime - temp
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
            # logging.info("    >>>>> reset {} {} {}".format(disk_fail_time, Disk.EVENT_FAIL, diskId))
            self.sys.metrics.failure_count += 1
            
        
        if self.sys.rack_fail > 0:
            for diskId in range(self.sys.rack_fail):
                disk_fail_time = random.random() * YEAR
                heappush(self.failure_queue, (disk_fail_time, Rack.EVENT_FAIL, diskId))
                # logging.info("    >>>>> reset {} {} {}".format(disk_fail_time, Rack.EVENT_FAIL, diskId))


        heapEndTime = time.time()
        mytimer.resetHeapTime += heapEndTime - resetGenFailEndTime
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
        return None
        



    def get_next_eventset(self, curr_time):
        diskset = []
        if self.failure_queue or self.repair_queue:
            next_event = self.get_next_event()
            #--------------------------------------
            next_event_time = next_event[0]
            next_event_type = next_event[1]
            diskset.append(next_event[2])
            #--------------------------------------------------------------
            # gather the events with the same occurring time and event type
            #--------------------------------------------------------------
            if next_event[1] == Disk.EVENT_FAIL:
                while self.failure_queue and self.failure_queue[0][0] == next_event_time and self.failure_queue[0][1] == next_event_type:
                    simultaneous_event = heappop(self.failure_queue)
                    diskset.append(simultaneous_event[2])
            else:
                while self.repair_queue and self.repair_queue[0][0] == next_event_time and self.repair_queue[0][1] == next_event_type:
                    simultaneous_event = heappop(self.repair_queue)
                    diskset.append(simultaneous_event[2])
            return (next_event_time, next_event_type, diskset)
        else:
            #print " -None, None, None- "
            return (None, None, None)



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, sysstate, mytimer):
        self.sys.metrics.iter_count += 1

        self.mytimer = mytimer
        start = time.time()
        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        seedEndTime = time.time()
        
        initialFailures = sysstate.gen_failure_times(sysstate.total_drives)
        logging.info("")
        logging.info("---------")
        genEndTime = time.time()
        mytimer.seedtime += seedEndTime - start
        mytimer.genfailtime += genEndTime - seedEndTime

        start = time.time()
        self.reset(initialFailures, mytimer)
        mytimer.resettime += (time.time() - start)

        curr_time = 0
        prob = 0
        loss_events = 0
        while True:
            iterStartTime = time.time()
            #---------------------------
            # extract the next event
            #---------------------------
            (event_time, event_type, diskset) = self.get_next_eventset(curr_time)
            getEventEndTime = time.time()
            mytimer.getEventTime += getEventEndTime - iterStartTime

            logging.info("----record----  {} {} {}".format(event_time, event_type, diskset))
            
            if event_time == None:
                break

            # for diskId in diskset:
            #     if event_type == Disk.EVENT_FAIL and self.state.disks[diskId].state == Disk.STATE_FAILED:
            #         logging.info("XXXXXXXXXXXX Disk {} failed again ok it happened".format(diskId))
            #--------------------------------------
            # update all disks state/priority
            #--------------------------------------
            curr_time = event_time
            self.state.update_curr_time(curr_time)

            self.state.update_state(event_type, diskset)
            updateStateEndTime = time.time()
            mytimer.updateStateTime += updateStateEndTime - getEventEndTime


            self.state.update_priority(event_type, diskset)
            updatePriorityEndTime = time.time()
            mytimer.updatePriorityTime += updatePriorityEndTime - updateStateEndTime

            if self.sys.place_type in [2,4]:
                new_rack_failures = self.state.update_rack_state(event_type, diskset)
                # updateRackStateEndTime = time.time()
                # mytimer.updateRackStateTime += updateRackStateEndTime - updatePriorityEndTime



            if self.sys.place_type in [2,4]:
                if len(new_rack_failures) > 0:
                    self.state.update_rack_priority(event_type, new_rack_failures, diskset)
                    # updateRackPriorityEndTime = time.time()
                    # mytimer.updateRackPriorityTime += updateRackPriorityEndTime - updateRackStateEndTime

            #---------------------------
            # exceed mission-time, exit
            #---------------------------
            if curr_time > self.mission_time:
                break
            #---------------------------
            # repair event, continue
            #---------------------------
            if event_type == Disk.EVENT_FAIL:
                # print("EVENT_REPAIR")
                #self.generate_fail_event(diskset, curr_time)
                new_failure_intervals = sysstate.dp_gen_new_failures(len(diskset))
                # print(new_failure_added)
                for i in range(len(diskset)):
                    disk_fail_time = new_failure_intervals[i] + curr_time
                    if disk_fail_time < self.mission_time:
                        heappush(self.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, diskset[i]))
                        # logging.info("    >>>>> reset {} {}".format(diskId, disk_fail_time))
                        continue
                    # print(self.failure_queue)
            # newFailEndTime = time.time()
            # mytimer.newFailTime += newFailEndTime - updateRackPriorityEndTime


            # if event_type == Disk.EVENT_FASTREBUILD:
            #     for disk in diskset:
            #         logging.info(">>FASTER_REBUILD " + str(disk))
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
            # checkLossEndTime = time.time()
            # mytimer.checkLossTime += checkLossEndTime - newFailEndTime

            self.repair.update_repair_event(diskset, self.state, curr_time, self.repair_queue)
            # updateRepairEndTime = time.time()
            # mytimer.updateRepairTime += updateRepairEndTime - checkLossEndTime
        return prob


    

