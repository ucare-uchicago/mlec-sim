from inspect import trace
from multiprocessing.pool import ThreadPool
from placement import Placement
from trinity import Trinity
from repair import Repair
from state import State
from disk import Disk
from heapq import *
import logging
import time
import sys
from constants import debug, YEAR
import numpy as np
from repair import dp_gen_new_failures
#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    def __init__(self, mission_time, iterations_per_worker, traces_per_worker, 
                num_disks, num_disks_per_server, k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio,
                top_k = 1, top_m = 0):
        self.mission_time = mission_time
        self.use_trace = use_trace
        self.disk_fail_distr = 0 #add it later
        self.traceDir = traceDir
        #---------------------------------------
        self.sys = Trinity(num_disks, num_disks_per_server, k, m, place_type, diskCap, rebuildRate, utilizeRatio, top_k, top_m)
        self.repair = Repair(self.sys, place_type)
        self.placement = Placement(self.sys, place_type)
        #---------------------------------------
        self.num_disks = num_disks
        self.failRatio = failRatio


    #------------------------------------------
    #------------------------------------------
    def reset(self, sysstate):
        self.events_queue = []
        self.repair_queue = []
        self.state = State(self.sys)
        #-----------------------------------------------------
        # initialize the stripesets inside each disk
        #-----------------------------------------------------
        for diskId in range(self.sys.num_disks):
            self.state.disks[diskId].percent[self.state.initial_priority] = 1.0 #100% stripes are good
            self.state.disks[diskId].repair_data = self.sys.diskSize * self.sys.utilizeRatio
        #-----------------------------------------------------
        # initialize the stripesets inside each server
        #-----------------------------------------------------
        for serverId in self.sys.servers:
            self.state.servers[serverId].percent[self.state.initial_priority] = 1.0 #100% stripes are good
            self.state.servers[serverId].repair_data = self.sys.diskSize * self.sys.utilizeRatio * self.sys.num_disks_per_server
        
        failure_times = sysstate.drives[sysstate.drives < YEAR]
        failure_idxs = np.where(sysstate.drives < YEAR)[0]
        failures = list(zip(failure_times, failure_idxs))
        #-----------------------------------------------------
        # generate disks failures events from failure traces
        #-----------------------------------------------------
        # if self.use_trace:
        for disk_fail_time, diskId in failures:
            heappush(self.events_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
            logging.info("    >>>>> reset {} {}".format(diskId, disk_fail_time))
        #-----------------------------------------------------
        self.sys.priority_per_set = {}
        # if self.sys.place_type == 2:
        #     for serverId in self.sys.servers:
        #         for stripeset in self.sys.flat_stripeset_server_layout[serverId]:
        #             self.sys.priority_per_set[tuple(stripeset)] = 0
        # if self.sys.place_type == 3:
        #     for serverId in self.sys.servers:
        #         for stripeset in self.sys.flat_draid_server_layout[serverId]:
        #             self.sys.priority_per_set[tuple(stripeset)] = 0




    def get_next_event(self):
        if self.events_queue or self.repair_queue:
            if len(self.repair_queue) == 0:
                next_event = heappop(self.events_queue)
            elif len(self.events_queue) == 0:
                next_event = heappop(self.repair_queue)
            else:
                first_event_time = self.events_queue[0][0]
                first_repair_time = self.repair_queue[0][0]
                if first_event_time < first_repair_time:
                    next_event = heappop(self.events_queue)
                else:
                    next_event = heappop(self.repair_queue)
            return next_event
        return None
        



    def get_next_eventset(self, curr_time):
        diskset = []
        if self.events_queue or self.repair_queue:
            next_event = self.get_next_event()
            #--------------------------------------
            next_event_time = next_event[0]
            next_event_type = next_event[1]
            diskset.append(next_event[2])
            #--------------------------------------------------------------
            # gather the events with the same occurring time and event type
            #--------------------------------------------------------------
            if next_event[1] == Disk.EVENT_FAIL:
                while self.events_queue and self.events_queue[0][0] == next_event_time and self.events_queue[0][1] == next_event_type:
                    simultaneous_event = heappop(self.events_queue)
                    diskset.append(simultaneous_event[2])
            else:
                while self.repair_queue and self.repair_queue[0][0] == next_event_time and self.repair_queue[0][1] == next_event_type:
                    simultaneous_event = heappop(self.repair_queue)
                    diskset.append(simultaneous_event[2])
            logging.debug("++++++++++ pop ", next_event_time, next_event_type, diskset, "curr-time", curr_time)
            return (next_event_time, next_event_type, diskset)
        else:
            #print " -None, None, None- "
            return (None, None, None)



    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, sysstate):
        logging.debug(" * begin running simulation")
        self.reset(sysstate)
        curr_time = 0
        prob = 0
        loss_events = 0
        while True:
            #---------------------------
            # extract the next event
            #---------------------------
            (event_time, event_type, diskset) = self.get_next_eventset(curr_time)
            logging.info("----record----")
            logging.info(event_time)
            logging.info(event_type)
            logging.info(diskset)
            if event_time == None:
                break
            rerun = False
            for diskId in diskset:
                logging.debug(str(diskId) + " disk ID" + ", diskset len: " + str(len(diskset)))
                logging.debug(str(event_type))
                logging.debug(self.state.disks)
                if event_type == Disk.EVENT_FAIL and self.state.disks[diskId].state == Disk.STATE_FAILED:
                    logging.info("XXXXXXXXXXXX Disk {} failed again but how can this happen??".format(diskId))
                    rerun = True
                    break
            if rerun is True:
                continue
            #--------------------------------------
            # update all disks clock/state/priority
            #--------------------------------------
            curr_time = event_time
            self.state.update_clock(curr_time)
            self.state.update_state(event_type, diskset)
            new_server_failures = self.state.update_server_state(event_type, diskset)
            self.state.update_priority(event_type, diskset)
            if len(new_server_failures) > 0:
                self.state.update_server_priority(event_type, new_server_failures, diskset)
            #---------------------------
            # exceed mission-time, exit
            #---------------------------
            if curr_time > self.mission_time:
                break
            #---------------------------
            # repair event, continue
            #---------------------------
            if event_type == Disk.EVENT_REPAIR:
                # print("EVENT_REPAIR")
                #self.generate_fail_event(diskset, curr_time)
                for disk in diskset:
                    new_failure_added = dp_gen_new_failures({}, diskset, curr_time, sysstate)
                    # print(new_failure_added)
                    for disk_fail_time, diskId in new_failure_added:
                        heappush(self.events_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
                        logging.info("    >>>>> reset {} {}".format(diskId, disk_fail_time))
                        # print(self.events_queue)
                    logging.info(">>REPAIR " + str(disk))

            if event_type == Disk.EVENT_FASTREBUILD:
                for disk in diskset:
                    logging.info(">>FASTER_REBUILD " + str(disk))
                #self.generate_fail_event(diskset, curr_time)
            #---------------------------
            # failure event, check PDL
            #---------------------------
            if event_type == Disk.EVENT_FAIL:
                #curr_failures = self.state.get_failed_disks()
                if self.placement.check_data_loss_prob(self.state):
                    prob = 1
                    logging.info("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    # print("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    # loss_events = self.placement.check_data_loss_events(self.state)
                    return prob
                    #------------------------------------------
                else:
                    prob = 0
                    #print "  >>>>>>> no data loss >>>>>>>  ", curr_failures
                
                    #------------------------------------------
            self.repair.update_repair_event(diskset, self.state, curr_time, self.repair_queue)
        return prob





    #----------------------------
    # run statistical model
    #----------------------------
    def run_iteration(self, one_iter):
        #logger.info("* begin an iteration %d with params %d*"% (one_iter, params))
        curr_time = 0
        while True:
            (event_time, diskId) = self.get_next_event(curr_time)
            curr_time = event_time
            if event_time > self.mission_time:
                break
        return (0, 0, 0)


    #----------------------------
    # display the results
    #----------------------------
    def display_results(self, final_results):
        #-----------------------------------------
        # collect the reliability metrics 
        #-----------------------------------------
        for each in final_results:
            (x, y, z) = each
            logging.debug(x, y, z)
        


if __name__ == "__main__":
    #--------------------------------------------
    total_iterations = 40
    num_threads = 1
    if total_iterations % num_threads != 0:
        logging.debug("total iterations should be divided by number of threads")
        sys.exit(2)
    iterations_per_worker = [total_iterations / num_threads] * num_threads
    #--------------------------------------------
    logging.debug(">>>>> start:", time.time())
    sim = Simulate(10, 6, 1, 9, 2,1,2,1,2,1)
    pool = ThreadPool(num_threads)
    results = pool.map(sim.run_simulation, iterations_per_worker)
    pool.close()
    pool.join()
    #--------------------------------------------
    final_results = []
    for one in results:
        final_results += one
    logging.debug(">>>>>>>>>>>>>>>> final results", final_results)
    sim.display_results(final_results)
    logging.debug(">>>>> end:", time.time())
    #--------------------------------------------
    

