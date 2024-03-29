from state import State
from system import System
from components.disk import Disk
from components.rack import Rack
from components.spool import Spool
from constants.PlacementType import PlacementType
from constants.Components import Components
from mytimer import Mytimer
from heapq import heappush, heappop
from typing import Dict, List

from constants.time import YEAR

import logging
import time
import numpy as np
import time
import os
import random
from pprint import pformat


from typing import Tuple, Optional
#----------------------------
# Logging Settings
#----------------------------

class Simulate:
    
    def __init__(self, mission_time, num_disks, sys: System, prev_fail_reports=None):
        self.mission_time = mission_time
        #---------------------------------------
        self.sys: System = sys
        #---------------------------------------
        self.num_disks = num_disks
        self.failure_queue = []
        self.repair_queue = []
        self.delay_repair_queue: Dict[Components, Dict[int, bool]] = {Components.DISK: {}, Components.DISKGROUP: {}}
        self.network_queue = []
        self.prev_event = None
        self.prev_fail_reports = prev_fail_reports
        self.curr_time = 0
        self.failure_generator = None

        self.log = []

    #------------------------------------------
    # initiate failure queue and repair queue
    # failure queue: the queue of failure events (including rack failure and disk failure)
    # repair queue: the queue of repair events (including rack repair and disk repair)
    # delay queue: containing stripe set ID that have been delayed due to insufficient bandwidth
    # network queue: containing network events (network bandwidth replenish for now)
    # element in the queue follows this format: (event_time, event_type, disk/rack ID)
    #------------------------------------------
    def reset(self, failureGenerator, mytimer):
        self.failure_queue = []
        self.repair_queue = []
        self.delay_repair_queue = {Components.DISK: {}, Components.DISKGROUP: {}}
        self.network_queue = []

        # self.sys.priority_per_set = {}

        start_state_reset_time = time.time()
        self.state = State(self.sys, mytimer, self)
        state_reset_done_time = time.time()
        mytimer.resetStateInitTime += (state_reset_done_time - start_state_reset_time)

        
        if self.sys.distribution == "catas_local_failure":
            for diskId in range(self.sys.m+1):
                heappush(self.failure_queue, (0.1, Disk.EVENT_FAIL, diskId))
                self.sys.metrics.failure_count += 1

        elif failureGenerator.is_burst:
            failures = failureGenerator.gen_failure_burst(self.sys.num_disks_per_rack, self.sys.num_racks)
            # failures = failureGenerator.gen_failure_burst(50, 50)
            for disk_fail_time, diskId in failures:
                heappush(self.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
                self.sys.metrics.failure_count += 1
        else:
            start_genfailure_time = time.time()
            initialFailures = failureGenerator.gen_new_failures(self.sys.num_disks)
            

            if self.prev_fail_reports != None:
                fail_report_index = random.randrange(len(self.prev_fail_reports))
                fail_report = self.prev_fail_reports[fail_report_index]

                # logging.info('fail_report: {}'.format(pformat(fail_report)))
                # self.log.append('fail_report: {}'.format(pformat(fail_report)))
                self.curr_time = float(fail_report['curr_time'])
                if self.sys.manual_spool_fail:
                    spool_sample_index = random.randrange(len(self.sys.spool_samples))
                    spool_sample = self.sys.spool_samples[spool_sample_index]
                    if float(spool_sample['curr_time']) > self.curr_time:
                        self.state.policy.manual_spool_fail = True
                        self.state.policy.manual_spool_fail_sample = spool_sample

                if self.sys.place_type in [PlacementType.MLEC_C_C, PlacementType.MLEC_C_D, PlacementType.MLEC_D_C, PlacementType.MLEC_D_D]:
                    frozen_disks = self.state.policy.manual_inject_failures(fail_report, self)
                    for frozen_diskId in frozen_disks:
                        initialFailures[frozen_diskId] = YEAR*10000
                else:
                    self.state.policy.manual_inject_failures(fail_report, self)
                    for disk_info in fail_report['disk_infos']:
                        initialFailures[disk_info['diskId']] = YEAR*10000
            else:
                if self.sys.manual_spool_fail:
                    spool_sample_index = random.randrange(len(self.sys.spool_samples))
                    spool_sample = self.sys.spool_samples[spool_sample_index]
                    if float(spool_sample['curr_time']) > self.curr_time:
                        self.state.policy.manual_spool_fail = True
                        self.state.policy.manual_spool_fail_sample = spool_sample
                        frozen_disks = self.state.policy.generate_manual_spool_fail_id()
                        for frozen_diskId in frozen_disks:
                            initialFailures[frozen_diskId] = YEAR*10000
                        heappush(self.failure_queue, (float(spool_sample['curr_time']), Spool.EVENT_MANUAL_FAIL, 0))
                
            finish_genfailure_time = time.time()
            mytimer.resetGenFailTime += finish_genfailure_time - start_state_reset_time

            start_parse_failure_time = time.time()
            
            failure_idxs = np.where(np.logical_and(
                                        initialFailures < self.mission_time,
                                        initialFailures >= self.curr_time))[0]
            
            finish_parse_failure_time = time.time()
            mytimer.resetParseFailTime += finish_parse_failure_time - start_parse_failure_time

            for diskId in failure_idxs:
                heappush(self.failure_queue, (initialFailures[diskId], Disk.EVENT_FAIL, diskId))
                self.sys.metrics.failure_count += 1
            
            
        
        #-----------------------------------------------------
        # generate disks failures events from failure traces
        #-----------------------------------------------------
        # if self.use_trace:
        # heappush(self.failure_queue, (0, Disk.EVENT_FAIL, 0))
        # heappush(self.failure_queue, (0.001, Disk.EVENT_FAIL, 1))
        # return

        # logging.info("initialFailures: {}".format(len(initialFailures)))


        
            
        # # debug print after heapsort, clearer for debug
        # for disk_fail_time, _, diskId in self.failure_queue:
        #     logging.debug("    >>>>> reset {} {} {}".format(disk_fail_time, Disk.EVENT_FAIL, diskId))
            
        
        if self.sys.rack_fail > 0:
            for diskId in range(self.sys.rack_fail):
                disk_fail_time = random.random() * YEAR
                heappush(self.failure_queue, (disk_fail_time, Rack.EVENT_FAIL, diskId))
                # logging.info("    >>>>> reset {} {} {}".format(disk_fail_time, Rack.EVENT_FAIL, diskId))


    def get_next_event(self) -> Optional[Tuple[float, str, int]]:        
        return self.state.policy.get_next_event(self)


    def clean_failures(self):
        self.state.policy.clean_failures()

    #----------------------------------------------------------------
    # run simulation based on statistical model or production traces
    #----------------------------------------------------------------
    def run_simulation(self, failureGenerator, mytimer):
        # logging.info("---------")
        # self.log.append("---------")

        self.sys.metrics.iter_count += 1
        self.mytimer: Mytimer = mytimer
        self.failure_generator = failureGenerator
        simulation_start_time = time.time()

        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
        # Debug purpose, static seed
        # np.random.seed(1)
        
        self.reset(failureGenerator, mytimer)
        if self.state.policy.sys_failed:
            # logging.info("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
            self.clean_failures()
            return 1

        reset_done_time = time.time()
        self.mytimer.resettime += (reset_done_time - simulation_start_time)

        self.curr_time = 0
        prob = 0
        loss_events = 0
        
        # logging.info("Initial network - inter: %s, intra: %s", self.state.network.inter_rack_avail, self.state.network.intra_rack_avail)
        events = 0
        while True:
            event_start = time.time()
            #---------------------------
            # extract the next event
            #---------------------------
            next_event = self.get_next_event()
            if next_event is None:
                break
            
            (event_time, event_type, diskId) = next_event
            # logging.info("Event %s on disk %s occured at %s", event_type, diskId, event_time)
            # logging.info("Delayed repair queue: %s", self.delay_repair_queue)
            # logging.info("Repair queue: %s", self.repair_queue)
            # logging.info("Failure queue length: %s", len(self.failure_queue))
            
            get_event_done_time = time.time()
            self.mytimer.getEventTime += (get_event_done_time - event_start)

            # logging.info("----record----  {} {} {}".format(event_time, event_type, diskId))
            # self.log.append("----record----  {} {} {}".format(event_time, event_type, diskId))
            
            #--------------------------------------
            # update all disks state/priority
            #--------------------------------------
            self.curr_time = event_time
            self.state.update_curr_time(self.curr_time)
            update_clock_done_time = time.time()
            self.mytimer.updateClockTime += (update_clock_done_time - get_event_done_time)

            if event_type == Spool.EVENT_MANUAL_FAIL:
                diskId = self.state.policy.manual_inject_spool_failure()
                if self.state.policy.check_pdl():
                    prob = 1
                    # logging.info("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    # self.log.append("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    break

                self.update_repair_events(Disk.EVENT_FAIL, diskId)
                events += 1
                continue

            self.state.policy.update_disk_state(event_type, diskId)
            update_state_done_time = time.time()
            self.mytimer.updateStateTime += (update_state_done_time - update_clock_done_time)

            self.state.policy.update_disk_priority(event_type, diskId)
            update_priority_done_time = time.time()
            self.mytimer.updatePriorityTime += (update_priority_done_time - update_state_done_time)

            #--------------------------------------
            # In MLEC-RAID, each disk group contains n=k+m disks. A rack can have multiple diskgroups
            # If this disk group has m+1 or more disk failures, then we need to repair the 
            # disk group using network erasure.
            # Note that other disk groups in this rack can be healthy and don't need repair
            #--------------------------------------
            if self.sys.place_type in [PlacementType.MLEC_C_C, PlacementType.MLEC_C_D, PlacementType.MLEC_D_C, PlacementType.MLEC_D_D]:
                new_diskgroup_failure = self.state.policy.update_diskgroup_state(event_type, diskId)
                if new_diskgroup_failure != None:
                    self.state.policy.update_diskgroup_priority(event_type, new_diskgroup_failure, diskId)
            update_diskgroup_priority_done_time = time.time()
            self.mytimer.updateDiskgrpPriorityTime += (update_diskgroup_priority_done_time - update_priority_done_time)
            #---------------------------
            # exceed mission-time, exit
            #---------------------------
            if self.curr_time > self.mission_time:
                break
            #---------------------------
            # new failure should be generated
            #  Note: this is to generate the failure time for the disk that we are going to 
            #        use to replace the failed disk
            #---------------------------
            if event_type == Disk.EVENT_REPAIR and not failureGenerator.is_burst:
                new_failure_intervals = failureGenerator.gen_new_failures(1)
                disk_fail_time = new_failure_intervals[0] + self.curr_time
                if disk_fail_time < self.mission_time:
                    heappush(self.failure_queue, (disk_fail_time, Disk.EVENT_FAIL, diskId))
                    # logging.info("    >>>>> reset {} {}".format(diskId, disk_fail_time))

            gen_new_fail_done_time = time.time()
            self.mytimer.newFailTime += (gen_new_fail_done_time - update_diskgroup_priority_done_time)
            
            #---------------------------
            # failure event, check PDL
            #---------------------------
            if event_type == Disk.EVENT_FAIL or event_type == Rack.EVENT_FAIL:
                #curr_failures = self.state.get_failed_disks()
                if self.state.policy.check_pdl():
                    prob = 1
                    # logging.info("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    # self.log.append("  >>>>>>>>>>>>>>>>>>> data loss >>>>>>>>>>>>>>>>>>>>>>>>>>>>  ")
                    break

            check_loss_done_time = time.time()
            self.mytimer.checkLossTime += (check_loss_done_time - gen_new_fail_done_time)
            
            
            self.update_repair_events(event_type, diskId)
            update_repair_event_done_time = time.time()
            self.mytimer.updateRepairTime += (update_repair_event_done_time - check_loss_done_time)
            events += 1
            # logging.info("------------END EVENT---------------")

        self.clean_failures()
        return prob


    def update_repair_events(self, event_type, diskId):
        self.state.policy.update_repair_events(event_type, diskId, self.repair_queue)
    
    def print_log(self):
        for log in self.log:
            print(log)

