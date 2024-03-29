from __future__ import annotations
import typing
if typing.TYPE_CHECKING:
    from simulate import Simulate

import copy
from components.disk import Disk
from components.rack import Rack
from system import System
from typing import Dict
from mytimer import Mytimer
from constants.PlacementType import PlacementType
from constants.constants import kilo

from policies.policy_factory import get_policy
from policies.policy import Policy

from components.network import Network

class State:
    #--------------------------------------
    # The 2 possible state
    #--------------------------------------
    SYSTEM_STATE_NORMAL = "state normal"
    SYSTEM_STATE_FAILED = "state failed"

    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, sys, mytimer, simulation):
        #----------------------------------
        self.simulation: Simulate = simulation
        self.sys: System = sys
        self.n: int = sys.k + sys.m
        
        self.disks = self.sys.disks
        
        # for diskId in self.disks:
        #     disk = self.disks[diskId]
        #     disk.state = Disk.STATE_NORMAL
        #     disk.priority = 0
        #     disk.repair_time = {}



        self.racks = self.sys.racks

        self.curr_time: float = 0.0
        self.failed_disks = {}
        self.failed_racks = {}
        self.repairing = True
        self.repair_start_time = 0

        self.mytimer: Mytimer = mytimer
        self.policy: Policy = get_policy(self.sys.place_type, self)
        # self.network = copy.deepcopy(self.sys.network)
        self.network = sys.network
        #----------------------------------


    def update_curr_time(self, curr_time):
        self.curr_time = curr_time
        self.policy.curr_time = curr_time

    

    # This returns array [{rackId: failedDisks}, numRacksWithFailure]
    def get_failed_disks_each_rack(self):
        failures = {}
        num_racks_with_failure = 0
        for rackId in self.sys.racksIds:
            rack_failures = self.get_failed_disks_per_rack(rackId)
            
            if (len(rack_failures) > 0):
                failures[rackId] = rack_failures
                num_racks_with_failure += 1
    
        return [failures, num_racks_with_failure]
    
    # This returns dict {stripeId: [disksId]}
    # WARNING: only use for debug. This will cause long simulation time
    def get_failed_disks_each_spool(self):
        spools = self.sys.net_raid_spools_layout
        result = {}
        for ssid in spools:
            failed_disks = []
            for diskId in spools[ssid]:
                if self.disks[diskId].state == Disk.STATE_FAILED:
                    failed_disks.append(diskId)
            
            if len(failed_disks) != 0:
                result[ssid] = failed_disks
        return result            

    def get_failed_disks_per_rack(self, rackId):
        # logging.info("sedrver {} get: {}".format(rackId, list(self.racks[rackId].failed_disks.keys())))
        return self.racks[rackId].failed_disks.keys()

    def get_failed_disks_per_spool(self, spoolId):
        failed_disks = []
        spool = self.sys.net_raid_spools_layout[spoolId]
        for diskId in spool:
            # logging.info("  get_failed_disks_per_spool  diskId: {}".format(diskId))
            if self.disks[diskId].state == Disk.STATE_FAILED:
                failed_disks.append(diskId)
        return failed_disks


    def get_failed_disks_per_spool_diskId(self, diskId):
        failed_disks = []
        rackId = diskId // self.sys.num_disks_per_rack
        spoolId = (diskId % self.sys.num_disks_per_rack) // self.n
        spool = self.sys.flat_cluster_rack_layout[rackId][spoolId]
        for d in spool:
            # logging.info("  get_failed_disks_per_spool  diskId: {}".format(diskId))
            if self.disks[d].state == Disk.STATE_FAILED:
                failed_disks.append(d)
        return failed_disks


    def get_failed_disks(self):
        return self.failed_disks.keys()

    def get_failed_racks(self):
        return self.failed_racks.keys()
