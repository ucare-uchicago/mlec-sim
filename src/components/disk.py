from __future__ import annotations
import typing
from typing import Optional

if typing.TYPE_CHECKING:
    from components.network import NetworkUsage

class Disk:
    #----------------------------------
    # The 2 possible state
    #----------------------------------
    STATE_NORMAL = "<state normal>"
    STATE_FAILED = "<state failed>"
    #----------------------------------
    # The 3 possible events
    #----------------------------------
    EVENT_DELAYED_FAIL = "<disk delayed fail>"
    EVENT_FAIL = "<disk failure>"
    EVENT_FASTREBUILD = "<disk fast rebuild>"
    EVENT_REPAIR = "<disk repair>"

    #----------------------------------
    # Initialize the disk
    #----------------------------------
    def __init__(self, diskId, repair_data, rackId):
        #-------------------------------
        # initialize the state be normal
        #-------------------------------
        self.diskId: int = diskId
        self.rackId: int = rackId
        self.diskgroupId: int = 0
        self.stripesetId: int = 0
        #-------------------------------
        # initialize the state be normal
        #-------------------------------
        self.state = self.STATE_NORMAL
        #-------------------------------
        # initialize the repair priority
        #-------------------------------
        self.priority = 0
        #-------------------------------
        # initialize priority percent
        #-------------------------------
        self.repair_time = {}
        self.repair_data = repair_data
        #-------------------------------
        # initialize repair metrics
        #-------------------------------
        self.repair_start_time: float = 0
        self.estimate_repair_time: float = 0
        self.curr_repair_data_remaining: float = 0
        self.good_num: int = 0
        self.fail_num: int = 0
        #-------------------------------
        # marking how much network resource this disk is using for repair
        #   none value means its not using any
        #-------------------------------
        self.network_usage: Optional[NetworkUsage] = None
        self.paused: bool = False
        #-------------------------------


    def update_disk_state(self, state):
        self.state = state
        
    # Override toString()
    def __str__(self):
        return "[dId: {}, rId: {}, sId: {}, state: {}, prio: {}, repair_time: {}, repair start time: {}]".format(self.diskId, self.rackId, self.stripesetId, self.state, self.priority, self.repair_time, self.repair_start_time)
            
