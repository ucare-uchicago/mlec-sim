class Rack:
    #----------------------------
    # The 2 possible disk states
    #----------------------------
    STATE_NORMAL = "< rack state normal >"
    STATE_FAILED = "< rack state failed >"

    #----------------------------------
    # The 2 possible events
    #----------------------------------
    EVENT_FAIL = "<rack failure>"
    EVENT_REPAIR = "<rack repair>"

    #----------------------------------
    # Initialize the rack
    #----------------------------------
    def __init__(self, rackId):
        self.rackId = rackId
        self.state = self.STATE_NORMAL
        #-------------------------------
        # initialize the repair priority
        #-------------------------------
        self.priority = 0
        #-------------------------------
        # initialize priority percent
        #-------------------------------
        self.percent = {} 
        self.repair_time = {}
        #-------------------------------
        self.repair_start_time: float = 0
        self.init_repair_start_time: float = 0
        self.estimate_repair_time: float = 0
        self.curr_repair_data_remaining: float = 0
        
        self.failed_disks = {}