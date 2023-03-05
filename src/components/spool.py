class Spool:
    #----------------------------
    # The 2 possible disk states
    #----------------------------
    STATE_NORMAL = "< slec pool state normal >"
    STATE_FAILED = "< slec pool state failed >"

    #----------------------------------
    # The 2 possible events
    #----------------------------------
    EVENT_FAIL = "<slec pool failure>"
    EVENT_REPAIR = "<slec pool repair>"
    EVENT_DELAYED_FAIL = "<slec pool delayed fail>"

    #----------------------------------
    # Initialize the rack
    #----------------------------------
    def __init__(self, spoolId, repair_data, num_disks, rackId=-1, mpoolId=-1):
        self.spoolId = spoolId
        self.num_disks = num_disks
        self.rackId = rackId
        self.mpoolId = mpoolId
        self.state = self.STATE_NORMAL
        # -------------------
        # rackGroupId, used for netraid
        # -------
        self.rackGroupId: int = -1
        #-------------------------------
        # initialize the repair priority
        #-------------------------------
        self.priority = 0
        #-------------------------------
        # initialize priority percent
        #-------------------------------
        self.percent = {} 
        self.repair_time = {}
        self.repair_data = repair_data
        #-------------------------------
        self.estimate_repair_time = 0.0
        self.repair_start_time = 0.0
        self.curr_repair_data_remaining = 0.0
        self.init_repair_start_time = 0.0
        #-------------------------------
        self.failed_disks = {}
        self.diskIds = []
        # -----------
        self.repair_rate = -1
        