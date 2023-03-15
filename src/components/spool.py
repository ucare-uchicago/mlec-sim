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
    def __init__(self, spoolId, num_disks, rackId=-1, mpoolId=-1, repair_data=-1):
        self.spoolId = spoolId
        self.num_disks = num_disks
        self.state = self.STATE_NORMAL
        # -------------------
        self.rackId = rackId
        self.mpoolId = mpoolId
        self.rackgroupId: int = -1
        #-------------------------------
        # initialize the repair priority
        #-------------------------------
        self.priority = 0
        self.failure_detection_time: float = 0.0
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
        # -----
        self.min_disk_counter = 0
        self.max_disk_counter = 0
        # ----
        # priority queue for local dp
        self.disk_priority_queue = {}
        self.disk_max_priority = 0
        self.failed_disks_undetected = {}
        self.failed_disks_in_repair = {}
        self.disk_repair_max_priority = 0
