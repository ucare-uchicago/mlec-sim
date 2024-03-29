class Spool:
    #----------------------------
    # The 2 possible disk states
    #----------------------------
    STATE_NORMAL = "< slec pool state normal >"
    STATE_FAILED = "< slec pool state failed >"

    #----------------------------------
    # The possible events
    #----------------------------------
    EVENT_FAIL = "<slec pool failure>"
    EVENT_REPAIR = "<slec pool repair>"
    EVENT_FASTREBUILD = "<slec pool fast rebuild>"
    EVENT_DELAYED_FAIL = "<slec pool delayed fail>"
    EVENT_MANUAL_FAIL = "<slec pool manual failure>"

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
        # repair data for each disk to repair in network
        self.repair_data = repair_data
        self.lost_local_stripes: float = 0.0
        # total data to be repaired in network
        self.total_network_repair_data: float = 0.0
        self.is_in_repair: bool = False
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
        self.failed_disks_network_repair = {}
        # --
        self.priority_percents = {}
        self.curr_prio_repair_started: bool = False
        #
        # self.good_num: int = 0
        # self.fail_num: int = 0
        self.rebuild_width = 0
        self.next_repair_disk = -1
