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
    def __init__(self, rackId, repair_data, spool_num):
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
        self.repair_data = repair_data
        #-------------------------------
        self.repair_start_time: float = 0
        self.init_repair_start_time: float = 0
        self.estimate_repair_time: float = 0
        self.curr_repair_data_remaining: float = 0
        
        
        self.failed_disks = {}
        self.spools_repair_finish = []
        for i in range(spool_num):
            self.spools_repair_finish.append(0)
        
        self.spool_failed_disks = []
        for i in range(spool_num):
            self.spool_failed_disks.append({})