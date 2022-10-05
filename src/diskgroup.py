from mpmath import mpf

class Diskgroup:
    #----------------------------
    # The 2 possible disk states
    #----------------------------
    STATE_NORMAL = "< diskgroup state normal >"
    STATE_FAILED = "< diskgroup state failed >"

    #----------------------------------
    # The 2 possible events
    #----------------------------------
    EVENT_FAIL = "<diskgroup failure>"
    EVENT_REPAIR = "<diskgroup repair>"

    #----------------------------------
    # Initialize the rack
    #----------------------------------
    def __init__(self, diskgroupId, repair_data):
        self.diskgroupId = diskgroupId
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
        # self.repair_start_time = 0
        # self.curr_repair_data_remaining = 0
        self.failed_disks = {}