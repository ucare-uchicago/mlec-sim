from mpmath import mpf

class Server:
    #----------------------------
    # The 2 possible disk states
    #----------------------------
    STATE_NORMAL = "< server state normal >"
    STATE_FAILED = "< server state failed >"

    #----------------------------------
    # The 2 possible events
    #----------------------------------
    EVENT_FAIL = "<server failure>"
    EVENT_REPAIR = "<server repair>"

    #----------------------------------
    # Initialize the server
    #----------------------------------
    def __init__(self, serverId, repair_data, stripeset_num):
        self.serverId = serverId
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
        self.stripesets_repair_finish = []
        for i in range(stripeset_num):
            self.stripesets_repair_finish.append(0)