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
    def __init__(self, serverId):
        self.serverId = serverId
        self.state = self.STATE_NORMAL
        #-------------------------------
        # server's local clock
        #-------------------------------
        self.clock = mpf(0)
        #-------------------------------
        # initialize the repair priority
        #-------------------------------
        self.priority = 0
        #-------------------------------
        # initialize priority percent
        #-------------------------------
        self.percent = {} 
        self.repair_time = {}
        self.repair_data = 0
        #-------------------------------
        self.repair_start_time = 0
        self.curr_repair_data_remaining = 0

    
    def update_clock(self, curr_time):
        self.clock = curr_time