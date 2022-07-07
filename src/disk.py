from mpmath import mpf

class Disk:
    #----------------------------------
    # The 2 possible state
    #----------------------------------
    STATE_NORMAL = "<state normal>"
    STATE_FAILED = "<state failed>"
    #----------------------------------
    # The 3 possible events
    #----------------------------------
    EVENT_FAIL = "<disk failure>"
    EVENT_FASTREBUILD = "<disk fast rebuild>"
    EVENT_REPAIR = "<disk repair>"

    #----------------------------------
    # Initialize the disk
    #----------------------------------
    def __init__(self, diskId, disk_fail_distr, trace_fail_times):
        #-------------------------------
        # initialize the state be normal
        #-------------------------------
        self.diskId = diskId
        #-------------------------------
        # initialize the state be normal
        #-------------------------------
        self.state = self.STATE_NORMAL
        #-------------------------------
        # disk's local clock
        #-------------------------------
        self.clock = mpf(0)
        #-------------------------------
        # failure distribution
        #-------------------------------
        self.disk_fail_distr = disk_fail_distr
        #-------------------------------
        # failure events from trace
        #-------------------------------
        self.trace_fail_times = trace_fail_times
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
        self.decluster_sets = 0
        self.stripeset_sets = 0
        self.draid_sets = 0
        #-------------------------------
        self.repair_start_time = 0
        self.curr_repair_data_remaining = 0
        self.good_num = 0
        self.fail_num = 0





    def update_clock(self, curr_time):
        self.clock = curr_time


    def update_state(self, state):
        self.state = state


if __name__ == "__main__":
    disk = Disk(20)
    disk.update_disk(10, "normal")
            
