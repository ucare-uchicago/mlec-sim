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
    def __init__(self, diskId, repair_data):
        #-------------------------------
        # initialize the state be normal
        #-------------------------------
        self.diskId = diskId
        self.rackId = 0
        self.stripesetId = 0
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
        # self.repair_start_time = 0
        # self.curr_repair_data_remaining = 0
        # self.good_num = 0
        # self.fail_num = 0


    def update_disk_state(self, state):
        self.state = state


if __name__ == "__main__":
    disk = Disk(20)
    disk.update_disk(10, "normal")
            
