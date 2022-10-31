from disk import Disk
from rack import Rack
from policies import *

class State:
    #--------------------------------------
    # The 2 possible state
    #--------------------------------------
    SYSTEM_STATE_NORMAL = "state normal"
    SYSTEM_STATE_FAILED = "state failed"

    #--------------------------------------
    # system state consists of disks state
    #--------------------------------------
    def __init__(self, sys, mytimer=None):
        #----------------------------------
        self.sys = sys
        self.n = sys.k + sys.m
        self.racks = {}
        self.disks = self.sys.disks
        for diskId in self.disks:
            disk = self.disks[diskId]
            disk.state = Disk.STATE_NORMAL
            disk.priority = 0
            disk.repair_time = {}
        if self.sys.place_type == 2:
            # rack_repair_data = sys.diskSize * self.n
            rack_repair_data = sys.diskSize * (self.sys.m + 1)
        else:
            # rack_repair_data = sys.diskSize * self.sys.num_disks_per_rack
            rack_repair_data = sys.diskSize * (self.sys.m + 1)

        self.stripeset_num_per_rack = sys.num_disks_per_rack // self.n
        for rackId in self.sys.racks:
            self.racks[rackId] = Rack(rackId, rack_repair_data, self.stripeset_num_per_rack)
        self.curr_time = 0
        self.failed_disks = {}
        self.failed_racks = {}
        self.repairing = True
        self.repair_start_time = 0

        self.mytimer = mytimer

        if self.sys.place_type == 0:
            self.policy = RAID(self)
        elif self.sys.place_type == 1:
            self.policy = Decluster(self)
        elif self.sys.place_type == 2:
            self.policy = MLEC(self)
        elif self.sys.place_type == 3:
            self.policy = NetRAID(self)
        elif self.sys.place_type == 4:
            self.policy = MLECDP(self)
        elif self.sys.place_type == 5:
            self.policy = NetDP(self)
        #----------------------------------
        



    def update_curr_time(self, curr_time):
        self.curr_time = curr_time
        self.policy.curr_time = curr_time



    #----------------------------------------------
    # update disk state
    #----------------------------------------------
    def update_disk_state(self, event_type, diskId):
        return self.policy.update_disk_state(event_type, diskId)


    #----------------------------------------------
    # update disk priority and compute disk repair time
    #----------------------------------------------
    def update_disk_priority(self, event_type, diskset):
        return self.policy.update_disk_priority(event_type, diskset)


    #----------------------------------------------
    # update rack state
    #----------------------------------------------
    def update_rack_state(self, event_type, diskId):
        return self.policy.update_rack_state(event_type, diskId)




                                
    
    #----------------------------------------------
    # update network-level priority
    #----------------------------------------------
    def update_rack_priority(self, event_type, new_failed_rack, diskId):
        self.policy.update_rack_priority(event_type, new_failed_rack, diskId)



    def update_diskgroup_state(self, event_type, diskId):
        return self.policy.update_diskgroup_state(event_type, diskId)

    
    # This returns array [{rackId: failedDisks}, numRacksWithFailure]
    def get_failed_disks_each_rack(self):
        failures = {}
        num_racks_with_failure = 0
        for rackId in self.sys.racks:
            failures[rackId] = self.get_failed_disks_per_rack(rackId)
            
            if (len(failures[rackId]) > 0):
                num_racks_with_failure += 1
    
        return [failures, num_racks_with_failure]
        
    def update_diskgroup_priority(self, event_type, new_failed_rack, diskId):
        self.policy.update_diskgroup_priority(event_type, new_failed_rack, diskId)


    def get_failed_disks_per_rack(self, rackId):
        # logging.info("sedrver {} get: {}".format(rackId, list(self.racks[rackId].failed_disks.keys())))
        return list(self.racks[rackId].failed_disks.keys())


    def get_failed_disks_per_stripeset(self, stripesetId):
        failed_disks = []
        stripeset = self.sys.net_raid_stripesets_layout[stripesetId]
        for diskId in stripeset:
            # logging.info("  get_failed_disks_per_stripeset  diskId: {}".format(diskId))
            if self.disks[diskId].state == Disk.STATE_FAILED:
                failed_disks.append(diskId)
        return failed_disks


    def get_failed_disks_per_stripeset_diskId(self, diskId):
        failed_disks = []
        rackId = diskId // self.sys.num_disks_per_rack
        stripesetId = (diskId % self.sys.num_disks_per_rack) // self.n
        stripeset = self.sys.flat_cluster_rack_layout[rackId][stripesetId]
        for d in stripeset:
            # logging.info("  get_failed_disks_per_stripeset  diskId: {}".format(diskId))
            if self.disks[d].state == Disk.STATE_FAILED:
                failed_disks.append(d)
        return failed_disks


    def get_failed_disks(self):
        return list(self.failed_disks.keys())

    def get_failed_racks(self):
        return list(self.failed_racks.keys())
