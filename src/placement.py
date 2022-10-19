from system import System
from disk import Disk
import logging
from rack import Rack
from state import State

class Placement:
    def __init__(self, sys, place_type):
        #---------------------------------------
        # Initialize System Storage System
        #---------------------------------------
        self.sys = sys
        self.place_type = place_type



    def check_data_loss_prob(self, state):
        switcher = {
                0: 'flat_cluster',
                1: 'flat_decluster',
                2: 'mlec_cluster'}
        #------------------------------------------------
        if self.place_type == 0:
            return self.flat_cluster_simulate(state)
        if self.place_type == 1:
            return self.flat_decluster_simulate(state)
        if self.place_type == 2:
            return self.mlec_cluster_simulate(state)
        if self.place_type == 3:
            return self.net_raid_simulate(state)
        if self.place_type == 4:
            return self.mlec_dp_simulate(state)
            


    def flat_cluster_simulate(self, state):
        prob = 0
        for rackId in self.sys.racks:
            fail_per_rack = state.get_failed_disks_per_rack(rackId)
            stripesets_per_rack = self.sys.flat_cluster_rack_layout[rackId]
            for stripeset in stripesets_per_rack:
                fail_per_set = set(stripeset).intersection(set(fail_per_rack))
                if len(fail_per_set) > self.sys.m:
                    prob = 1
                    return prob
        return prob


    def mlec_cluster_simulate(self, state):
        prob = 0
        for i in range(state.policy.num_diskgroup_stripesets):
            failed_diskgroups_per_stripeset = state.policy.get_failed_diskgroups_per_stripeset(i)
            if len(failed_diskgroups_per_stripeset) > self.sys.top_m:
                prob = 1
        return prob

    def mlec_dp_simulate(self, state):
        prob = 0
        for i in range(state.policy.num_rack_groups):
            if state.policy.rack_group_failures[i] > self.sys.top_m:
                prob = 1
                return prob
        return prob


    def net_raid_simulate(self, state):
        prob = 0
        failed_disks = state.get_failed_disks()
        for diskId in failed_disks:
            disk = state.disks[diskId]
            failed_disks_per_stripeset = state.get_failed_disks_per_stripeset(disk.stripesetId)
            if len(failed_disks_per_stripeset) > self.sys.top_m:
                prob = 1
                return prob
        return prob



    def flat_decluster_simulate(self, state):
        prob = 0
        for diskId in state.disks:
            #print "diskId", diskId, "priority",state.disks[diskId].priority
            if state.disks[diskId].priority > self.sys.m:
                prob = 1
                return prob
        return prob




if __name__ == "__main__":
    #-----------------------------------------------------------------------------------
    sys = System(100, 10, 8, 2,0,2,1,2)
    placement = Placement(sys, 1)
    state = State(sys)
    print("at the beginning, data loss: {}".format(placement.flat_cluster_simulate(state)))

    state.racks[0].failed_disks[0] = 1
    print("after disk 0 fails, data loss? : {}".format(placement.flat_cluster_simulate(state)))
    state.racks[0].failed_disks[1] = 1
    print("after disk 1 fails, data loss? : {}".format(placement.flat_cluster_simulate(state)))
    state.racks[0].failed_disks[2] = 1
    print("after disk 2 fails, data loss? : {}".format(placement.flat_cluster_simulate(state)))


