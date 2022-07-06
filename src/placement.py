from trinity import Trinity
from disk import Disk
import logging

class Placement:
    def __init__(self, sys, place_type):
        #---------------------------------------
        # Initialize Trinity Storage System
        #---------------------------------------
        self.sys = sys
        self.place_type = place_type



    def check_data_loss_prob(self, state):
        switcher = {
                0: 'flat_cluster',
                1: 'flat_decluster',
                2: 'flat_stripeset',
                3: 'flat_draid'}
        #------------------------------------------------
        if self.place_type == 0:
            return self.flat_cluster_simulate(state)
        if self.place_type == 1:
            return self.flat_decluster_simulate(state)
        if self.place_type == 2:
            return self.flat_stripeset_simulate(state)
        if self.place_type == 3:
            return self.flat_draid_simulate(state)
            


    def check_data_loss_events(self, state):
        loss_events = 0
        if self.place_type == 0:
            for serverId in self.sys.servers:
                prob = 0
                fail_per_server = state.get_failed_disks_per_server(serverId)
                stripesets_per_server = self.sys.flat_cluster_server_layout[serverId]
                for stripeset in stripesets_per_server:
                    fail_per_set = set(stripeset).intersection(set(fail_per_server))
                    if len(fail_per_set) > self.sys.m:
                        prob = 1
                        break
                if prob == 1:
                    loss_events += 1
            return loss_events
        if self.place_type == 1 or self.place_type == 2 or self.place_type == 3:
            for serverId in self.sys.servers:
                prob = 0
                for diskId in self.sys.disks_per_server[serverId]:
                    if state.disks[diskId].priority > self.sys.m:
                        prob = 1
                        break
                if prob == 1:
                    loss_events += 1
            return loss_events 


    def flat_cluster_simulate(self, state):
        prob = 0
        for serverId in self.sys.servers:
            fail_per_server = state.get_failed_disks_per_server(serverId)
            stripesets_per_server = self.sys.flat_cluster_server_layout[serverId]
            for stripeset in stripesets_per_server:
                fail_per_set = set(stripeset).intersection(set(fail_per_server))
                if len(fail_per_set) > self.sys.m:
                    prob = 1
                    return prob
        return prob



    def flat_draid_simulate(self, state):
        prob = 0
        for diskId in state.disks:
            if state.disks[diskId].priority > self.sys.m:
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


    def flat_stripeset_simulate(self, state):
        prob = 0
        for diskId in state.disks:
            if state.disks[diskId].priority > self.sys.m:
                prob = 1
                return prob
        return prob




if __name__ == "__main__":
    #-----------------------------------------------------------------------------------
    sys = Trinity(6, 1, 9, 2,1,2,1,2,1)
    sim = Placement(sys, 1)
    failures = [2,3,4,6]
    
    logging.debug(sim.flat_decluster_simulate(failures))
    logging.debug(sim.flat_stripeset_simulate(failures))
            

