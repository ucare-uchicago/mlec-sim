import logging
import operator as op
import random
import argparse
import numpy as np
from trinity import Trinity

#----------------------------------------------------
# Simulation for affected stripesets
#----------------------------------------------------

class Affect:
    def __init__(self, sys):
        #------------------------------------
        # Initialize Trinity storgae system
        #------------------------------------
        self.sys = sys
        #------------------------------------
        
        
    def flat_decluster_simulate(self, failures):
        prob = 0
        total_sets = 0
        affect_frequency = {}
        for i in range(self.sys.m+1):
            affect_frequency[i] = 0
        n = self.sys.k + self.sys.m
        #-------------------------
        # flat decluster layout
        #-------------------------
        for server_stripeset in self.sys.flat_decluster_server_layout:
            fail_per_server = set(server_stripeset).intersection(set(failures))
            total_sets += self.ncr(len(server_stripeset), n)
            fail_num = len(fail_per_server)
            good_per_server = len(server_stripeset) - fail_num
            if fail_num > self.sys.m:
                prob = 1
                max_failures = min(n, fail_num)
                for i in range(self.sys.m+1, max_failures+1):
                    affect_frequency[0] += self.ncr(fail_num, i) * self.ncr(good_per_server, n-i)
            else:
                for i in range(1, fail_num+1):
                    affect_frequency[i] += self.ncr(fail_num, i) * self.ncr(good_per_server, n-i)
        #---------------------------------------
        for key in affect_frequency:
            affect_frequency[key] = float(affect_frequency[key])/total_sets
        #---------------------------------------
        return affect_frequency





    def flat_stripeset_simulate(self, failures):
        prob = 0
        total_sets = 0
        affect_frequency = {}
        for i in range(self.sys.m+1):
            affect_frequency[i] = 0
        n = self.sys.k + self.sys.m
        for flat_stripeset in self.sys.flat_stripeset_layout:
            #--------------------------------------------------
            total_sets += 1
            fail_per_stripeset = set(flat_stripeset).intersection(set(failures))
            fail_num = len(fail_per_stripeset)
            if fail_num > self.sys.m:
                prob = 1
                affect_frequency[0] += 1
                #---------------------------------------
            else:
                if fail_num>0:
                    affect_frequency[fail_num] += 1
        #---------------------------------------
        for key in affect_frequency:
            affect_frequency[key] = float(affect_frequency[key])/total_sets
        #---------------------------------------
        return affect_frequency



    def ncr(self, n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        denom = reduce(op.mul, range(1, r+1), 1)
        return numer / denom



if __name__ == "__main__":
    #-----------------------------------------------------------------------------------
    # add arguments from command line
    #-----------------------------------------------------------------------------------
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-percent', type=float, help="failure percent",default=0.05)
    #-----------------------------------------------------------------------------------
    args = parser.parse_args()
    #-----------------------------------------------------------------------------------
    cluster_file = open('dat/cluster.txt','a')
    decluster_file = open('dat/decluster.txt','a')
    #---------------------------------------------
    # start simulations in Campaign storage system
    #---------------------------------------------
    sys = Trinity(6, 1, 9, 2,1,2,1,2,1)
    args.mb = 1
    sim = Affect(sys)
    num_iter = 100
    #large_bursts = (args.mt+1)*(args.mb+1)
    large_bursts = int(args.percent * len(sim.sys.disks))
    #--------------------------------------
