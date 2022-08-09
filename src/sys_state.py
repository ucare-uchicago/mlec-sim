from math import log
from drive_args import DriveArgs
import numpy as np
from heapq import *

class SysState:
    
    def __init__(self, total_drives, drive_args, placement, drives_per_server, 
                top_d_shards = 1, top_p_shards = 0, adapt = False, server_fail = 0):
        self.drive_args = drive_args
        self.mode = placement

        self.total_drives = total_drives
        self.good_cnt = self.total_drives
        self.fail_cnt = 0
        self.drives_per_server = drives_per_server
        self.top_d_shards = top_d_shards
        self.top_p_shards = top_p_shards

        self.drive_args = drive_args
        self.failure_rate = -365.25/log(1-drive_args.afr_in_pct/100)
        self.failures_store = []
        self.failures_store_len = 100
        self.failures_store_idx = self.failures_store_len

        self.adapt = adapt
        self.server_fail = server_fail

        if placement == 'RAID':
            self.place_type = 0
        elif placement == 'DP':
            self.place_type = 1
        elif placement == 'MLEC':
            self.place_type = 2

    def gen_drives(self):
        # Initialize simulated drives
        # print("Days between failures {}".format(failure_rate))
        self.drives = self.gen_failure_times(self.total_drives)
        
    # This generate a system of failure times
    def gen_failure_times(self, n):
        temp = np.random.exponential(self.failure_rate, n)
        return temp
    
    
    def dp_gen_new_failures(self, n):
        if n > self.failures_store_len - self.failures_store_idx:
            self.failures_store = np.random.exponential(self.failure_rate, self.failures_store_len)
            self.failures_store_idx = 0

        store_end = self.failures_store_idx + n
        new_failures = self.failures_store[self.failures_store_idx: store_end]
        self.failures_store_idx = store_end

        return new_failures

    def fail(self, n):
        self.good_cnt -= n
        self.fail_cnt += n

    def repair(self, n):
        self.good_cnt += n
        self.fail_cnt -= n

    def print(self, injected=[]):
        mystr = '---------\n'
        mystr += 'Total Drv: ' + str(self.total_drives) + '\n'
        mystr += 'Good Cnt: ' + str(self.good_cnt) + '\n'
        mystr += 'Failed Cnt: ' + str(self.fail_cnt) + '\n'
        for inj in injected:
            mystr += inj + '\n'
        mystr += '---------'
        print(mystr, flush=True)

    def failure_rate(self):
        failure_rate = -365.25/log(1-self.drive_args.afr_in_pct/100)
        return failure_rate