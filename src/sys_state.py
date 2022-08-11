from math import log
from drive_args import DriveArgs
import numpy as np
from heapq import *

class SysState:
    
    def __init__(self, total_drives, drive_args, placement, drives_per_server, 
                top_d_shards = 1, top_p_shards = 0, adapt = False, server_fail = 0, distribution = 'exp'):
        self.drive_args = drive_args
        self.mode = placement

        self.total_drives = total_drives
        self.good_cnt = self.total_drives
        self.fail_cnt = 0
        self.drives_per_server = drives_per_server
        self.top_d_shards = top_d_shards
        self.top_p_shards = top_p_shards

        self.drive_args = drive_args
        self.mtbf_days = -365.25/log(1-drive_args.afr_in_pct/100)
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
        elif placement == 'RAID_NET':
            self.place_type = 3

        if distribution == 'exp':
            self.failure_generator = Exponential(self.mtbf_days)
        elif distribution == 'weibull':
            self.failure_generator = Weibull(self.mtbf_days)

    def gen_drives(self):
        # Initialize simulated drives
        self.drives = self.gen_failure_times(self.total_drives)
        
    # This generate a system of failure times
    def gen_failure_times(self, n):
        temp = self.failure_generator.get(n)
        return temp
    
    
    def dp_gen_new_failures(self, n):
        if n > self.failures_store_len - self.failures_store_idx:
            self.failures_store = self.failure_generator.get(self.failures_store_len)
            self.failures_store_idx = 0

        store_end = self.failures_store_idx + n
        new_failures = self.failures_store[self.failures_store_idx: store_end]
        self.failures_store_idx = store_end

        return new_failures


class Exponential:
    def __init__(self, mtbf_days):
        self.mtbf_days = mtbf_days

    def get(self, n):
        return np.random.exponential(self.mtbf_days, n)


class Weibull:
    def __init__(self, mtbf_days):
        self.mtbf_days = mtbf_days

    def get(self, n):
        return np.random.weibull(2, n) * self.mtbf_days