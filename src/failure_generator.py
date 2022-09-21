from math import log
from drive_args import DriveArgs
import numpy as np
from heapq import *

class FailureGenerator:
    
    def __init__(self, afr, distribution = None):
        self.afr = afr
        self.distribution = distribution
        if distribution == None:
            self.distribution = Exponential(afr)
        
        self.failures_store = []
        self.failures_store_len = 100
        self.failures_store_idx = self.failures_store_len
        
    # This generate a system of failure times
    def gen_failure_times(self, n):
        temp = self.distribution.get(n)
        return temp
    
    
    def gen_new_failures(self, n):
        if n > self.failures_store_len - self.failures_store_idx:
            self.failures_store = self.distribution.get(self.failures_store_len)
            self.failures_store_idx = 0

        store_end = self.failures_store_idx + n
        new_failures = self.failures_store[self.failures_store_idx: store_end]
        self.failures_store_idx = store_end

        return new_failures


class Exponential:
    def __init__(self, afr_in_pct):
        self.mtbf_days = -365.25/log(1-afr_in_pct/100)

    def get(self, n):
        return np.random.exponential(self.mtbf_days, n)


class Weibull:
    def __init__(self, alpha, beta):
        self.alpha = alpha
        self.beta = beta

    def get(self, n):
        return np.random.weibull(self.beta, n) * self.alpha * 365.25