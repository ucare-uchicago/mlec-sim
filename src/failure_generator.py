from math import log
import numpy as np
from heapq import heappush, heappop
import pandas as pd
import random

class FailureGenerator:
    
    def __init__(self, afr, distribution = None, is_burst = False):
        self.afr = afr
        self.distribution = distribution
        if distribution == None:
            self.distribution = Exponential(afr)
        self.is_burst = is_burst
        
        self.failures_store = []
        self.failures_store_len = 100
        self.failures_store_idx = self.failures_store_len
        
    # This generate a system of failure times
    def gen_failure_times(self, n):
        temp = self.distribution.get(n)
        return temp
    
    # When a disk fails, we will recover data on a spare disk
    # Here we generate failure for the spare disk
    # it's expensive to call the "random" function (e.g. np.random.exponential)
    # Therefore, we call the random function once and get e.g. 100 failures
    # We cache them, and use them for the next 100 spare disks.
    def gen_new_failures(self, n):
        if n > self.failures_store_len - self.failures_store_idx:
            self.failures_store = self.distribution.get(self.failures_store_len)
            self.failures_store_idx = 0

        store_end = self.failures_store_idx + n
        new_failures = self.failures_store[self.failures_store_idx: store_end]
        self.failures_store_idx = store_end

        return new_failures

    def gen_failure_burst(self, disks_per_rack, num_total_racks):
        if hasattr(self.distribution, 'gen_failure_burst'):
            return self.distribution.gen_failure_burst(disks_per_rack, num_total_racks)
        failures = []
        (num_fail_racks, num_fail_disks) = self.distribution.sample()
        assert (num_fail_racks <= num_fail_disks), 'have more failed racks than failed disks which is impossible!'

        # we randomly choose num_racks racks to places place the failures
        rackids = random.sample(range(num_total_racks), num_fail_racks)

        # we first make sure every affected rack has at least one disk failure
        # we then randomly distribute the remaining disk failures among these racks
        rack_disk_nums = [1] * num_fail_racks
        for i in range(num_fail_racks, num_fail_disks):
            rack_index = random.randrange(num_fail_racks)
            rack_disk_nums[rack_index] += 1

        for i in range(num_fail_racks):
            num_fail_disks_per_rack = rack_disk_nums[i]
            rackid = rackids[i]
            disk_indices_in_rack = random.sample(range(disks_per_rack), num_fail_disks_per_rack)
            for disk_index_in_rack in disk_indices_in_rack:
                diskid = disks_per_rack * rackid + disk_index_in_rack
                failures.append((0, diskid))        # (failure time,  failure disk id)

        # print((num_fail_racks, num_fail_disks))
        # print(rackids)
        # print(rack_disk_nums)
        # print(failures)
        return failures




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

class GoogleBurst:
    def __init__(self, disks_per_rack, num_racks):
        self.num_racks = num_racks
        self.disks_per_rack = disks_per_rack
        googleOccurances = pd.read_csv("failures/google/{}-rack-{}-node.csv".format(num_racks, disks_per_rack))
        self.population = []
        self.probs = []
        for index, row in googleOccurances.iterrows():
            # print(row)
            racks = int(row['num of racks affected'])
            nodes = int(row['num of nodes affected'])
            prob = row['probability']
            self.population.append((racks, nodes))
            self.probs.append(prob)

    def sample(self):
        sample = random.choices(self.population, self.probs)
        return sample[0]



if __name__ == "__main__":
    bursts = GoogleBurst(50, 50)
    print(bursts.population)
    print(bursts.probs)

    failureGenerator = FailureGenerator(1, bursts)


    for i in range(100):
        failureGenerator.gen_failure_burst(50, 50)
        print('---')
