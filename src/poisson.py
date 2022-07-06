#~/usr/bin/python
import numpy as np
import random
import logging
#----------------------------------------------------------------------------------
# suppose you want to simulate percent%failure events in t time, arrival rate 0.01%
#----------------------------------------------------------------------------------

class Poisson:
    def __init__(self, N, percent, period):
        self.N = N
        self.percent = percent
        self.period = period


    def generate_poisson_failures(self, print_=False):
        trace_entry = []
        total_failed_disks = self.N * self.percent
        failures = random.sample(range(self.N), int(total_failed_disks))
        #--------------------------------------------------
        # Average seconds between failures
        #--------------------------------------------------
        tau = self.period / total_failed_disks
        logging.info('%d seconds between failures'%tau)
        logging.debug("--", self.N, total_failed_disks, failures, tau)
        rand = np.random.RandomState(0)  # universal random seed
        logging.debug("tau: " + str(tau) + " total_failed_disks: " + str(total_failed_disks))
        time_between_fails = np.random.poisson(tau, int(total_failed_disks))
        failure_times = []
        last_fail = 0
        for t in time_between_fails:
            failure_times.append(last_fail + t)
            last_fail = last_fail + t

        for i in range(len(failures)):
            fail_time = failure_times[i]
            diskId = failures[i]
            logging.debug("diskId", failures[i], failure_times[i])
            trace_entry.append((fail_time, diskId))
        return trace_entry
        


if __name__ == "__main__":
    poisson = Poisson(11000, 0.01, 24.0)
    poisson.generate_poisson_failures()
