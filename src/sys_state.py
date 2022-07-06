from math import log
from drive_args import DriveArgs
import numpy as np

class SysState:
    
    def __init__(self, total_drives, drive_args: DriveArgs, placement='DP'):
        self.drive_args = drive_args
        self.mode = placement

        # for the sake of simulation, we are going to round the total_drives
        self.total_drives = (total_drives // drive_args.total_shards) * drive_args.total_shards
        self.good_cnt = self.total_drives
        self.fail_cnt = 0

        self.drive_args = drive_args

    def gen_drives(self):
        # Initialize simulated drives
        # print("Days between failures {}".format(failure_rate))
        self.drives = self.gen_failure_times(self.total_drives)
        
    # This generate a system of failure times
    def gen_failure_times(self, n):
        failure_rate = -365.25/log(1-self.drive_args.afr_in_pct/100)
        temp = np.random.exponential(failure_rate, n)
        return temp


        # This generate a system of failure times
    def gen_failure_times_jiajun(self, n):
        failure_rate = -365.25/log(1-self.drive_args.afr_in_pct/100)
        temp = np.random.exponential(failure_rate, n)

        # If it is RAID, we reshape it so that its a list of stripes
        if self.mode == 'RAID':
            temp = np.reshape(temp, (-1, self.drive_args.total_shards))
        
        return temp

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