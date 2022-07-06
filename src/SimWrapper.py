import subprocess
import os
from multiprocessing.pool import ThreadPool

import logging

def call_proc(cmd):

    devnull = open(os.devnull, 'w')
    print ("Called with " + cmd)
    p = subprocess.Popen(cmd, stdout=devnull, shell=True)


if __name__ == "__main__":
    logging.basicConfig(filename="simwrapper.log", level=logging.DEBUG)

    pool = ThreadPool(10);

    for i in range(1, 100, 5):
        disk_num = 164 * i;
        file_name = "164model-D164-N" + str(disk_num) + ".txt";
        cmd = "python SOLsim.py -type 1 -rebuildRate 50 -utilizeRatio 1 -no-trace -D 164 -N " + str(disk_num) + " -failRatio 0.01 -T 86400 -k 8 -m 2 -outputFile ./164model-D164-P1/164model-D164-N" + str(disk_num) + ".txt"
        print cmd
        #pool.apply(call_proc, (cmd, ))
    
    # for i in range(0, 13):
    #     k = 20 - i;
    #     cmd = "python SOLsim.py -type 1 -rebuildRate 50 -utilizeRatio 1 -no-trace -D 126 -N 12800 -failRatio 0.05 -T 31536000 -k " + str(k) + " -m 2 -outputFile ./" + str(k) + "-2.txt > /dev/null";
    #     pool.apply(call_proc, (cmd,))
    #     logging.info(cmd)

    pool.close() 
    pool.join()
