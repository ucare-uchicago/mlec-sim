from multiprocessing import Pool
from signal import pause
from trinity import Trinity
from parser import Parser
from weibull import Weibull
from simulate import Simulate
import numpy as np
import argparse
import logging
import time
import sys, traceback
import os, os.path
import time
import signal


class SOLsim:
    def __init__(self, mission_time, iterations_per_worker, traces_per_worker, 
                num_disks, num_disks_per_server, k, m, use_trace, place_type, traceDir, diskCap, rebuild, utilizeRatio):
        #---------------------------------------
        # >>  trace simulation 
        #---------------------------------------
        self.sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, 
                num_disks, num_disks_per_server, k, m, use_trace, place_type, traceDir, diskCap, rebuild, utilizeRatio)
        #---------------------------------------
        # >>  regular simulation 
        #---------------------------------------



def start(tasks_per_worker):
    #print "---------------------- -------------------"
    (iterations_per_worker, traces_per_worker, mission_time, num_disks, num_disks_per_server, 
            k, m, use_trace, place_type, traceDir, outputFile, diskCap, rebuildRate, utilizeRatio, failRatio) = tasks_per_worker
    #print "------------------------------------------"
    if use_trace:
        disk_fail_distr = None
    else:
        disk_fail_distr = Weibull(shape=1.12, scale=87600, location=0)
    #-------------------------------------------------
    # Initialize the simulation and run the simulation
    #-------------------------------------------------
    try: 
        logging.debug("===========SIMULATION PARAMETERS==============")
        logging.debug("Mission time: " + str(mission_time))
        logging.debug("Iterations per worker: " + str(iterations_per_worker))
        logging.debug("Traces per worker: " + str(traces_per_worker))
        logging.debug("Num disks: " + str(num_disks))
        logging.debug("Num disk per server: " + str(num_disks_per_server))
        logging.debug("k: " + str(k) + " m: " + str(m))
        logging.debug("Use trace: " + str(use_trace))
        logging.debug("Placetype: " + str(place_type))
        logging.debug("Trace dir: " + str(traceDir))
        logging.debug("Disk cap: " + str(diskCap))
        logging.debug("Rebuild rate: " + str(rebuildRate))
        logging.debug("Utilize ratio: " + str(utilizeRatio))
        logging.debug("Fail ratio: " + str(failRatio))
        logging.debug("===========SIMULATION PARAMETERS==============")
        
        for i in range(5):
            try:
                time.sleep(1);
            except KeyboardInterrupt:
                logging.debug("Keyboard interrupted")
                sys.exit(-1);

        sim = Simulate(mission_time, iterations_per_worker, traces_per_worker, num_disks, num_disks_per_server, 
                    k, m, use_trace, place_type, traceDir, diskCap, rebuildRate, utilizeRatio, failRatio)
        
        return sim.run_simulation(iterations_per_worker, traces_per_worker)
    except Exception:
        logging.exception(Exception)
        



def display_results(final_results):
    #-----------------------------------------
    # collect the reliability metrics 
    #-----------------------------------------
    for each in final_results:
        (x, y, z) = each
        logging.debug(x, y, z)
    


def setup_parameters():
    #-----------------------------------------------------------------------------------
    # add arguments from command line
    #-----------------------------------------------------------------------------------
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-T', type=int, help="mission time", default=43568)
    #-----------------------------------------------------------------------------------
    parser.add_argument('-N', type=int, help="#disk", default=18144)
    #parser.add_argument('-N', type=int, help="#disk", default=11000)
    #parser.add_argument('-D', type=int, help="#disks per server", default=110)
    #parser.add_argument('-D', type=int, help="#disks per server", default=168)
    parser.add_argument('-D', type=int, help="#disks per server", default=126)
    #parser.add_argument('-R', type=int, help="#racks", default=36)
    #parser.add_argument('-S', type=int, help="#servers per rack",default=6)
    #parser.add_argument('-D', type=int, help="#disks per server", default=84)
    #-----------------------------------------------------------------------------------
    parser.add_argument('-k', type=int, help="#data chunks",default=8)
    parser.add_argument('-m', type=int, help="# parity chunks",default=2)
    #-----------------------------------------------------------------------------------
    parser.set_defaults(trace=False)
    parser.add_argument('-trace', help="use real-world traces", dest="trace", action="store_true")
    parser.add_argument('-no-trace', help="use generated traces", dest="trace", action="store_false")
    #-----------------------------------------------------------------------------------
    parser.add_argument('-type', type=int, help="placement type",default=1)
    parser.add_argument('-traceDir', type=str, help="trace directory",default="../164-failure-traces/")
    parser.add_argument('-outputFile', type=str, help="output file",default="164models.txt")
    #parser.add_argument('-data', type=int, help="sys data size",default=10000)
    #-----------------------------------------------------------------------------------
    parser.add_argument('-diskCap', type=int, help="disk capacity", default=20*1024*1024)
    parser.add_argument('-rebuildRate', type=int, help="rebuild rate", default=50)
    parser.add_argument('-utilizeRatio', type=float, help="utilize ratio",default=1.0)
    parser.add_argument('-failRatio', type=float, help="percentage of disk failure", default=0.01)
    #-----------------------------------------------------------------------------------
    args = parser.parse_args()
    #-----------------------------------------------------------------------------------
    (mission_time, num_disks, num_disks_per_server, k, m, use_trace, place_type, traceDir, outputFile, diskCap, rebuildRate, utilizeRatio) = (args.T, args.N, args.D, args.k, args.m, args.trace, args.type, args.traceDir, args.outputFile, args.diskCap, args.rebuildRate, args.utilizeRatio)
    fail_ratio = args.failRatio;

    #-----------------------------------------------------------------------------------
    return (mission_time, num_disks, num_disks_per_server, k, m, use_trace, place_type, traceDir, outputFile, diskCap, rebuildRate, utilizeRatio, fail_ratio)



if __name__ == "__main__":
    #-------------------------------------------
    # set up the parameters for simulation
    #-------------------------------------------
    params_tuple = setup_parameters()
    
    log_name = "solsim-k" + str(params_tuple[3]) + "m" + str(params_tuple[4]) + ".log"
    logging.basicConfig(filename=log_name, level=logging.INFO)

    outputFile = params_tuple[8]
    modelfile = open(outputFile,'a')
    #--------------------------------------------
    # calculate iterations per thread worker
    #--------------------------------------------
    total_iterations = 64
    num_threads = 64
    if total_iterations % num_threads != 0:
        logging.debug("total iterations should be divided by number of threads")
        sys.exit(2)
    iterations_per_worker = [total_iterations / num_threads] * num_threads
    #--------------------------------------------
    # assign different traces for thread workers
    #--------------------------------------------
    traceDir = os.listdir(params_tuple[7])
    #num_traces = len(traceDir)
    num_traces = 21
    logging.info ("num_traces: " + str(num_traces));
    traces_per_worker = np.split(np.arange(start=1, stop=num_traces), num_threads)
    #traces_per_worker = np.split(np.arange(num_traces, num_traces+1), num_threads)
    #--------------------------------------------
    # assign different tasks for diff workers
    #--------------------------------------------
    tasks_per_worker = zip(iterations_per_worker, traces_per_worker)
    for workId in range(len(tasks_per_worker)):
        tasks_per_worker[workId] += params_tuple
    #--------------------------------------------
    # Start the simulations for different workers
    #--------------------------------------------
    pool = Pool(num_threads)
    results = pool.map(start, tasks_per_worker)
    pool.close()
    pool.join()
    #--------------------------------------------
    # collect the final results from diff workers
    #--------------------------------------------
    final_results = []
    prob_sum = 0
    events_sum = 0
    for each in results:
        logging.debug(">>>> each result ", each)
        for (traceId, prob, loss_events) in each:
            #print "(traceId, prob, lost_data)", traceId, prob, lost_data
            modelfile.write("%d, %f \n" % (traceId, prob))
            prob_sum += prob
            events_sum += loss_events
            #final_results += each
    logging.info(">>>>>>>>>>>>>>>> * final prob * " + str(float(prob_sum)/num_traces ))
    logging.info(">>>>>>>>>>>>>>>> * loss events * " + str(events_sum))
    modelfile.write("- %d, %f \n" % (num_traces, float(prob_sum)/num_traces))
    modelfile.close()
    #print ">>>>>>>>>>>>>>>> final results", final_results
    #display_results(final_results)
    #print ">>>>> end:", time.time()
    #--------------------------------------------
    

