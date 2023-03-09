import logging
import math
import argparse

from constants.PlacementType import parse_placement

from simulators.normal_sim import NormalSim
from simulators.manual_fail_sim import ManualFailSim



if __name__ == "__main__":
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-sim_mode', type=int, help="simulation mode. Default is 0", default=0)
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate (MB/s).", default=30)
    parser.add_argument('-intrarack_speed', type=float, help="Intra-rack speed (Gb/s).", default=100)
    parser.add_argument('-interrack_speed', type=float, help="Inter-rack speed (Gb/s).", default=10)
    parser.add_argument('-cap', type=int, help="disk capacity (TB)", default=20)
    parser.add_argument('-adapt', type=bool, help="assume seagate adapt or not", default=False)
    parser.add_argument('-k_local', type=int, help="number of data chunks in local EC", default=7)
    parser.add_argument('-p_local', type=int, help="number of parity chunks in local EC", default=1)
    parser.add_argument('-k_net', type=int, help="number of data chunks in network EC", default=7)
    parser.add_argument('-p_net', type=int, help="number of parity chunks in network EC", default=1)
    parser.add_argument('-total_drives', type=int, help="number of total drives in the system", default=-1)
    parser.add_argument('-drives_per_rack', type=int, help="number of drives per rack", default=-1)
    parser.add_argument('-placement', type=str, help="placement policy. Can be RAID/DP/MLEC/LRC", default='MLEC')
    parser.add_argument('-dist', type=str, help="disk failure distribution. Can be exp/weibull", default='exp')
    parser.add_argument('-concur', type=int, help="how many threads to use concurrently", default=200)
    # parser.add_argument('-epoch', type=int, help="how many epochs to run", default=200)
    parser.add_argument('-iter', type=int, help="how many iterations in a epoch in a thread to run", default=500)
    parser.add_argument('-metric', type=bool, help="Output metric line below result line in result file", default=False)
    parser.add_argument('-infinite_chunks', type=int, help="whether assume a disk has infinite chunks. Default is 1 which means true", default=1)
    parser.add_argument('-chunksize', type=int, help="disk chunk size in KB.", default=128)
    parser.add_argument('-spool_size', type=int, help="number of disks in a slec dp pool.", default=120)
    parser.add_argument('-repair_scheme', type=int, help="catastrophic repair scheme.", default=0)
    args = parser.parse_args()

    sim_mode = args.sim_mode

    afr = args.afr
    io_speed = args.io_speed
    cap = args.cap
    adapt = args.adapt
    k_local = args.k_local
    p_local = args.p_local
    k_net = args.k_net
    p_net = args.p_net
    
    intrarack_speed = args.intrarack_speed
    interrack_speed = args.interrack_speed
    
    # Multi-threading stuff
    concur = args.concur
    # epoch = args.epoch
    epoch = concur
    iters = args.iter

    total_drives = args.total_drives
    if total_drives == -1:
        total_drives = (k_local+p_local) * (k_net+p_net)


    drives_per_rack = args.drives_per_rack
    if drives_per_rack == -1:
        drives_per_rack=k_local+p_local
    
    placement = parse_placement(args.placement)
    if placement in ['RAID', 'DP']:
        k_net = 1
        p_net = 0
        
    
    if placement in ['RAID_NET']:
        k_local = 1
        p_local = 0

    dist = args.dist
    metric = args.metric

    infinite_chunks = (args.infinite_chunks != 0)
    chunksize = args.chunksize
    spool_size = args.spool_size
    repair_scheme = args.repair_scheme

    if sim_mode == 0:
        result = NormalSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, distribution=dist, concur=concur, epoch=epoch, iters=iters,
                   infinite_chunks=infinite_chunks, chunksize=chunksize, spool_size=spool_size, repair_scheme=repair_scheme)
    elif sim_mode == 1:
        result = ManualFailSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, distribution=dist, concur=concur, epoch=epoch, iters=iters,
                   infinite_chunks=infinite_chunks, chunksize=chunksize, spool_size=spool_size, repair_scheme=repair_scheme)
    # elif sim_mode == 2:
    #     result = ManualFailTwoRackSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
    #                cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
    #                total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    # elif sim_mode == 3:
    #     result = IoOverYearSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
    #                cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
    #                total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    # elif sim_mode == 4:
    #     result = WeibullSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
    #                cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
    #                total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    # elif sim_mode == 5:
    #     result = MetricSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
    #                cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
    #                total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    # elif sim_mode == 6:
    #     result = BurstSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
    #                cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
    #                total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    else:
        raise NotImplementedError("Simulation mode not recognized")
    
    if result is not None:
        # nn = str(round(-math.log10(res[0]/res[1]),2) - math.log10(factorial(l1args.parity_shards)))
        nines = "NA" if result.failed_iter == 0 else str(round(-math.log10(result.failed_iter/result.total_iter),3))
        sigma = "NA" if result.failed_iter == 0 else str(round(1/(math.log(10) * (result.failed_iter**0.5)),3))
        print("Num of Nine: " + nines)
        print("error sigma: " + sigma)
        
        output = open("s-result-{}.log".format(placement), "a")
        output.write("(kn:{}+pn:{})(kl:{}+pl:{}) td:{} afr:{} cap:{} io:{} ibw:{} cbw:{} nn:{} sd:{} f:{} t:{} ad:{}\n".format(
            k_net, p_net, k_local, p_local, total_drives,
            afr, cap, io_speed, intrarack_speed, interrack_speed, nines, sigma, result.failed_iter, result.total_iter, "adapt" if adapt else "notadapt"))
        output.close()
        
        if metric:
            metric_output = open("s-metric-{}.log".format(placement), "a")
            metric_output.write(result.metrics.single_line())
            metric_output.close()
        
        
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100