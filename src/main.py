import logging
import argparse

from simulators.burst_sim import BurstSim
from simulators.metric_sim import MetricSim
from simulators.io_over_year_sim import IoOverYearSim
from simulators.normal_sim import NormalSim
from simulators.weibull_sim import WeibullSim
from simulators.manual_1_rack_fail_sim import ManualFailOneRackSim
from simulators.manual_2_rack_fail_sim import ManualFailTwoRackSim


if __name__ == "__main__":
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description='Parse simulator configurations.')
    parser.add_argument('-sim_mode', type=int, help="simulation mode. Default is 0", default=0)
    parser.add_argument('-afr', type=int, help="disk annual failure rate.", default=5)
    parser.add_argument('-io_speed', type=int, help="disk repair rate (MB/s).", default=30)
    parser.add_argument('-intrarack_speed', type=int, help="Intra-rack speed (Gb/s).", default=100)
    parser.add_argument('-interrack_speed', type=int, help="Inter-rack speed (Gb/s).", default=10)
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
    parser.add_argument('-epoch', type=int, help="how many epochs to run", default=200)
    parser.add_argument('-iter', type=int, help="how many iterations in a epoch in a thread to run", default=50000)
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
    epoch = args.epoch
    iters = args.iter

    total_drives = args.total_drives
    if total_drives == -1:
        total_drives = (k_local+p_local) * (k_net+p_net)


    drives_per_rack = args.drives_per_rack
    if drives_per_rack == -1:
        drives_per_rack=k_local+p_local
    
    placement = args.placement
    if placement in ['RAID', 'DP']:
        k_net = 1
        p_net = 0
        
    
    if placement in ['RAID_NET']:
        k_local = 1
        p_local = 0

    dist = args.dist

    if sim_mode == 0:
        NormalSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, distribution=dist, concur=concur, epoch=epoch, iters=iters)
    elif sim_mode == 1:
        ManualFailOneRackSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    elif sim_mode == 2:
        ManualFailTwoRackSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    elif sim_mode == 3:
        IoOverYearSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    elif sim_mode == 4:
        WeibullSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    elif sim_mode == 5:
        MetricSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)
    elif sim_mode == 6:
        BurstSim().simulate(afr=afr, io_speed=io_speed, intrarack_speed=intrarack_speed, interrack_speed=interrack_speed,
                   cap=cap, adapt=adapt, k_local=k_local, p_local=p_local, k_net=k_net, p_net=p_net,
                   total_drives=total_drives, drives_per_rack=drives_per_rack, placement=placement, concur=concur, epoch=epoch, iters=iters, distribution=dist)


    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_afr=50&d_cap=20&dr_rw_speed=50&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&spares=0&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&ec_mode=3&d_afr=2&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&adapt_mode=2&nhost_per_chass=1&ndrv_per_dg=50&tnhost_per_chass=8&tndrv_per_dg=8&ph_nspares=0&tnchassis=1&tnrack=1&rec_wr_spd_alloc=100
    # tetraquark.shinyapps.io:/erasure_coded_storage_calculator_pub/?tab=results&d_cap=20&dr_rw_speed=30&ndatashards=7&nredundancy=1&nhost_per_chass=1&ndrv_per_dg=50&spares=0&woa_nhost_per_chass=1&rec_wr_spd_alloc=100