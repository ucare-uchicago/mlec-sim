python main.py -afr=1 -k_net=5 -p_net=2 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=2
python main.py -afr=1 -k_net=5 -p_net=2 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=3 -prev_fail_reports_filename=fail_reports_5+2-1+0_SLEC_NET_CP_2f_rs0.log
# python main.py -afr=1 -k_net=5 -p_net=2 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=4 -prev_fail_reports_filename=fail_reports_14+6-1+0_SLEC_NET_CP_3f_rs0.log
# python main.py -afr=1 -k_net=5 -p_net=2 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=5 -prev_fail_reports_filename=fail_reports_14+6-1+0_SLEC_NET_CP_4f_rs0.log
# python main.py -afr=1 -k_net=14 -p_net=6 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=6 -prev_fail_reports_filename=fail_reports_14+6-1+0_SLEC_NET_CP_5f_rs0.log
# python main.py -afr=1 -k_net=14 -p_net=6 -total_drives=57600 -drives_per_rack=960 -io_speed=40 -placement=SLEC_NET_CP -concur=256 -iter=100000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -detection_time=0 -num_net_fail_to_report=7 -prev_fail_reports_filename=fail_reports_14+6-1+0_SLEC_NET_CP_6f_rs0.log



python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=100 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2

python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-14+6_SLEC_LOCAL_DP_2f_rs0.log


python main.py -afr=5 -io_speed=40 -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=5000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2
python main.py -afr=5 -io_speed=40 -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-8+2_SLEC_LOCAL_SODP_2f_rs0.log

python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=4


python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_DP -spool_size=50 -iter=5000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2
python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_DP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_DP_2f_rs0.log
python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_DP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=4 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_DP_3f_rs0.log



python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=500 
-concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2
python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_SODP_2f_rs0.log
python main.py -afr=5 -io_speed=40 -k_local=7 -p_local=3 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=4 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_SODP_3f_rs0.log




python main.py -afr=5 -io_speed=40 -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=500 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2 -detection_time=30
python main.py -afr=5 -io_speed=40 -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=50 -placement=SLEC_LOCAL_SODP -spool_size=50 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-8+2_SLEC_LOCAL_SODP_2f_rs0.log -detection_time=30
