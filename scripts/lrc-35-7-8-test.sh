echo "(35,7,8) lrc"

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=100 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=2 -detection_time=30

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=100 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=3 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=1000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=4 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=2000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=5 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=2000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=6 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=4000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=7 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=4000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=8 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=9 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=10 -detection_time=30 -prev_fail_reports_filename=true

python main.py -k_local=5 -k_net=35 -p_net=8 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=LRC_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=2 -num_net_fail_to_report=11 -detection_time=30 -prev_fail_reports_filename=true
