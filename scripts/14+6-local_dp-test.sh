python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=100 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2

python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-14+6_SLEC_LOCAL_DP_2f_rs0.log

python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=4 -prev_fail_reports_filename=fail_reports_1+0-14+6_SLEC_LOCAL_DP_3f_rs0.log


python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=5 -prev_fail_reports_filename=fail_reports_1+0-14+6_SLEC_LOCAL_DP_4f_rs0.log

python main.py -k_local=14 -p_local=6 -total_drives=57600 -drives_per_rack=960 -spool_size=120 -io_speed=40 -afr=1 -placement=SLEC_LOCAL_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=6 -prev_fail_reports_filename=fail_reports_1+0-14+6_SLEC_LOCAL_DP_5f_rs0.log
