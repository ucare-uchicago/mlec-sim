python main.py -afr=1 -io_speed=40 -k_local=17 -p_local=3 -total_drives=120 -drives_per_rack=120 -placement=SLEC_LOCAL_DP -spool_size=120 -iter=5000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=2 -detection_time=30
python main.py -afr=1 -io_speed=40 -k_local=17 -p_local=3 -total_drives=120 -drives_per_rack=120 -placement=SLEC_LOCAL_DP -spool_size=120 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_DP_2f_rs0.log -detection_time=30
python main.py -afr=1 -io_speed=40 -k_local=17 -p_local=3 -total_drives=120 -drives_per_rack=120 -placement=SLEC_LOCAL_DP -spool_size=120 -iter=50000 -concur=200 -sim_mode=1 -repair_scheme=0 -num_local_fail_to_report=4 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_DP_3f_rs0.log -detection_time=30