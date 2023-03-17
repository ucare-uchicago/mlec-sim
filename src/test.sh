python main.py -io_speed 10 -cap 100 -k_net 8 -p_net 2 -k_local 8 -p_local 2  -total_drives 100 -drives_per_rack 10 -spool_size 10 -placement=MLEC_C_C -concur=256 -iter=1000 -afr=5 -sim_mode=1 -interrack_speed=10000 -num_net_fail_to_report=1 -num_local_fail_to_report=0 -repair_scheme=1

python main.py -io_speed 10 -cap 100 -k_net 8 -p_net 2 -k_local 8 -p_local 2  -total_drives 100 -drives_per_rack 10 -spool_size 10 -placement=MLEC_C_C -concur=256 -iter=1000 -afr=5 -sim_mode=1 -interrack_speed=10000 -num_net_fail_to_report=1 -num_local_fail_to_report=2 -prev_fail_reports_filename=fail_reports_8+2-8+2_MLEC_C_C_1f0f_rs1.log -repair_scheme=1

python main.py -io_speed 10 -cap 100 -k_net 8 -p_net 2 -k_local 8 -p_local 2  -total_drives 100 -drives_per_rack 10 -spool_size 10 -placement=MLEC_C_C -concur=256 -iter=1000 -afr=5 -sim_mode=1 -interrack_speed=10000 -num_net_fail_to_report=2 -num_local_fail_to_report=0 -prev_fail_reports_filename=fail_reports_8+2-8+2_MLEC_C_C_1f2f_rs1.log -repair_scheme=1

python main.py -io_speed 10 -cap 100 -k_net 8 -p_net 2 -k_local 8 -p_local 2  -total_drives 100 -drives_per_rack 10 -spool_size 10 -placement=MLEC_C_C -concur=256 -iter=1000 -afr=5 -sim_mode=1 -interrack_speed=10000 -num_net_fail_to_report=2 -num_local_fail_to_report=2 -prev_fail_reports_filename=fail_reports_8+2-8+2_MLEC_C_C_2f0f_rs1.log -repair_scheme=1

python main.py -io_speed 10 -cap 100 -k_net 8 -p_net 2 -k_local 8 -p_local 2  -total_drives 100 -drives_per_rack 10 -spool_size 10 -placement=MLEC_C_C -concur=256 -iter=1000 -afr=5 -sim_mode=1 -interrack_speed=10000 -num_net_fail_to_report=3 -num_local_fail_to_report=0 -prev_fail_reports_filename=fail_reports_8+2-8+2_MLEC_C_C_2f2f_rs1.log -repair_scheme=1
