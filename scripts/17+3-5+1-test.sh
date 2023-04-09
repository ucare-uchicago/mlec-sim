echo “local_cp 5+1”

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 5 -p_local 1  -total_drives 6 -drives_per_rack 6 -spool_size 6 -placement=SLEC_LOCAL_CP -concur=256 -iter=100000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=2 -detection_time=30 -repair_scheme=0

echo “mlec_c_c rs3”

python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 5 -p_local 1  -total_drives 57600 -drives_per_rack 960 -spool_size 6 -placement=MLEC_C_C -concur=256 -iter=100 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=1 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail

python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 5 -p_local 1  -total_drives 57600 -drives_per_rack 960 -spool_size 6 -placement=MLEC_C_C -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=2 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true

python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 5 -p_local 1  -total_drives 57600 -drives_per_rack 960 -spool_size 6 -placement=MLEC_C_C -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=3 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true

python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 5 -p_local 1  -total_drives 57600 -drives_per_rack 960 -spool_size 6 -placement=MLEC_C_C -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=4 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true
