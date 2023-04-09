echo “local_dp 10+2”

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=100000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=2 -detection_time=30 -repair_scheme=0

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=1000000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=3 -detection_time=30 -repair_scheme=0 -prev_fail_reports_filename=true


echo “mlec_c_d rs3”

python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 10 -p_local 2  -total_drives 57600 -drives_per_rack 960 -spool_size 120 -placement=MLEC_C_D -concur=256 -iter=100 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=1 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail


echo “local_dp 10+2”
python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=100000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=2 -detection_time=30 -repair_scheme=0

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=1000000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=3 -detection_time=30 -repair_scheme=0 -prev_fail_reports_filename=true


echo “mlec_c_d rs3”
python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 10 -p_local 2  -total_drives 57600 -drives_per_rack 960 -spool_size 120 -placement=MLEC_C_D -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=2 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true



echo “local_dp 10+2”
python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=100000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=2 -detection_time=30 -repair_scheme=0

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=1000000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=3 -detection_time=30 -repair_scheme=0 -prev_fail_reports_filename=true


echo “mlec_c_d rs3”
python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 10 -p_local 2  -total_drives 57600 -drives_per_rack 960 -spool_size 120 -placement=MLEC_C_D -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=3 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true


echo “local_dp 10+2”
python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=100000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=2 -detection_time=30 -repair_scheme=0

python main.py -io_speed 40 -cap 20 -k_net 5 -p_net 1 -k_local 10 -p_local 2  -total_drives 120 -drives_per_rack 120 -spool_size 120 -placement=SLEC_LOCAL_DP -concur=256 -iter=1000000 -afr=1 -sim_mode=1 -interrack_speed=20000 -num_net_fail_to_report=0 -num_local_fail_to_report=3 -detection_time=30 -repair_scheme=0 -prev_fail_reports_filename=true


echo “mlec_c_d rs3”
python main.py -io_speed 40 -cap 20 -k_net 17 -p_net 3 -k_local 10 -p_local 2  -total_drives 57600 -drives_per_rack 960 -spool_size 120 -placement=MLEC_C_D -concur=256 -iter=10000 -afr=1 -sim_mode=1 -interrack_speed=2 -num_net_fail_to_report=4 -num_local_fail_to_report=0 -detection_time=30 -repair_scheme=3 --manual_spool_fail -prev_fail_reports_filename=true
