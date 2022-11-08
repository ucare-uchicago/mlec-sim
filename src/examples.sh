

# burst raid
python main.py -sim_mode 6 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=RAID
python burst.py -k_local 6 -p_local 1 -total_drives 21 -drives_per_rack 7 -placement=RAID
python burst.py -k_local 6 -p_local 2 -total_drives 24 -drives_per_rack 8 -placement=RAID
python burstBrute.py -k_net 3 -p_net 0 -k_local 6 -p_local 1

# burst dp
python main.py -sim_mode 6 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=DP

# burst raid net
python main.py -sim_mode 6 -k_net 8 -p_net 2 -total_drives 2500 -drives_per_rack 50 -placement=RAID_NET
python burst_ornl.py -k_net 8 -p_net 2 -total_drives 32000 -drives_per_rack 100 -placement=RAID_NET

# burst mlec dp
python main.py -sim_mode 6 -k_net 16 -p_net 2 -k_local 8 -p_local 2 -total_drives 2700 -drives_per_rack 50 -placement=MLEC_DP
python main.py -sim_mode 6 -k_net 8 -p_net 2 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=MLEC_DP
python burst.py -k_net 8 -p_net 2 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=MLEC_DP

# burst mlec
python main.py -sim_mode 6 -io_speed 100 -cap 20 -k_net 8 -p_net 2 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=MLEC
python burst.py -k_net 2 -p_net 1 -k_local 6 -p_local 1 -placement=MLEC
python burst.py -k_net 2 -p_net 1 -k_local 6 -p_local 2 -placement=MLEC
python burstBrute.py -k_net 2 -p_net 1 -k_local 6 -p_local 1
python burst.py -k_net 8 -p_net 2 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=MLEC
python burst.py -k_net 8 -p_net 2 -k_local 8 -p_local 2 -total_drives 400 -drives_per_rack 20 -placement=MLEC