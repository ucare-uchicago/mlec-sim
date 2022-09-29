

# burst raid
python main.py -sim_mode 6 -afr 2 -io_speed 100 -cap 20 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=RAID

# burst dp
python main.py -sim_mode 6 -afr 2 -io_speed 100 -cap 20 -k_local 8 -p_local 2 -total_drives 2500 -drives_per_rack 50 -placement=DP

# burst raid net
python main.py -sim_mode 6 -afr 2 -io_speed 100 -cap 20 -k_net 8 -p_net 2 -total_drives 2500 -drives_per_rack 50 -placement=RAID_NET