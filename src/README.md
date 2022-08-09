# Process
1. Generate failure times for all the drives in the system
2. Select the failure times that are less than one year
3. for-each failed drives
   1. Mark all the drives that should be repaired by now as repaired
   2. Generate new failures for those drives that have been repaired
   3. Check whether there are more drives in repair than parity, if so, fail the system
   4. for-each priority level (stripes that contain x number of failed drives)
      1. Fix that failed disk contained in the stripe

# Components
- `main.py` - the main simulation loop and parallelization
- `constants.py` - toggle on global debug print
- `drive_args.py` - class responsible for storing drive related information
- `sys_state.py` - class responsible for storing whole system state (good drives, failed drives, etc.)
- `repair_queue.py` - a wrapper around python heapq that is responsible for keeping track of disk repairs and schedule overlapping repair in FIFO manner.

# Setting up the simulator
- Using conda to install the following components
   - `numpy`
   - `matplotlib`
   - `math`

# Configuring the simulator
All the simulator configs are located after line 178 of `main.py`. There are the following things to configure for
- **simulate()**: the parallelization wrapper for simulating. Modify this to adjust to your computer
   - **iter**: how many independent simulations(from system generation to system failure or 1 year) are ran by a single thread
   - **epoch**: how many sequential threads we spawn to parallelize simulation. We aggregate the simulations ran and number of failures at the end so total simulations ran would be iter*epoch
   - **concur**: the size of the thread pool working on the epochs. Usually this is the cpu core count of your computer.
Also, the debug printing toggle is in `constants.py`. Please make sure to disable debug print when running actual simulation. Otherwise you might get a log file of dozen gigabytes.

# Running the simulator
- python main.py -h
- python main.py -n_local=8 -k_local=2 -total_drives=50 -drives_per_server=50 -io_speed=100 -placement=RAID
- The simulation result would be in the file `s-result-<mode>.log` file with the following format
   - <data_shard>-<parity_shard>-<afr_in_percent>: <number_of_nines>