# MLEC-Sim

A simulator to simulate how different erasure coding approaches behaves against disk failures in storage systems
and analyze their performance including durability, rebuild IO, network traffic, availability etc.

## High level logic
1. Generate failure times (following a specific distribution e.g. exponential distribution) for all the drives in the system
2. Select the failure times that are less than the mission time (e.g. one year).
3. For each failed drives
   1. Mark all the drives that should be repaired by now as repaired
   2. Calculate the repair time for drive.
   3. Check if the drive failure causes the server to fail. If so, fail the server.
      1. If the server fails, calculate the repair time for the server
      2. Check if the server failures causes the system to fail. If so, fail the system, return fail.
   4. Generate new failures for those drives that have been repaired
4. If the system survives after the mission time, return success.
5. Repeat process 1-4 for M times, get N failures and M-N successes. The the probability of data is N/M.

## Components
- `main.py` - the main simulation loop and parallelization
- `constants.py` - toggle on global debug print

## Prerequisites
- Install `conda`: https://www.anaconda.com
- Using conda to install the following components
   - `numpy`
   - `matplotlib`
   - `math`

## Usage
```
usage: main.py [-h] [-sim_mode SIM_MODE] [-afr AFR] [-io_speed IO_SPEED] [-cap CAP] [-adapt ADAPT] [-n_local N_LOCAL] [-k_local K_LOCAL]
               [-n_net N_NET] [-k_net K_NET] [-total_drives TOTAL_DRIVES] [-drives_per_server DRIVES_PER_SERVER] [-placement PLACEMENT]
               [-dist DIST]

Parse simulator configurations.

optional arguments:
  -h, --help            show this help message and exit
  -sim_mode SIM_MODE    simulation mode
  -afr AFR              disk annual failure rate.
  -io_speed IO_SPEED    disk repair rate.
  -cap CAP              disk capacity (TB)
  -adapt ADAPT          assume seagate adapt or not
  -n_local N_LOCAL      number of data chunks in local EC
  -k_local K_LOCAL      number of parity chunks in local EC
  -n_net N_NET          number of data chunks in network EC
  -k_net K_NET          number of parity chunks in network EC
  -total_drives TOTAL_DRIVES
                        number of total drives in the system
  -drives_per_server DRIVES_PER_SERVER
                        number of drives per server
  -placement PLACEMENT  placement policy. Can be RAID/DP/MLEC/LRC
  -dist DIST            disk failure distribution. Can be exp/weibull
```

## Example
- python main.py -h
- python main.py -n_local=8 -k_local=2 -total_drives=50 -drives_per_server=50 -io_speed=100 -placement=RAID
- The simulation result would be in the file `s-result-<mode>.log` file with the following format
   - <data_shard>-<parity_shard>-<afr_in_percent>: <number_of_nines>