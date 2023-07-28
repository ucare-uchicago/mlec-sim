# MLEC-Sim

A simulator to simulate how different erasure coding approaches behaves against disk failures in storage systems
and analyze their performance including durability, rebuild IO, network traffic, availability etc.

An artifact for our paper is provided in Chameleon Trovi at https://chameleoncloud.org/experiment/share/6bec6d21-a9d3-42bc-ab2a-72f7bad62acc 

## High level logic
1. Generate failure times (following a specific distribution e.g. exponential distribution) for all the drives in the system
2. Select the failure times that are less than the mission time (e.g. one year).
3. For each failed drives
   1. Mark all the drives that should be repaired by now as repaired
   2. Calculate the repair time for drive.
   3. Check if the drive failure causes the rack to fail. If so, fail the rack.
      1. If the rack fails, calculate the repair time for the rack
      2. Check if the rack failures causes the system to fail. If so, fail the system, return fail.
   4. Generate new failures for those drives that have been repaired
4. If the system survives after the mission time, return success.
5. Repeat process 1-4 for M times, get N failures and M-N successes. The the probability of data is N/M.

## Components
- `main.py` - the main simulation loop and parallelization
- `simulation.py`

## Prerequisites
- set up github ssh key
```
ssh-keygen -t rsa -b 4096
```

Then
```
ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
cat ~/.ssh/id_rsa.pub
```
Copy and paste into: https://github.com/settings/keys
- Install `conda`:
```
wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh
bash Anaconda3-2023.03-1-Linux-x86_64.sh
```

When prompted, enter "yes".

After it's installed, close the terminal. And then open a new terminal. You should see your terminal outputs:

```
(base) cc@ubuntu:~$
```



- Using conda to install the required components
```
conda install -y numpy matplotlib mpmath pandas
```

- Download this repo to the node that you have reserved:
```
git clone git@github.com:ucare-uchicago/mlec-sim.git
```


## Usage
```
usage: main.py [-h] [-sim_mode SIM_MODE] [-afr AFR] [-io_speed IO_SPEED] [-cap CAP] [-adapt ADAPT] [-k_local N_LOCAL] [-p_local K_LOCAL]
               [-k_net N_NET] [-p_net K_NET] [-total_drives TOTAL_DRIVES] [-drives_per_rack DRIVES_PER_SERVER] [-placement PLACEMENT]
               [-dist DIST]

Parse simulator configurations.

optional arguments:
  -h, --help            show this help message and exit
  -sim_mode SIM_MODE    simulation mode
  -afr AFR              disk annual failure rate.
  -io_speed IO_SPEED    disk repair rate.
  -cap CAP              disk capacity (TB)
  -adapt ADAPT          assume seagate adapt or not
  -k_local N_LOCAL      number of data chunks in local EC
  -p_local K_LOCAL      number of parity chunks in local EC
  -k_net N_NET          number of data chunks in network EC
  -p_net K_NET          number of parity chunks in network EC
  -total_drives TOTAL_DRIVES
                        number of total drives in the system
  -drives_per_rack DRIVES_PER_SERVER
                        number of drives per rack
  -placement PLACEMENT  placement policy. Can be RAID/DP/MLEC/LRC/DP_NET/RAID_NET
  -dist DIST            disk failure distribution. Can be exp/weibull
```

## Example

### Normal simulation
- `python main.py -h`
- `python main.py -k_local=8 -p_local=2 -total_drives=100 -drives_per_rack=50 -io_speed=100 -placement=SLEC_LOCAL_CP -concur=256 -iter=10000 -sim_mode=0`
- The simulation result would be in the file `s-result-<mode>.log` file with the following format
   - `(kn:<k_net>+pn:<p_net>)(kl:<k_local>+pl:<p_local>) td:<total_drives> afr:<afr> cap:<drive_cap> io:<drive_io> ibw:<intrarack_bw> cbw:<crossrack_bw> nn:<num_of_nines> sd:<stddev> f:<failed_iters> t:<total_iters> ad:<adapt>`
   - A parser script that takes a result file as input and return a parsed dictionary is provided in `src/parse.py`

### Manual Failure Injection

Sometimes the durability is too high to simulate. For example, if the durability is 7 nines, you will need to run 10 million iterations to get only 
one system failure. 

Therefore, we introduce manual failure injection. Here is an example of how it works.

For example, you want to simulate the durability of local SLEC 7+3 with clustered placement (i.e. RAID) in a system with 100 disks.

You should use `sim_mode=1` and can do it in 3 stages:

1. Stage 1: Simulate the probability that the system can have 2 concurrent disk failures in a pool:

`python main.py -k_local=7 -p_local=3 -total_drives=100 -drives_per_rack=50 -io_speed=100 -placement=SLEC_LOCAL_CP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=1 -num_local_fail_to_report=2`

You might see output like:

```
simulation time: 8.614731550216675
failed_iters: 39522  total_iters: 2560000
Num of Nine: 1.811
error sigma: 0.002
average aggregate down time: 0.0
avail_nines:NA
Num of Nine: 1.811
error sigma: 0.002
```

It means the probability for the system to have 2 concurrent disk failures in a pool over one year is 39522/2560000, which means 1.811 nines.

When the 2 concurrent failures happen, the simulate records the current system state. The system states of all the cases are written into the 
log file: `fail_reports_1+0-7+3_SLEC_LOCAL_CP_2f_rs1.log`.

2. Stage 2: Given the system has 2 concurrent disk failures in a pool, simulate the probability that the system can have 3 concurrent disk failures in a pool.

`python main.py -k_local=7 -p_local=3 -total_drives=100 -drives_per_rack=50 -io_speed=100 -placement=SLEC_LOCAL_CP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=1 -num_local_fail_to_report=3 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_CP_2f_rs1.log`

The simulator will always start from a random system state from `fail_reports_1+0-7+3_SLEC_LOCAL_CP_2f_rs1.log`, and continue the simulation.

You probably would see the output like:

```
simulation time: 9.35881495475769
failed_iters: 6840  total_iters: 2560000
Num of Nine: 2.573
error sigma: 0.005
average aggregate down time: 0.0
avail_nines:NA
Num of Nine: 2.573
error sigma: 0.005
```

It means given 2 concurrent disk failures in a pool, the probability that the system have 3 concurrent disk failures in a pool over one year is
6840/2560000, which means 2.573 nines.

When the 3 concurrent failures happen, the simulate records the current system state. The system states of all the cases are written into the 
log file: `fail_reports_1+0-7+3_SLEC_LOCAL_CP_3f_rs1.log`.

3. State 3: Given the system has 3 concurrent disk failures in a pool, simulate the probability that the system can have 4 concurrent disk failures in a pool.
Note that when the system has 4 concurrent disk failures in a pool, the system unrecoverably loses data.

`python main.py -k_local=7 -p_local=3 -total_drives=100 -drives_per_rack=50 -io_speed=100 -placement=SLEC_LOCAL_CP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=1 -num_local_fail_to_report=4 -prev_fail_reports_filename=fail_reports_1+0-7+3_SLEC_LOCAL_CP_3f_rs1.log`

The simulator will always start from a random system state from `fail_reports_1+0-7+3_SLEC_LOCAL_CP_3f_rs1.log`, and continue the simulation.

You probably would see the output like:

```
simulation time: 8.115314960479736
failed_iters: 5945  total_iters: 2560000
Num of Nine: 2.634
error sigma: 0.006
average aggregate down time: 0.0
avail_nines:NA
Num of Nine: 2.634
error sigma: 0.006
```

It means given 3 concurrent disk failures in a pool, the probability that the system have 4 concurrent disk failures in a pool over one year is
5945/2560000, which means 2.634 nines.

4. Put all together: Therefore, the durability of the 7+3 system is 1.811+2.573+2.634=7.018 nines, with the standard deviation as 
sqrt(0.002^2+0.005^2+0.006^2)=0.008.

Congratulations! You managed to simulate 7 nines in 30 seconds!

You can apply the same method to simulate the system with more parities.



## Variable namings:
spool: slec disk pool
mpool: mlec disk pool
