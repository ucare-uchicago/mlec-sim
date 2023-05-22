# README

## Example

Example code to get the durability of the following setup:

Net-SLEC	DP	16+4	

IO: 250 MB/s

capacity: 25 TB	

Disk # per rack: 106

cross-rack bandwidth: inf

rack #: 20

total drives #: 2120

AFR: 1

detection time: 30

You'll need to run the following commands:

1. 

```
python main.py -k_net=16 -p_net=4 -total_drives=2120 -drives_per_rack=106 -io_speed=250 -cap=25 -afr=1 -placement=SLEC_NET_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=100000 -num_net_fail_to_report=2 -detection_time=30
```

It runs simulations manually.

`-concur=256` means running the simulation with 256 threads. You might need to modify this value to adapt to your own machine's thread #.

`-num_net_fail_to_report=2` means that, when there is a stripe with 2 failed chunks (priority 2), the simulation reports data loss.

You'll see output like this:

```
simulation time: 15.460009336471558
failed_iters: 90375  total_iters: 2560000
Nines   sigma   failed  total
1.452 0.001 90375 2560000
```

2. 
```
python main.py -k_net=16 -p_net=4 -total_drives=2120 -drives_per_rack=106 -io_speed=250 -cap=25 -afr=1 -placement=SLEC_NET_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=100000 -num_net_fail_to_report=3 -detection_time=30 -prev_fail_reports_filename=true
```

`-num_net_fail_to_report=3` means that, when there is a stripe with 3 failed chunks (priority 3), the simulation reports data loss.

`-prev_fail_reports_filename=true` means it's based on the simulation results of the previous step. So it's NOT manual simulations, but a splitting simulation which is based on all cases with a stripe having 2 failed chunks.

You'll get something like:

```
failed_iters: 1928  total_iters: 2560000
Nines   sigma   failed  total
3.123 0.01 1928 2560000
```

3. 
```
python main.py -k_net=16 -p_net=4 -total_drives=2120 -drives_per_rack=106 -io_speed=250 -cap=25 -afr=1 -placement=SLEC_NET_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=100000 -num_net_fail_to_report=4 -detection_time=30 -prev_fail_reports_filename=true
```

You'll get something like:
```
simulation time: 11.763625144958496
failed_iters: 1007  total_iters: 2560000
Nines   sigma   failed  total
3.405 0.014 1007 2560000 
```

4.
```
python main.py -k_net=16 -p_net=4 -total_drives=2120 -drives_per_rack=106 -io_speed=250 -cap=25 -afr=1 -placement=SLEC_NET_DP -concur=256 -iter=10000 -sim_mode=1 -repair_scheme=0 -interrack_speed=100000 -num_net_fail_to_report=5 -detection_time=30 -prev_fail_reports_filename=true
```

You'll get something like:

```
simulation time: 12.402836799621582
failed_iters: 758  total_iters: 2560000
Nines   sigma   failed  total
3.529 0.016 758 2560000
```

5.
For a 16+4 EC, when there exists a stripe with 5 failed chunks, it's data loss.

So the durability is the sum of all the nines reported above:

1.452+3.123+3.405+3.529=11.509
