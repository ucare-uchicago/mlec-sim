### DP_NET 
```
python main.py -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=10 -io_speed=100 -placement=DP_NET -concur=1 -epoch=1 -iter=1 -afr=60
```

### DP
```
python main.py -k_local=8 -p_local=2 -total_drives=50 -drives_per_rack=50 -io_speed=100 -placement=DP -concur=16 -epoch=16 -iter=1000 -afr=60
```

### Difference between DP and DP_NET
- Parallelism is going to be different due to disjoint rack placement
- System failure determination is going to be different
  - We use max failed disk per rack rather than total failed disk

### Parity Factorial Compensation
- PS: There was a c! (c being the number of parities) difference between the simulator vs the web app. It is related to subtlety of repair speed effectively scaling up with number of failures (this is how the simulation works). I amended the simulation results to remove that offset. Now the simulator and the web app report practically same answer. However, note that this is a small adjustment only, and it won't explain the gap between the numbers 