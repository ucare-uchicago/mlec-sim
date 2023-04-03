import sys
import json
import math
from decimal import *

getcontext().prec = 100

if len(sys.argv) < 2+1:
    print("python parse-netdp-log.py [k_net] [p_net]")
k = int(sys.argv[1])
p = int(sys.argv[2])
priority = p+1
chunksize = 128/1024   # 128KB to MB
diskcap = 20*1024*1024 # 20TB to MB

filename = '../fail_reports_{}+{}-1+0_SLEC_NET_DP_{}f_rs0.log'.format(
                                k, p, priority)
with open(filename, 'r') as f:
    reports = json.load(f)

survival_probs = []
for report in reports:
    for disk_info in report['disk_infos']:
        if int(disk_info['priority']) == priority:
            priority_percents = json.loads(disk_info['priority_percents'])
            fail_percent = Decimal(priority_percents[str(priority)])
            survival_prob = (1-fail_percent)**Decimal(diskcap//chunksize)
            survival_probs.append(survival_prob)

avg_survival_prob = sum(survival_probs) / len(survival_probs)
nines = -math.log10(1-avg_survival_prob)
print('{:.3f}'.format(nines))