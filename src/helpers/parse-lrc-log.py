import sys
import json
import math
from decimal import *
from lrc_decode import compute_decode_recoverability

getcontext().prec = 100

if len(sys.argv) < 3+1:
    print("python parse-lrc-log.py [k] [l] [r]")
k = int(sys.argv[1])
l = int(sys.argv[2])
assert k%l==0, "not divisible"
local_k = k // l
r = int(sys.argv[3])
danger_fail_priority = r+2
must_fail_priority = r+3
chunksize = 128/1024   # 128KB to MB
diskcap = 20*1024*1024 # 20TB to MB

recoverability = compute_decode_recoverability(k, l, r, danger_fail_priority, True, 100000)
print("recoverability: {}".format(recoverability))
danger_fail_filename = '../fail_reports_{}+{}-{}+1_LRC_DP_{}f_rs0.log'.format(
                                k, r, local_k, danger_fail_priority)
with open(danger_fail_filename, 'r') as f:
    danger_fail_reports = json.load(f)

survival_probs_danger = []
for report in danger_fail_reports:
    for disk_info in report['disk_infos']:
        if int(disk_info['priority']) == danger_fail_priority:
            priority_percents = json.loads(disk_info['priority_percents'])
            fail_percent = Decimal(priority_percents[str(danger_fail_priority)])
            # print(fail_percent)
            stripe_fail_prob = fail_percent * Decimal(1-recoverability)
            # print(stripe_fail_prob)
            survival_prob = (1-stripe_fail_prob)**Decimal(diskcap//chunksize)
            survival_probs_danger.append(survival_prob)

avg_survival_prob = sum(survival_probs_danger) / len(survival_probs_danger)
print(avg_survival_prob)
nines = -math.log10(1-avg_survival_prob)
print('danger priority {}  nines {:.3f}'.format(danger_fail_priority, nines))



must_fail_filename = '../fail_reports_{}+{}-{}+1_LRC_DP_{}f_rs0.log'.format(
                                k, r, local_k, must_fail_priority)
with open(must_fail_filename, 'r') as f:
    must_fail_reports = json.load(f)

survival_probs_must = []
for report in must_fail_reports:
    for disk_info in report['disk_infos']:
        if int(disk_info['priority']) == must_fail_priority:
            priority_percents = json.loads(disk_info['priority_percents'])
            fail_percent = Decimal(priority_percents[str(must_fail_priority)])
            stripe_fail_prob = fail_percent
            survival_prob = (1-stripe_fail_prob)**Decimal(diskcap//chunksize)
            survival_probs_must.append(survival_prob)

avg_survival_prob = sum(survival_probs_must) / len(survival_probs_must)
print(avg_survival_prob)
nines = -math.log10(1-avg_survival_prob)
print('must priority {}  nines {:.3f}'.format(must_fail_priority, nines))
