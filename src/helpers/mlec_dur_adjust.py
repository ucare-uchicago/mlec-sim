# For MLEC_D_D, when there are p+1 catastrophic local failures, there isn't necessarily data loss.
# This is because every pool only has a small number of catastrophic local stripes.
# And these catastrophic local stripes don't necessarily reside on the same network stripe.
# So here we compute the probability that some network stripe has p+1 catastrophic stripes.
# And we use this probability to adjust the durability for MLEC_D_D
import math

total_drives = 57600
spool_size = 120
num_racks = 60
drives_per_rack = 960
chunk_size=128 #KB
drive_capacity = 20 # TB
num_chunks_per_drive = drive_capacity * 1024 * 1024 * 1024 // chunk_size
num_spools_per_rack = drives_per_rack // spool_size

k_net=10
p_net=2
k_local=17
p_local=3

# when p_local+1 disks failure, how many stripes are catastrophic?
num_catas_stripes = math.comb(117, 17) / math.comb(119, 19) * num_chunks_per_drive

# portion of catastrophic stripes out of all stripes in the spool
prob_catas_stripe = num_catas_stripes / (num_chunks_per_drive*spool_size/(k_local+p_local))


# for each network stripe, it randomly resides on (10+2) racks
# the probability that a network stripe resides on 3 problematic racks is:
prob_error_racks = math.comb(57, 10+2-3) / math.comb(60, 10+2)


# probability that a network stripe resides on 3 catas spools:
prob_on_all_catas_spools = prob_error_racks * ((1/num_spools_per_rack) ** (p_net+1))


# probability that a network stripe is lost:
prob_net_stripe_lost = prob_on_all_catas_spools * (prob_catas_stripe ** (p_net+1))

total_net_stripes = num_chunks_per_drive*total_drives/((k_local+p_local)*(k_net+p_net))

prob_survival = (1-prob_net_stripe_lost) ** total_net_stripes


