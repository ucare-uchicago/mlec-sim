import math

failure_prob = 1-0.99968

nines = round(-math.log10(failure_prob),3)

print("failure prob: {}\nnines:{}".format(failure_prob, nines))