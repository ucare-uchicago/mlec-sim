import math

nines = 2.46
# loss_rate = 10**(0-nines)
loss_rate = 277660 / 25600000

afr = 1 - math.exp(0-loss_rate)

print(afr*100)