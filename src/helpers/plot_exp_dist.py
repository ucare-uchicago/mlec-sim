import matplotlib.pyplot as plt
import numpy as np
import math

# 100 linearly spaced numbers
n = 100000000
x = np.random.exponential(365.25/0.0083, n)
x = np.sort(x)

temp = 0
count = 0
cell = 5
t = []
pdf = []
for i in range(n):
    while x[i] > temp + cell:
        t.append(temp)
        pdf.append(count/n)
        temp += cell
        count = 0
    if temp > 365.25:
            break
    count += 1
    if i == n - 1:
        t.append(temp)
        pdf.append(count/n)
        temp += cell



# setting the axes at the centre
fig = plt.figure()

plt.plot(t, pdf, 'r')

# plot the function
plt.savefig('exp_dist.png', format='png')