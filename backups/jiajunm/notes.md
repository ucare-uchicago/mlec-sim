1. Need to check upon disk repair whether there are sibling drives using network bandwidth
2. If not, we need to return all the bandwidth being used by this disk
3. If yes, we need to split the bandwidth again
----
So why not just keep track of all the bandwidth usage on a single disk?
So basically when there are 2 failures in the diskgroup, we have 7*100/2 each
----
IMPORTANT: delay repair disks with another failure in the same disk group is getting delayed again due to off>= m check that tries to split the bandwidth
----
There are things returning more network bandwidth to the system than it should (total network bandwidth keeps increaseing)
- It appears that diskgroup network usage might duplicate disk network usage, causing network usage to be double dipped
- Look at disk 179
----
somehow the delay repair disks are getting pushed into the delay repair queue
-> no, but we keep processing delayed repair, instead of repair queue, so the repaired bandwidth is not being returned

how to fix this
-> 

disks that are already using network resource when diskgroup failed -> not sure what happens yet

disk in the already failed diskgroup will be paused, but will not enter the repair queue. This means that the network resource it occupies will not be released



Implement network bandwidth sharing for top-level diskgroup stripeset
---
Repair across rack, depending on EC, may exceed the cross-rack bandwidth. Limit the repair traffic coming out of the rack.