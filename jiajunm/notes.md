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