import heapq
from constants import debug

class RepairQueue:

    def __init__(self):
        # storing disk tuple of [repair_finish_time, disk_id]
        self.backing_queue = []
        heapq.heapify(self.backing_queue)

    # simple wrapper
    def peek(self):
        if (len(self.backing_queue) == 0):
            return None
        
        return self.backing_queue[0]

    # simple wrapper
    def pop(self):
        return heapq.heappop(self.backing_queue)

    # this performs the priority scheduling logic
    # disk_tup is (finish_time, repair_time, disk_id)
    def push(self, disk_tup):
        fail_disk = disk_tup[2]

        # we check whether the list already contains this disk
        # if yes, we find the one with the longest repair time
        #   because all the repairs are done sequentially
        already_repairing = None
        for tup in self.backing_queue:
            if tup[2] == fail_disk:
                already_repairing = tup

        if already_repairing == None:
            # this means there is no such disk queued
            heapq.heappush(self.backing_queue, disk_tup)
        else:
            # this means that this disk is already repairing, we need to adjust the finish time
            if debug: print(">>Fail disk {} already repairing with finish time {}".format(fail_disk, already_repairing[0]))
            self.backing_queue.remove(already_repairing)
            # TODO: this is some unfortunate implementation - reheapify
            # TODO: use lazy delete - keep a set of to be delete, and then check when popping
            heapq.heapify(self.backing_queue)
            # then push
            if debug: print(">>Pushing new finish time {} with repair time {}".format(already_repairing[0] + disk_tup[1], disk_tup[1]))
            heapq.heappush(self.backing_queue, (already_repairing[0] + disk_tup[1], already_repairing[1] + disk_tup[1], disk_tup[2]))

    # this removes all the entries from the queue if it has a
    #  REPAIR FINISH TIME earlier than the time arg
    def filter(self, time):
        # TODO: optimize with dict approach or numpy
        if debug: print("Filter all drives repaired before " + str(time))
        removed = []
        while (self.peek() != None and self.peek()[0] < time):
            disk_popped = self.pop()
            #print("Popping disk " + str(disk_popped[2]) + " which is repaired at " + str(disk_popped[0]) + ", queue size left: " + str(self.size()))
            removed.append(disk_popped)
        #print("Queue size after filter " + str(self.size()) + ", queue head time " + str("NA" if self.peek() == None else self.peek()[0]))
        return removed

    def size(self):
        return len(self.backing_queue)

    def print(self):
        # print("Printing rec queue with len " + str(len(self.backing_queue)))
        mystr = ""
        count = 0
        for disk in self.backing_queue:
            if count != len(self.backing_queue) - 1:
                mystr += "({:0.1f},{:0.1f},{})<=".format(disk[0], disk[1], disk[2])
            else:
                mystr += "({:0.1f},{:0.1f},{})".format(disk[0], disk[1], disk[2])
        
            count += 1
            
        print(mystr, flush=True)