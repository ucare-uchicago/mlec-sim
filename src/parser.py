#! /usr/bin/env
from array import *
import time, datetime
from disk import Disk
import os
import csv

class Parser:
    def __init__(self, traceId, traceDir):
        self.traceDir = traceDir
        failures = self.parse_file(traceId)
        #self.parse_trace_events(failures)


    def parse_file(self, traceId):
        failures = []
        #path_name = "../traces/test/fail_stream_test"
        #path_name = "../traces/fail_stream_test"
        #path_name = "../traces/fail_stream_sequential"
        path_name = self.traceDir + "fail_stream_seq_164_"
        #path_name = self.traceDir+"fail_stream_seq_164_"
        file_name = path_name + str(traceId) + '.csv'
        f = open(file_name, "r")
        #------------------------------------------------------------------------------
        # rackId + slot + Node Vertical + file system ID can be used to calculate diskId
        #--------------------------------------------------------------------------------
        disks_per_node = 84
        disks_per_rack = disks_per_node * 6
        disks_per_system = disks_per_rack * 18
        #----------------------------
        # testing examples
        #----------------------------
        #disks_per_node = 9
        #disks_per_rack = disks_per_node * 1
        #disks_per_system = disks_per_rack * 3
        with f as csvfile:
            tracereader = csv.DictReader(csvfile)
            for row in tracereader:
                rackId = int(row['Rack Number'])-1
                nodeId = int(row['Node Vertical'])+1
                slotId = int(row['Slot'])-1
                systemId = int(row['File System ID']) - 11138
                failTime = row['Failure Time']
                #print 'rack', rackId, 'server',nodeId, 'slot', slotId, 'system',systemId
                #----------------------------------
                # calculate the diskId
                #----------------------------------
                diskId = systemId*disks_per_system+(rackId-1)*disks_per_rack+(nodeId-1)*disks_per_node+slotId
                #print systemId, rackId, nodeId, slotId, diskId
                date, clock = failTime.split(' ')
                #print date, clock
                #---------------------------------------------------
                year, month, day = self.parse_year_month_day(date)
                hour, minute, second = self.parse_hour_minute_second(clock)
                fail_time = datetime.datetime(year, month, day, hour, minute, second)
                start_time = datetime.datetime(2016, 1, 1, 0, 0, 0)
                #---------------------------------------------------
                curr_time = float((fail_time - start_time).total_seconds())/3600
                #curr_time = float((fail_time - start_time).total_seconds())
                failures.append((curr_time, diskId))
        #-----------------------------------------------------------
        # failures is an array storing tuples (diskId, failure-time)
        #-----------------------------------------------------------
        self.trace_entry = failures
        #self.display_trace_entry(traceId, failures)




    #----------------------------------------------------------
    # sort the trace events based on disk failure time
    #----------------------------------------------------------
    # def parse_trace_events(self, failures):
    #    sorted_events = sorted(failures, key=lambda item: item[1])
    #    for i in range(len(sorted_events)):
    #        diskId = sorted_events[i][0]
    #        curr_time = sorted_events[i][1]
    #        if diskId not in self.trace_entry:
    #            self.trace_entry[diskId] = [curr_time]
    #        else:
    #            self.trace_entry[diskId].append(curr_time)
    #    #print sorted_events
    #    print "  >>>> ", self.trace_entry


    def parse_year_month_day(self, time):
        year,month,day = time.split('-')
        #print int(year), int(month), int(day)
        return int(year), int(month), int(day)

    def parse_hour_minute_second(self, time):
        hour, minute, second = time.split(':')
        #print int(hour), int(minute), int(second)
        return int(hour), int(minute), int(second)

if __name__ == "__main__":
    for i in range(1):
        parser = Parser(i, "../100/")
