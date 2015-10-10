#!/usr/bin/env python
#-*- coding:utf-8 -*-

import time
import datetime

class LibTime(object):
    
    def __init__(self):
        pass
    
    def get_now(self, timestamp = False, format = "%Y-%m-%d %H:%M:%S"):
        if timestamp:
            return time.time()
        else:
            return time.strftime(format,time.localtime(time.time()))
        
    def timestamp_to_string(self, timestamp, format = "%Y-%m-%d %H:%M:%S"):
        return time.strftime(format,time.localtime(timestamp))
        
    def string_to_timestamp(self, time_str, format = "%Y-%m-%d %H:%M:%S"):
        return time.mktime(time.strptime(time_str, format))
    
    def get_days_ago(self, current_day, format = "%Y-%m-%d %H:%M:%S",day = 0, minute = 0, second = 0, ago = True):
        if ago:
            return datetime.datetime.strptime(current_day, format) - datetime.timedelta(days = day, minutes = minute, seconds = second)
        else:
            return datetime.datetime.strptime(current_day, format) + datetime.timedelta(days = day, minutes = minute, seconds = second)
        
if __name__ == "__main__":
    LT = LibTime()
    print LT.get_now()
    print LT.timestamp_to_string(1414408197)
    print LT.string_to_timestamp("2014-10-27 19:09:57")
    print LT.get_days_ago("20141027", "%Y%m%d", 1, 2 )