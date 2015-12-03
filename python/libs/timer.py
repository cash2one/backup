#!/usr/bin/env python
import time
import datetime
import re

def convert_date_fmt(date, fmt_before, fmt_after):
    return time.strftime(fmt_after, time.strptime(date, fmt_before))

def timestamp_to_string(t, f = "%Y%m%d"):
    return time.strftime(f, time.localtime(t))

def string_to_timestamp(t, f = "%Y%m%d"):
    return time.strptime(t, f)

def get_today(f = "%Y%m%d%H%M%S"):
    return time.strftime(f, time.localtime(time.time()))
                         
def get_time_ago(current_day, format = "%Y-%m-%d %H:%M:%S", day = 0, minute = 0, second = 0, ago = True):
    if ago:
        return datetime.datetime.strptime(current_day, format) - datetime.timedelta(days = day, minutes = minute, seconds = second)
    else:
        return datetime.datetime.strptime(current_day, format) + datetime.timedelta(days = day, minutes = minute, seconds = second)

if __name__ == "__main__":
    pass