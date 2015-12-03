#!/usr/bin/env python
#-*- coding:utf-8 -*-
import re
import os
import time
import urllib
import json
import base_conf
import log

def parse_opts(argvs):
    opts = {}
    for argv in argvs:
        _argv_list = re.findall(r"--([^-=]+?)=([^=]+)", argv)
        if len(_argv_list) != 1:
            continue

        key, value = _argv_list[0]
        opts[key.strip()] = value.strip()
    return opts

def filter_blank(blank):
    try:
        if blank is None:
            return "-"
        if str(blank).strip() == "":
            return "-"
        ret = blank.replace("\n", " ").encode("utf-8")
    except Exception as info:
        if type(blank) == type(u''):
            blank = blank.encode("utf-8").replace("\n", " ")
        ret = str(blank)
    ret = ret.replace("\t", " ")
    return ret

def json_string_decode(string):
    try:
        return json.JSONDecoder().decode(string)
    except Exception as info:
        print "tools.str2dict failed : json", string, info
        return None

def get_file_list(filepath):
    if os.path.isfile(filepath):
        return [filepath]

    else:
        ret = []
        for file in os.listdir(filepath):
            _path = os.path.join(filepath, file)
            if os.path.isfile(_path):
                ret.append(_path)
            else:
                ret.extend(get_file_list(_path))
        return ret

def create_done(done_list, done = "done"):
    for done_file in done_list:
        os.system("touch %s" % ".".join([done_file, done]))

def check_done(done_list):
    for done in done_list:
        done = ".".join([done, "done"])
        if not os.path.exists(done):
            return False
    return True

def scp(src_dir, src_name, des_dir, des_name, logger = None):
    if logger is not None:
        logger.info("[tools.scp][src_dir=%s;src_name=%s;des_dir=%s;des_name=%s;]" % (src_dir, src_name, des_dir, des_name))
    os.system("mkdir -p %s" % des_dir)
    cmd = "scp -r %s/%s %s/%s" % (src_dir, src_name, des_dir, des_name)
    if logger is not None:
        logger.info("cmd = [%s]" % cmd)
    return os.system(cmd)

def convert_values(values, charset = "utf-8"):
    ret = []
    for v in values:
        if type(v) == type(u''):
            ret.append(v.encode(charset))
        else:
            ret.append(v)
    return ret

def convert_percent(a, off = 2):
    return "%s%%" % round(100 * a, off)

def parse_number(string, regx = r"^[+-]?\d+(?:\.\d+)?"):
    if string is None:
        return string
    if string.startswith("%"):
        string = string[3:]
    ret = re.findall(regx, string)
    if len(ret):
        return ret[0]
    return None

def filter_number(num):
    num = str(num)
    if not num:
        return False

    num = num.strip()
    if not num:
        return False

    if num.startswith("%"):
        num = re.sub(r"^%\w{2}", "", num)
        return filter_number(num)
    return parse_number(num)

def filter_mark_name(mark):
    mark = re.sub(r"[^\w#\.\-]", "", mark)
    return mark

def mkdir(path):
    os.system("mkdir -p %s" % path)

def logger(path):
    return log.Log(path).instance()

if __name__ == "__main__":
    print parse_number("1.11.111.111.111")
    print parse_number("-1.11aaa.aa")
    print parse_number("+1.11.111.111.111")
    print parse_number("1.11.111.111.111")
    print parse_number("1234242aa")
    print filter_number("50704814366#classInfo")
    a = "5102871547#10006-weixin-1"
    print re.sub(r"(?<=\d)[^\.\d].+$", "", a)
    print filter_number("5102871547#10006-weixin-1")
    print filter_number(u"151114779321.1242.131.13.13.1")
