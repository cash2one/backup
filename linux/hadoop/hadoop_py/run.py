#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import os
import re
import datetime
import time
import ConfigParser
from libs.log import log

ROOT = os.path.abspath(os.path.dirname(__file__))

class Hadoop(object):
    global ROOT
    
    current_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
    HADOOP_MULAN_NAME = "mulan"
    HADOOP_KHAN_NAME = "khan"
    PLACE_HOLDER_DICT = {"python": "/home/work/.jumbo/bin/python",
                         "app_home": "/home/work/luoruiyang/tasks/hadoop/app"}

    def __init__(
            self,
            app = "default_app_name",
            diff_conf = ROOT + "/conf/diff_conf",
            time_seq = False,
            base_conf = ROOT + "/conf/base_conf",
            options = {},
            config_handle = None,
            log_handle = None):

        self.app_name = app
        self.app_path = os.path.join(ROOT, "app", self.app_name)
        self.diff_conf = os.path.join(ROOT, diff_conf)
        print self.diff_conf
        self.base_conf = os.path.join(ROOT, base_conf)
        self.time_seq = time_seq
        self.mulan_client = True
        self.alert_phone_list = []
        self.config_handle = config_handle
        assert self.config_handle is not None, "config_handle is None"
        self.log_handle = log_handle
        assert self.log_handle is not None, "log_handle is None"
        self.app_config = self.merge_conf(self.base_conf, self.diff_conf)
        self.local_file = False
        self.options = options
    
    def run(self):
        self.pre_exec_cmd()

        self.streaming()

        self.data_to_local()

        self.post_exec_cmd()

    def data_to_local(self):
        if self.app_config["hadoop"]["data_to_local"] == "10000":
            if self.app_config["hadoop"]["clear_local_data"] == "10000":
                os.system("rm -rf %s/*" % os.path.join(self.app_path, "data"))

            local_data_file = os.path.join(self.app_path, "data", self.app_name + "." + self.app_config["hadoop"]["time_name"] + "." + self.current_time)
            print local_data_file
            self.local_file = local_data_file
            cmd = "rm -rf %s" % local_data_file
            os.system(cmd)

            cmd = "%s fs -getmerge %s %s" % (self.app_config["hadoop"]["hadoop"], self.app_config["job_conf"]["job_output"], local_data_file)
            self.log_handle.info(cmd)
            status = os.system(cmd)
            if status != 0:
                self.log_handle.fatal("Hadoop Merge Failed")
                self.sms("[Hadoop:%s] Hadoop Merge Failed" % self.app_name)
                print "Hadoop Merge Failed"
            else:
                self.log_handle.info("Hadoop Merge Success")
                print "local_dir = [%s]" % local_data_file
                print "Hadoop Merge Success"

    def place_holder(self, cmd):
        ret = cmd
        for ph in self.PLACE_HOLDER_DICT:
            ret = ret.replace("<{%s}>" % ph, self.PLACE_HOLDER_DICT[ph])

        return ret

    def post_exec_cmd(self):
        if "exec" in self.app_config:
            for exec_key in self.app_config["exec"]:
                try:
                    # 默认参数: 当前目录及此次产出到本地的文件名
                    cmd = self.app_config["exec"][exec_key] % self.local_file#self.app_name, self.app_config["hadoop"]["time_name"])
                except:
                    cmd = self.app_config["exec"][exec_key]

                cmd = self.place_holder(cmd)

                status = os.system(cmd)
                if status != 0:
                    self.log_handle.fatal("cmd [%s] exec failed" % cmd)
                    self.sms("[Hadoop:%s] cmd [%s] exec failed" % (self.app_name, cmd))
                else:
                    print "cmd [%s] exec success" % cmd
                    self.log_handle.info("cmd [%s] exec success" % cmd)

    def pre_exec_cmd(self):
        if "pre_exec" in self.app_config:
            for exec_key in self.app_config["pre_exec"]:
                cmd = self.app_config["pre_exec"][exec_key]
                
                cmd = self.place_holder(cmd)
                
                status = os.system(cmd)
                if status != 0:
                    self.log_handle.fatal("cmd [%s] exec failed" % cmd)
                    self.sms("[Hadoop:%s] cmd [%s] pre_exec failed" % (self.app_name, cmd))
                else:
                    print "cmd [%s] exec success" % cmd
                    self.log_handle.info("cmd [%s] exec success" % cmd)

    def streaming(self):
        self.log_handle.info("get streaming cmd begin")
        streaming_cmd = '''%s fs -rmr %s''' % (self.app_config["hadoop"]["hadoop"], self.app_config["job_conf"]["job_output"])
        status = os.system(streaming_cmd)

        streaming_cmd =  '''%s streaming'''                           % self.app_config["hadoop"]["hadoop"]
        streaming_cmd += '''    -D mapred.job.name="%s"'''            % self.app_config["job_conf"]["job_name"]
        streaming_cmd += '''    -D mapred.job.map.capacity="%s"'''    % self.app_config["job_conf"]["job_map_cap"]
        streaming_cmd += '''    -D mapred.job.reduce.capacity="%s"''' % self.app_config["job_conf"]["job_reduce_cap"]
        streaming_cmd += '''    -D mapred.job.priority="%s"'''        % self.app_config["job_conf"]["job_priority"]
        streaming_cmd += '''    -D mapred.map.tasks="%s"'''           % self.app_config["job_conf"]["job_map_tasks"]
        streaming_cmd += '''    -D mapred.reduce.tasks="%s"'''        % self.app_config["job_conf"]["job_reduce_tasks"]
        streaming_cmd += '''    -D mapred.max.split.size="%s"'''      % self.app_config["job_conf"]["job_max_split_size"]
        streaming_cmd += '''    -D mapred.memory.limit="%s"'''        % self.app_config["job_conf"]["job_mem_limit"]
        streaming_cmd += '''    -D stream.memory.limit="%s"'''        % self.app_config["job_conf"]["stream_mem_limit"]
        # temp
        streaming_cmd += '''    -D abaci.job.suicide.min.ark.cache.error.count="%s"''' % self.app_config["job_conf"]["job_cache_error"]

        streaming_cmd += '''    -D dce.shuffle.enable=false'''
        streaming_cmd += '''    -input "%s"'''                        % self.app_config["job_conf"]["job_input"]
        streaming_cmd += '''    -output "%s"'''                       % self.app_config["job_conf"]["job_output"]
        streaming_cmd += '''    -partitioner %s'''                    % self.app_config["job_conf"]["job_partitioner"]
        if self.app_config["job_conf"].get("job_second_sort", "10001") == "10000":
            if "job_key_fields" in self.app_config["job_conf"]\
                and "job_key_partition" in self.app_config["job_conf"]:
                streaming_cmd += '''    -D stream.num.map.output.key.fields="%s"'''\
                                                                      % self.app_config["job_conf"]["job_key_fields"]
                streaming_cmd += '''    -D num.key.fields.for.partition=""'''\
                                                                      % self.app_config["job_conf"]["job_key_partition"]

        if self.app_config["job_conf"].get("job_input_join", "10001") != "10000":
            streaming_cmd += '''    -inputformat "%s"'''              % self.app_config["job_conf"]["job_inputformat"]
        streaming_cmd += '''    -cacheArchive "%s"'''                 % self.app_config["job_conf"]["job_py_cache_archive"]
        streaming_cmd += '''    -mapper "%s %s'''                     % (self.app_config["job_conf"]["job_py_env"], self.app_config["job_conf"]["job_mapper"])
        mapper_argv = self.options.get("mapper_argv", False)
        if mapper_argv is not False:
            streaming_cmd += " "
            streaming_cmd += " ".join(mapper_argv.split("_"))
        streaming_cmd += '''"'''

        streaming_cmd += '''    -reducer "%s %s'''                   % (self.app_config["job_conf"]["job_py_env"], self.app_config["job_conf"]["job_reducer"])
        reducer_argv = self.options.get("reducer_argv", False)
        if reducer_argv is not False:
            streaming_cmd += " "
            streaming_cmd += " ".join(reducer_argv.split("_"))
        streaming_cmd += '''"'''

        bin_dir = os.path.join(self.app_path, "bin")
        for bin_file in os.listdir(bin_dir):
            bin_file_path = os.path.join(bin_dir, bin_file)
            if bin_file_path.endswith("pyc"):
                continue
            streaming_cmd += '''    -file "%s"'''                  % bin_file_path
        streaming_cmd += ''' '''

        self.print_streaming_cmd(streaming_cmd)
        self.log_handle.info("hadoop_cmd = {\n%s\n}" % streaming_cmd.replace("    ", "\n    "))
        status = os.system(streaming_cmd)
        if status != 0:
            self.log_handle.fatal("Hadoop Job Failed~")
            self.sms("Hadoop Job Failed\n[%s]" % self.app_name)
            sys.exit(-1)

    def print_streaming_cmd(self, cmd):
        print 
        print "=" * 150
        print cmd.replace("    ", "\n    ")
        print "=" * 150
        print

    def sms(self, msg):
        for phone in self.alert_phone_list:
            cmd = '''gsmsend -s emp01.baidu.com:15003 -s emp02.baidu.com:15003 %s@"%s"''' % (phone, msg)
            os.system(cmd)

    def time_squence(self, time_str, time_name = False, format = "%Y%m%d"):
        if time_str.find(":") != -1:
            if time_name:
                return time_str.replace(":", "_to_")
            start, end = time_str.split(":")
            ret_str = []
            start_time = datetime.datetime.strptime(start, format)
            end_time = datetime.datetime.strptime(end, format)
            while start_time <= end_time:
                ret_str.append(start_time.strftime(format))
                start_time = start_time + datetime.timedelta(days = 1)
            return ",".join(ret_str)
        else:
            return time_str

    def merge_conf(self, base, diff):
        self.log_handle.info("merge_conf begin")
        if not os.path.exists(base):
            self.log_handle.fatal("base_conf [%s] does not exists" % base)
            self.sms("[Hadoop:%s] merge_conf base_conf [%s] does not exists" % (self.app_name, base))
            sys.exit(-1)

        ret = {}
        try:
            self.config_handle.read(base)
            for section in self.config_handle.sections():
                ret[section] = {}
                for item in self.config_handle.options(section):
                    ret[section][item] = self.config_handle.get(section, item)
        except Exception as info:
            self.log_handle.fatal("merge_conf get base_conf failed")
            self.sms("[Hadoop:%s] merge_conf get base_conf failed" % self.app_name)
            sys.exit(-1)

        try:
            self.config_handle.read(diff)
            for section in self.config_handle.sections():
                if section not in ret:
                    ret[section] = {}
                for item in self.config_handle.options(section):
                    ret[section][item] = self.config_handle.get(section, item)
        except Exception as info:
            self.log_handle.fatal("merge_conf get diff_conf failed")
            self.sms("[Hadoop:%s] merge_conf get diff_conf failed" % self.app_name)
            sys.exit(-1)

        if "hadoop" not in ret["hadoop"]:
            self.log_handle.fatal("set hadoop client type [mulan or khan]")
            self.sms("[Hadoop:%s] set hadoop client type" % self.app_name)
            sys.exit(-1)

        if ret["hadoop"]["hadoop"] != self.HADOOP_MULAN_NAME:
            ret["hadoop"]["hadoop"] = ret["hadoop"]["hadoop_client_khan"]
            self.mulan_client = False
            self.log_handle.info("use khan client, current_level[%s]" % ret["job_conf"]["job_priority"])
            print "[NOTICE] use khan client, current_level[%s]" % ret["job_conf"]["job_priority"]
        else:
            ret["hadoop"]["hadoop"] = ret["hadoop"]["hadoop_client_mulan"]
            self.mulan_client = True
            self.log_handle.info("use mulan client, current_level [%s]" % ret["job_conf"]["job_priority"])
            print "[NOTICE] use mulan client, current_level [%s]" % ret["job_conf"]["job_priority"]

        ret["hadoop"]["time_name"] = self.time_squence(ret["hadoop"]["time_seq"], True)
        if ret["hadoop"]["time_seq"].find(":") != -1:
            ret["hadoop"]["time_name"] = self.time_squence(ret["hadoop"]["time_seq"], True)
            ret["hadoop"]["time_seq"] = self.time_squence(ret["hadoop"]["time_seq"])

        if self.time_seq != False:
            if self.time_seq.find(":") != -1:
                ret["hadoop"]["time_name"] = self.time_squence(self.time_seq, True)
                ret["hadoop"]["time_seq"] = self.time_squence(self.time_seq)
            else:
                ret["hadoop"]["time_seq"] = self.time_seq
                ret["hadoop"]["time_name"] = self.time_seq

        if "job_input" not in ret["job_conf"]:
            self.log_handle.fatal("job_input is not set")
            self.sms("[Hadoop:%s] job_input is not set" % self.app_name)
            sys.exit(-1)
        else:
            length = len(re.findall(r"%s", ret["job_conf"]["job_input"]))
            if length > 0:
                ret["job_conf"]["job_input"] = ret["job_conf"]["job_input"] % tuple([ret["hadoop"]["time_seq"]] * length)

        prefix_type = ret["job_conf"]["job_output_prefix_mulan"].rstrip("/")
        if not self.mulan_client:
            prefix_type = ret["job_conf"]["job_output_prefix_khan"].rstrip("/")
        ret["job_conf"]["job_output"] = "/".join([prefix_type,
                                                  ret["job_conf"]["job_username"],
                                                  "hadoop_job",
                                                  ret["job_conf"]["job_name"],
                                                  ret["hadoop"]["time_name"], self.current_time])

        if "job_output_hold" in ret["job_conf"] and ret["job_conf"]["job_output_hold"] != "10001":
            ret["job_conf"]["job_output"] = os.path.join(ret["job_conf"]["job_output_hold"], ret["hadoop"]["time_name"])

        print "job_output =", ret["job_conf"]["job_output"]
        ret["job_conf"]["job_name"] = "_".join([ret["job_conf"]["job_name"], self.current_time])

        if "alert_phone" not in ret["hadoop"]:
            print "FATAL : please set alert_phone in conf_file : [alert_phone = 1,2,3]"
        else:
            self.alert_phone_list = ret["hadoop"]["alert_phone"].strip().split(",")

        self.log_handle.info("merge_conf success")
        return ret

def parse_options(argvs):
    assert len(argvs) >= 2, "Usage: python run.py app_name --[key]=[value]"

    job_name = argvs[1].strip("/").split("/")[-1]
    options = {"job_name": job_name}
    for argv in argvs[2:]:
        _argv_list = re.findall(r"--([^-=]+?)=([^=]+)", argv)
        if len(_argv_list) != 1:
            continue
        key, value = _argv_list[0]
        options[key] = value.strip()
    return options

def run(options):
    # set job_name <=> ./app/job_name/
    JOB_NAME = options.get("job_name", "default_job_name")
    LOG_FILE = ROOT + "/app/%s/log/%s" % (JOB_NAME, JOB_NAME)
    
    L = log.Log(LOG_FILE).instance()
    L.info("LOG_FILE = [%s]" % LOG_FILE)
    
    C = ConfigParser.ConfigParser()

    DIFF_CONF = ROOT + "/app/%s/conf/diff_conf" % JOB_NAME
    L.info("JOB_NAME = [%s], DIFF_CONF = [%s]" % (JOB_NAME, DIFF_CONF))
    
    JOB_TIME_SEQ = options.get("day", False)

    H = Hadoop(JOB_NAME, DIFF_CONF, JOB_TIME_SEQ, options = options, config_handle = C, log_handle = L)
    H.run()

if __name__ == "__main__":
    run(parse_options(sys.argv))
