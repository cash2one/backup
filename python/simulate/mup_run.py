#!/usr/bin/env python
#-*- coding:utf-8 -*-
#-----------------------------------------------------
import sys
import os
import time
import copy
import math
import random
import multiprocessing
import gc
gc.disable()
#----------------------------------------------------
# config, log
import ConfigParser
C = ConfigParser.ConfigParser()
from libs.log import log
L = log.Log("./log/simulate").instance()
#----------------------------------------------------
# global var
ROOT = os.path.abspath(os.path.dirname(__file__))
result_tmp = {"ori":{"bid":0.0, "click":0.0, "price":0.0, "ads":0.0}, "qc":{"bid":0.0, "click":0.0, "price":0.0, "ads":0.0, "replace":0.0, "price_bid":0.0}}
global_result = {"total":copy.deepcopy(result_tmp), "204":copy.deepcopy(result_tmp), "225":copy.deepcopy(result_tmp)}
global_offset = 0.5
global_start = 1
global_end = 1
global_mp_sample = False
global_mp_split_lines = "100000"
#---------------------------------------------------

# multiprocess class
class mup_process(object):
    '''
        mup_name : 进程名
        filepath : 输入文件路径
        exec_run : 传入的进程处理函数
        _ret     : 子进程向父进程传递信息的dict
    '''
    global global_result

    def __init__(self, mup_name, filepath, exec_run, _ret):
        self.filepath = filepath
        self.mup_name = mup_name
        self.exec_run = exec_run
        self.ret = _ret

        self.result = copy.deepcopy(global_result)
        self.run()

    def run(self):
        with open(self.filepath) as fin:
            while True:
                line = fin.readline()
                if not line:
                    break

                ret = self.exec_run.do_process_line(line.strip())

                self.update_result(self.result, ret)

        self.ret["data"] = str(self.result)

    def update_result(self, des, src):
        for cmatch in src:
            for key in src[cmatch]:
                for attr in src[cmatch][key]:
                    des[cmatch][key][attr] += src[cmatch][key][attr]

# multiprocess start
class MP(object):
    def __init__(self, lines = "1000000", sample = False, offset = False, ratio_index = False):
        self.offset = offset
        print "--" * 40
        print "current offset = [%s]" % self.offset, time.ctime()
        self.sample = sample
        self.split_lines = lines
        self.ratio_index = ratio_index
        print "current ratio_index = [%s]" % self.ratio_index, time.ctime()
        self.simulate_instance = Simulate(self.offset, self.ratio_index)

    def run(self):
        global global_result

        input_file_name = self.simulate_instance.data_filename
        input_dir = self.split_input_file(input_file_name)
        file_list = os.listdir(input_dir)

        print "smaple file_list before len(file_list) = [%s]" % len(file_list)
        file_list = self.random_choice(file_list)
        print "smaple file_list after len(file_list) = [%s]" % len(file_list)

        result = []
        mup_index = 1
        mup_obj = []
        ret_dict = []
        for filepath in file_list:
            
            ret = multiprocessing.Manager().dict()
            ret_dict.append(ret)

            p = multiprocessing.Process(target=mup_process, args=("mup_%s" % mup_index, os.path.join(input_dir, filepath), self.simulate_instance, ret))
            p.start()
            mup_obj.append(p)
            mup_index += 1

        for obj in mup_obj:
            obj.join()

        for _ret in ret_dict:
            _ret = dict(_ret)
            _ret = eval(_ret["data"])
            self.update_result(global_result, _ret)

        self.simulate_instance.print_result(global_result)

    def random_choice(self, file_list, N = 12):
        if self.sample:
            ret = []
            while len(ret) < N:
                file_path = random.choice(file_list)
                if file_path not in ret:
                    ret.append(file_path)
        else:
            ret = file_list[:N]
        return ret

    def listen(self):
        pass

    def split_input_file(self, input_file_name):
        global ROOT
        filename = input_file_name.strip("/").split("/")[-1]
        split_file_dir = os.path.join(ROOT, "data", filename + ".root")
        if not os.path.exists(split_file_dir):
            os.mkdir(split_file_dir)
        else:
            return split_file_dir

        split_cmd = "split -l %s %s %s/%s." % (self.split_lines, input_file_name, split_file_dir, filename)
        status = os.system(split_cmd)
        if status != 0:
            print "split file failed"
            sys.exit(-1)
        return split_file_dir

    def update_result(self, des, src):
        for cmatch in src: 
            for key in src[cmatch]:
                for attr in src[cmatch][key]:
                    des[cmatch][key][attr] += src[cmatch][key][attr]


class Simulate(object):

    global C, L, ROOT

    CONF_DIR = "conf"
    DATA_DIR = "data"
    AD_INDEX = {"clk":0, "rank":1, "price":2, "l_qvalue":3, "ubm_q":4, "bid":5, "c":6, "v":7, "calc_score":8}
    Q_FACTOR = 1
    PRICE_FACTOR = 1
    PPIM_CMATCH = "204"
    PP_CMATCH = "225"
    DIVIDEQ = False
    DISGSP_C_FACTOR = 10**6

    def __init__(self, offset, ratio_index, conf = "base_conf"):
        self.conf_path = os.path.join(ROOT, self.CONF_DIR, conf)
        self.data_path = os.path.join(ROOT, self.DATA_DIR)
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)
        self.data_filename = os.path.join(self.data_path, "query_cluster_base_data.%s")
        self.RESERVE_PRICE_MIDDLE = False
        self.RESERVE_PRICE_RATIO_INDEX = False 
        self.RESERVE_PRICE_ONLINE = False
        self.USE_UBMQ = False

        self.reserve_price_middle_offset = offset
        self.reserve_price_ratio_index = ratio_index
        self.position_bias = {}
        self.reserve_price = {}
        self.config = self._conf()

        self.DISGSP_V_RATIO = float(self.config["main"]["disgsp_v_ratio"])
        self.DISGSP_PP_T = float(self.config["main"]["disgsp_pp_t"])
        self.DISGSP_PPIM_T = float(self.config["main"]["disgsp_ppim_t"])
        
        self.run()

    def _conf(self):
        ret = {}
        try:
            C.read(self.conf_path)
            for section in C.sections():
                ret[section] = {}
                for item in C.options(section):
                    ret[section][item] = C.get(section, item)
        except Exception as info:
            L.fatal("load conf failed, conf_path[%s]" % self.conf_path)
            sys.exit(-1)

        if "use_ubmq" in ret["switch"]:
            if ret["switch"]["use_ubmq"] == "1":
                #print "use ubmq"
                self.USE_UBMQ = True

        if "divideq" in ret["switch"]:
            if ret["switch"]["divideq"] == "1":
                self.DIVIDEQ = True

        if "reserve_price_middle" in ret["switch"]:
            if ret["switch"]["reserve_price_middle"] == "1":
                self.RESERVE_PRICE_MIDDLE = True

        if "reserve_price_ratio_index" in ret["switch"]:
            if ret["switch"]["reserve_price_ratio_index"] == "1":
                print "RESERVE_PRICE_RATIO_INDEX is True"
                self.RESERVE_PRICE_RATIO_INDEX = True

        if "reserve_price_online" in ret["switch"]:
            if ret["switch"]["reserve_price_online"] == "1":
                self.RESERVE_PRICE_ONLINE = True
        
        print "[switch]"
        for switch in ret["switch"]:
            print switch.ljust(15) + ":\t" + str(ret["switch"][switch])
        print "-" * 50
        return ret

    def load_base_data(self):
        if "date_str" not in self.config["main"]:
            L.fatal("date_str is not set")
            sys.exit(-1)

        #py_cmd = "cd ~/backup/task/hadoop_py && python run.py query_cluster_base_data %s" % self.config["main"]["date_str"]
        #status = os.system(py_cmd)
        #if status != 0:
        #    L.fatal("hadoop cmd failed")
        #    sys.exit(-1)

        self.data_filename = self.data_filename % self.config["main"]["date_str"]
        #cp_cmd = "cp ~/backup/task/hadoop_py/app/query_cluster_base_data/data/query_cluster_base_data.%s %s" % (self.config["main"]["date_str"], self.data_filename)
        #status = os.system(cp_cmd)
        #if status != 0:
        #    L.fatal("cp error")
        #    sys.exit(-1)

    def load_reserve_price(self):
        if "reserve_price_filepath" not in self.config["main"]:
            L.fatal("reserve_price_filepath is not set")
            sys.exit(-1)
        file_path = os.path.join(ROOT, self.data_path, self.config["main"]["reserve_price_filepath"])
        with open(file_path) as fin:
            while True:
                line = fin.readline()
                if not line:
                    break

                self.load_reserve_price_parser(line.strip())

        L.info("load_reserve_price success")

    def load_reserve_price_parser(self, line):
        lines = line.split("\t")
        query = lines[0].decode("gbk", "ignore")
        cmatch = lines[1]
        if self.RESERVE_PRICE_RATIO_INDEX:
            rp = [float(x.strip()) for x in lines[-1].strip("[]").split(",")]
        elif self.RESERVE_PRICE_MIDDLE:
            rp = [float(x) for x in lines[3:]]
        elif self.RESERVE_PRICE_ONLINE:
            rp = [float(x) for x in lines[3:-1]]
        else:
            rp = [float(x) for x in lines[2:]]

        if query not in self.reserve_price:
            self.reserve_price[query] = {}

        if cmatch not in self.reserve_price[query]:
            self.reserve_price[query][cmatch] = rp

    def sort_ad(self, line, sort_index):
        ret = []
        for ad in line.strip().split("\t"):
            ad = ad.split("_")
            ret.append(ad)
        ret_sorted = sorted(ret, key = lambda x:x[sort_index])
        return ret_sorted

    def recompute_price(self, ad_list, cmatch):
        price_index = self.AD_INDEX["price"]
        #ranking_score = []
        #for ad in ad_list:
        #    ranking_score.append(self.get_ranking_score(ad))

        for i in xrange(len(ad_list) - 1):
            ad_list[i][price_index] = self.disgsp(ad_list[i], cmatch)

        if len(ad_list) >= 1:
            last_ad_index = len(ad_list) - 1
            ad_list[last_ad_index][price_index] = self.disgsp(ad_list[last_ad_index], cmatch, True)

    def disgsp(self, ad, cmatch, last = False):
        next_rs = float(ad[self.AD_INDEX["calc_score"]])
        c = float(ad[self.AD_INDEX["c"]]) / self.DISGSP_C_FACTOR
        v = float(ad[self.AD_INDEX["v"]])
        if cmatch == self.PPIM_CMATCH:
            t = self.DISGSP_PPIM_T
        else:
            t = self.DISGSP_PP_T

        sortq = math.pow(float(ad[self.AD_INDEX["ubm_q"]]) / 10, t)

        if last:
            price = v * self.DISGSP_V_RATIO
            return price

        threshold = v * sortq

        if sortq == 0:
            return 0

        if next_rs > threshold:
            price = next_rs / sortq
        else:
            price = c * v + next_rs * (1 - c) / sortq

        return price

    def do_process_line(self, line):
        lines = line.split(":")
        query, cmatch = lines[0].split("{<_>}")
        query = query.decode("gbk", "ignore")

        rank_index = self.AD_INDEX["rank"]
        ad_list = self.sort_ad(lines[1], rank_index)

        # re compute price by disgsp
        self.recompute_price(ad_list, cmatch)
        
        ret = {}
        ret["ori"] = self.calc_price(ad_list, cmatch)
        
        #query_cmatch = "<{_}>".join([query, cmatch])
        #self.query_cmatch["ori"][query_cmatch] = True

        rp = self.reserve_price.get(query, False)

        if rp == False:
            ret["qc"] = copy.deepcopy(ret["ori"])
            return {"total":copy.deepcopy(ret), cmatch:copy.deepcopy(ret)}

        if cmatch not in rp:
            ret["qc"] = copy.deepcopy(ret["ori"])
            return {"total":copy.deepcopy(ret), cmatch:copy.deepcopy(ret)}

        rp = rp[cmatch]
        rp = self.change_reserve_price(rp)
        ret["qc"] = self.calc_price(ad_list, cmatch,  rp)
        final_ret = {cmatch:copy.deepcopy(ret), "total":copy.deepcopy(ret)}
        return final_ret

    def change_reserve_price(self, rp):
        if self.RESERVE_PRICE_MIDDLE:
            rp = self.do_reserve_price_middle(rp)

        if self.RESERVE_PRICE_RATIO_INDEX:
            rp = self.do_reserve_price_ratio_index(rp)

        return rp

    def do_reserve_price_middle(self, rp, FACTOR = 1000000, N = 8, t = 1.4):
        mean = math.pow(math.exp(float(rp[0])), t)
        var = math.pow(math.exp(float(rp[1])), t)
        ret = (mean + var * self.reserve_price_middle_offset) * FACTOR
        return [ret] * N

    def do_reserve_price_ratio_index(self, rp, N = 8):
        if self.reserve_price_ratio_index + 1 > len(rp):
            index = len(rp) - 1
        else:
            index = self.reserve_price_ratio_index
        ret = rp[index]
        return [ret] * N

    def calc_price(self, ad_list, cmatch, rp = False):
        if cmatch == self.PP_CMATCH:
            rank_max = 8
        else:
            rank_max = 4
        if rp != False:
            return self.calc_price_query_cluster(ad_list, self.position_bias[cmatch], rp, rank_max)
        else:
            return self.calc_price_psa(ad_list, self.position_bias[cmatch], rank_max)

    def calc_price_psa(self, ad_list, pb, rank_max = 8):
        price_index = self.AD_INDEX["price"]
        if self.USE_UBMQ:
            q = self.AD_INDEX["ubm_q"]
        else:
            q = self.AD_INDEX["l_qvalue"]

        bid_index = self.AD_INDEX["bid"]

        revenue = 0.0
        bid = 0.0
        click = 0.0
        rank = 1
        ad_num = 0
        for ad in ad_list:
            bid_value = float(ad[bid_index]) / self.PRICE_FACTOR
            q_value = float(ad[q]) / self.Q_FACTOR
            
            if rank > rank_max:
                click_value = q_value * pb[str(rank_max)]
            else:
                click_value = q_value * pb[str(rank)]

            if click_value > 0:
                price = float(ad[price_index]) / self.PRICE_FACTOR
                if price > bid_value:
                    price = bid_value

                revenue += price * click_value
                click += click_value
                bid += bid_value

                ad_num += 1

                rank += 1
        
        return {"click":click, "price":revenue, "bid":bid, "ads":ad_num}
        #return sum([(float(ad[price]) / self.PRICE_FACTOR) * (float(ad[q]) / self.Q_FACTOR) * pb[ad[rank]] for ad in ad_list])
        #return sum([float(ad[price]) * float(ad[q]) * pb[ad[rank]] for ad in ad_list])

    def calc_price_query_cluster(self, ad_list, pb, rp = False, rank_max = 8):
        price_index = self.AD_INDEX["price"]
        if self.USE_UBMQ:
            q = self.AD_INDEX["ubm_q"]
        else:
            q = self.AD_INDEX["l_qvalue"]
        rank_index = self.AD_INDEX["rank"]
        bid_index = self.AD_INDEX["bid"]

        revenue = 0.0
        bid = 0.0
        click = 0.0
        rank = 1
        ad_num = 0
        replace = 0
        price_bid = 0

        for ad in ad_list:
            bid_value = float(ad[bid_index]) / self.PRICE_FACTOR
            q_value = float(ad[q]) / self.Q_FACTOR
            q_bid = q_value * bid_value
            if rank > rank_max:
                reserve_price = rp[rank_max - 1]
            else:
                reserve_price = rp[rank - 1]
           
            #print "[calc_price_query_cluster] q=[%s] bid=[%s] q_bid=[%s] reserve_price=[%s] ratio=[%s]" % (q_value, bid_value, q_bid, reserve_price, reserve_price/q_bid - 1)
            if q_bid < reserve_price:
                continue

            if rank > rank_max:
                click_value = q_value * pb[str(rank_max)]
            else:
                click_value = q_value * pb[str(rank)]
            #print "[calc_price_query_cluster] click_value=[%s]" % click_value
            if click_value > 0:
                price = float(ad[price_index]) / self.PRICE_FACTOR
                if self.DIVIDEQ:
                    reserve_price = float(reserve_price) / q_value

                #print "[calc_price_query_cluster] price=[%s] reserve_price=[%s] ratio=[%s]" % (price, reserve_price, reserve_price / price -1)
                if price < reserve_price:
                    replace += 1
                    price = reserve_price

                if price > bid_value:
                    price_bid += 1
                    price = bid_value
                
                revenue += price * click_value
                bid += bid_value
                click += click_value

                ad_num += 1

                rank += 1
            else:
                continue

        return {"bid":bid, "click":click, "price":revenue, "ads":ad_num, "replace":replace, "price_bid":price_bid}

    #def do_process(self):
    #    tmp = {"ori":{"bid":0.0, "click":0.0, "price":0.0, "ads":0.0}, "qc":{"bid":0.0, "click":0.0, "price":0.0, "ads":0.0, "replace":0.0, "price_bid":0.0}}
    #    result = {"total":copy.deepcopy(tmp), self.PPIM_CMATCH:copy.deepcopy(tmp), self.PP_CMATCH:copy.deepcopy(tmp)}
    #    count = 0
    #    with open(self.data_filename) as fin:
    #        while True:
    #            line = fin.readline()
    #            if not line:
    #                break
    #            ret = self.do_process_line(line.strip())
    #            for cmatch in ret:
    #                for key in ret[cmatch]:
    #                    for attr in ret[cmatch][key]:
    #                        result[cmatch][key][attr] += ret[cmatch][key][attr]

    #            count += 1
    #            if count % 100000 == 0:
    #                print time.ctime(), "processing %sth" % count
    #            if count > 2000000:
    #                break

    def print_result(self, result):
        for cmatch in result:
            print "cmatch = %s" % cmatch
            revenue = result[cmatch]

            price_ratio = revenue["qc"]["price"] / revenue["ori"]["price"] - 1
            print "price_ratio = [%s%%]" % round(price_ratio * 100, 4)

            click_ratio = revenue["qc"]["click"] / revenue["ori"]["click"] - 1
            print "click_ratio = [%s%%]" % round(click_ratio * 100, 4)

            ori_acp = revenue["ori"]["price"] / revenue["ori"]["click"]
            qc_acp = revenue["qc"]["price"] / revenue["qc"]["click"]
            acp_ratio = qc_acp / ori_acp - 1
            print "acp_ratio = [%s%%]" % round(acp_ratio * 100, 4)

            ori_jfb = revenue["ori"]["price"] / revenue["ori"]["bid"]
            qc_jfb = revenue["qc"]["price"] / revenue["qc"]["bid"]
            jfb_ratio = qc_jfb / ori_jfb - 1
            print "jfb_ratio = [%s%%]" % round(jfb_ratio * 100, 4)

            ad_num_ratio = revenue["qc"]["ads"] / revenue["ori"]["ads"] - 1
            print "ad_num_ratio = [%s%%], qc_ads = [%s], ori_ads = [%s]" % (round(ad_num_ratio * 100, 4), revenue["qc"]["ads"], revenue["ori"]["ads"])

            ad_cut_num = revenue["ori"]["ads"] - revenue["qc"]["ads"]
            ad_cut_ratio = float(ad_cut_num) / revenue["ori"]["ads"]
            print "ad_cut_num = %s, ad_cut_ratio = %s%%" % (ad_cut_num, round(ad_cut_ratio * 100, 4))

            print "qc replace nums = [%s] ratio = [%s%%]" % (revenue["qc"]["replace"], round((float(revenue["qc"]["replace"]) / revenue["ori"]["ads"]) * 100.0, 4))
            print "qc price = bid, nums = [%s] ratio = [%s%%]" % (revenue["qc"]["price_bid"], round((float(revenue["qc"]["price_bid"]) / revenue["ori"]["ads"]) * 100.0, 4))
            print "\n\n"
        
        print "--" * 40
        print "\n\n"



    def load_pbias(self):
        if "pbias_filepath" not in self.config["main"]:
            L.fatal("pbias is not set")
            sys.exit(-1)
        
        pbias_filepath = os.path.join(ROOT, self.data_path, self.config["main"]["pbias_filepath"])
        L.info(pbias_filepath)
        with open(pbias_filepath) as fin:
            while True:
                line = fin.readline()
                if not line:
                    break

                cmatch, rank , ratio = line.strip().split("\t")
                
                if cmatch not in self.position_bias:
                    self.position_bias[cmatch] = {}

                self.position_bias[cmatch][rank] = float(ratio)


    def run(self):
        self.load_base_data()

        self.load_pbias()
        
        self.load_reserve_price()

        #self.do_process()


if __name__ == "__main__":
    offset = global_offset
    start = global_start
    end = global_end
    sample = global_mp_sample
    mp_split_lines = global_mp_split_lines

    while start <= end:
        global_result = None
        global_result = {"total":copy.deepcopy(result_tmp), "204":copy.deepcopy(result_tmp), "225":copy.deepcopy(result_tmp)}

        MP_RUN = MP(mp_split_lines, sample, start)
        MP_RUN.run()

        start += offset

    #sample = global_mp_sample 
    #mp_split_lines = global_mp_split_lines
    #start = 0
    #end = 10
    #while start < end:
    #    global_result = None
    #    global_result = {"total":copy.deepcopy(result_tmp), "204":copy.deepcopy(result_tmp), "225":copy.deepcopy(result_tmp)}
    #    MP_RUN = MP(mp_split_lines, sample, False, start)
    #    MP_RUN.run()

    #    start += 1
