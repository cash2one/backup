#!/usr/bin/env python
#-*- coding:utf-8 -*-
import gc
gc.disable()

import sys
import os
import random
import math
import time
import copy
import multiprocessing
from libs.kdtree import kdtree

from libs.log import log
L = log.Log("./log/kmeans").instance()

import ConfigParser
C = ConfigParser.ConfigParser()

ROOT = os.path.abspath(os.path.dirname(__file__))

class Distance(object):

    def euclidean_metric(self, x, y):
        return math.sqrt(sum([math.pow(x[i] - y[i], 2) for i in xrange(len(x))]))

    def angle_cosine(self, x, y):
        inner_product = sum([x[i] * y[i] for i in xrange(len(x))])
        return -1 * inner_product / (self.get_mod_of_vector(x) * self.get_mod_of_vector(y))
    
    def get_mod_of_vector(self, x):
        return math.sqrt(sum([math.pow(x[i], 2) for i in range(len(x))]))

class KMeans(object):
    global C, L, ROOT
    DISTANCE = Distance().euclidean_metric
    #DISTANCE = Distance().angle_cosine

    def __init__(self, conf):
        self.conf_dir = os.path.join(ROOT, "conf")
        self.conf_file_path = os.path.join(self.conf_dir, conf)
        self.config = self._conf(self.conf_file_path)
        self.data_dir = os.path.join(ROOT, "data")
        self.output_dir = os.path.join(ROOT, "output")
        self.data = {}
        self.data_keys = []
        self.clusters = {}
        self.errors = {"last_error":0.0, "current_error":0.0}
        self.process_num = False
        self.KD_TREE = False
        self.KDTree_t = False
        self.config = self._conf(self.conf_file_path)
        self.K = int(self.config["kmeans"]["k"])
        self.good_to_stop = False
        self.error_up_bound = float(self.config["kmeans"]["error_up_bound"])
        self.error_change_threshold = float(self.config["kmeans"]["error_change_threshold"])
        self.errors = {"min_error":1000000, "current_error":10000000, "last_error":1000000, "good_cluster":None}

    def _conf(self, conf):
        if not os.path.isfile(conf):
            L.fatal("conf_file[%s] does not exists~" % conf)
            sys.exit(-1)

        ret = {}
        try:
            C.read(conf)
            for section in C.sections():
                ret[section] = {}
                for item in C.options(section):
                    ret[section][item] = C.get(section, item)
        except Exception as info:
            L.fatal("read conf failed")
            sys.exit(-1)
        if "env" in ret:
            if "process_num" in ret["env"]:
                try:
                    self.process_num = int(ret["env"]["process_num"])
                except:
                    self.process_num = False

            if "kd_tree" in ret["env"]:
                if ret["env"]["kd_tree"] == "1":
                    self.KD_TREE = True
                else:
                    self.KD_TREE = False
        return ret

    def load_data(self):
        L.info("load_data begin")
        input_data_path = os.path.join(self.data_dir, self.config["kmeans"]["input"])
        with open(input_data_path) as fin:
            while True:
                line = fin.readline()
                if not line:
                    break

                lines = line.strip().split("\t")
                self.data[lines[0]] = [float(x) for x in lines[1:]]
                self.data_keys.append(lines[0])

        L.info("load_data success, input lines = [%s]" % len(self.data_keys))

    def random_choice(self, querys, N = 12):
        if N > len(querys):
            N = len(querys) - 1
            self.K = N

        ret = []
        while True:
            query = random.choice(querys)
            if query not in ret:
                ret.append(query)

            if len(ret) == N:
                break
        return ret

    def kmeans_init_centers(self):
        L.info("kmeans_init_centers begin")
        ret = self.random_choice(self.data_keys, self.K)
        self.d = len(self.data[ret[0]])
        index = 0
        for query in ret:
            self.clusters[index] = {"center":self.data[query], "samples":[]}
            index += 1

        L.info("kmeans_init_centers success")

    def clear_clusters_samples(self):
        for center in self.clusters.keys():
            self.clusters[center]["samples"] = []

    def compute_distance(self, vector_x, vector_y):
        return self.DISTANCE(vector_x, vector_y)

    def mup_process_assign_samples(self, mup_name, exec_class, ret):
        clusters = copy.deepcopy(exec_class.clusters)
        DISTANCE_UP_BOUND = 10000000000
        data_length = len(exec_class.data_keys)
        name, mup_index, offset = mup_name.split("_")

        offset = int(offset)
        start = int(mup_index)
        while start < data_length:
            query = exec_class.data_keys[start]
            vector = exec_class.data[query]
            # 方法一 从kd树中找寻与当前vector最近临的点
            if self.KD_TREE:
                assign_center = self.KDTree_t.query(vector)
                assign_center = assign_center[-1][-1]
            # 方法二 采用穷举的方式遍历出最近的中心点
            else:
                assign_center_distance = DISTANCE_UP_BOUND
                for center in clusters:
                    center_vector = clusters[center]["center"]
                    distance = exec_class.compute_distance(center_vector, vector)
                    if distance < assign_center_distance:
                        assign_center = center
                        assign_center_distance = distance

            clusters[assign_center]["samples"].append(query)
            start += offset

        ret["data"] = str(clusters)

    def kmeans_assign_samples_mup(self):
        self.multiprocess_function_t(
                self.mup_process_assign_samples,
                (self,),
                self.clear_clusters_samples,
                self.merge_clusters,
        )

    def merge_clusters(self, src):
        for center in self.clusters:
            self.clusters[center]["samples"].extend(src[center]["samples"])

    def compute_center(self, samples):
        ret = [0.0] * self.d
        count = 0
        for query in samples:
            vector = self.data[query]
            for i in xrange(self.d):
                ret[i] += vector[i]
            count += 1
        if count == 0:
            return False

        for i in xrange(self.d):
            ret[i] = ret[i] / count
        
        return ret

    def mup_process_update_centers(self, mup_name, exec_class, ret):
        K = exec_class.K
        name, mup_index, offset = mup_name.split("_")

        offset = int(offset)
        start = int(mup_index)
        clusters = {}
        while start < K:
            center = exec_class.clusters[start]
            tmp = exec_class.compute_center(center["samples"])
            clusters[start] = {}
            clusters[start]["samples"] = center["samples"]
            if tmp:
                clusters[start]["center"] = tmp
            else:
                clusters[start]["center"] =  center["center"]

            start += offset

        ret["data"] = str(clusters)

    def kmeans_update_centers_mup(self):
        self.multiprocess_function_t(
                self.mup_process_update_centers,
                (self,),
                False,
                self.clusters.update
                )

    def print_clusters(self):
        print "-" * 50
        for center in self.clusters:
            print "center[%s]" % center, "size[%s]" % len(self.clusters[center]["samples"])
        print "-" * 50

    def kmeans_compute_errors_mup(self):
        self.multiprocess_function_t(
                self.mup_process_compute_errors,
                (self,),
                False,
                self.update_errors,
                True
                )

    def mup_process_compute_errors(self, mup_name, exec_class, ret):
        K = exec_class.K
        name, mup_index, offset = mup_name.split("_")

        offset = int(offset)
        start = int(mup_index)
        error = 0.0
        while start < K:
            center = exec_class.clusters[start]
            vector_c = center["center"]
            for sample in center["samples"]:
                vector_x = exec_class.data[sample]
                error += exec_class.DISTANCE(vector_c, vector_x)

            start += offset

        ret["data"] = str(error)
    
    # multi processing function
    def multiprocess_function_t(self, target, args, pre_process_func = False, post_process_func = False, ret_all = False, multi_process_nums = 12):
        '''
            target     :    进程调用的主程序
            args       :    target参数
            pre        :    预处理函数
            post       :    后处理函数
            ret_all    :    标记子进程返回值的处理方式
            m_p_n      :    进程数
        '''
        #if self.process_num is not False:
        #    multi_process_nums = self.process_num

        if pre_process_func is not False:
            pre_process_func()
 
        mup_ret_dict = []
        mup_obj = []
        
        for i in xrange(multi_process_nums):
            ret = multiprocessing.Manager().dict()
            mup_ret_dict.append(ret)
            p = multiprocessing.Process(
                    target=target,
                    args=tuple(["mup_%s_%s" % (i, multi_process_nums)] + list(args) + [ret])
                )
            p.start()
            mup_obj.append(p)


        for obj in mup_obj:
            obj.join()
        
        if ret_all:
            if post_process_func is not False:
                post_process_func(mup_ret_dict)
        else:
            for ret in mup_ret_dict:
                ret = dict(ret)
                ret = eval(ret["data"])
                if post_process_func is not False:
                    post_process_func(ret)

    def update_errors(self, ret_dict):
        error = 0.0
        for ret in ret_dict:
            ret = dict(ret)
            error += float(ret["data"])

        current_error = self.errors["current_error"]
        last_error = self.errors["last_error"]
        min_error = self.errors["min_error"]
        if error < min_error:
            self.errors["min_error"] = error
            self.errors["good_cluster"] = copy.deepcopy(self.clusters)

        self.errors["current_error"] = error
        self.errors["last_error"] = current_error

    def kmeans_output_centers(self):
        output_path = self.config["kmeans"]["output"] % time.strftime("%Y%m%d%H%M", time.localtime(time.time()))
        with open(output_path, "wb") as fout:
            final_clusters = self.errors["good_cluster"]
            for center in final_clusters:
                for sample in final_clusters[center]["samples"]:
                    fout.write("\t".join([sample, str(center)]))
                    fout.write("\n")

    def kmeans(self):

        self.kmeans_init_centers()

        iter_start = 1
        iter_end = int(self.config["kmeans"]["max_iter"])
        while iter_start <= iter_end:
            print "-" * 50
            if self.KD_TREE:
                print "update_kd_tree begin", time.ctime()
                self.KDTree_t = kdtree.KDTree.construct_from_data(self.clusters)
                print "update kd_tree end", time.ctime()

            print "current iter [%s]" % iter_start, time.ctime()
            print "kmeans_assign_samples begin", time.ctime()
            self.kmeans_assign_samples_mup()
            print "kmeans_assign_samples success", time.ctime()


            print "kmeans_update_centers begin", time.ctime()
            self.kmeans_update_centers_mup()
            print "kmeans_update_centers success", time.ctime()


            #self.print_clusters()

            self.kmeans_compute_errors_mup()
            print "current_error", self.errors["current_error"]
            print "last_error", self.errors["last_error"]
            print "min_error", self.errors["min_error"]
            print "=" * 50
            if self.errors["current_error"] < self.error_up_bound:
                #or self.errors["current_error"] - self.errors["last_error"] < self.error_change_threshold:
                #self.good_to_stop = True
                if self.errors["current_error"] > self.errors["min_error"]:
                    print "plus : current_error > min_error"
                break
            iter_start += 1

        self.print_clusters()

        print "kmeans_output_centers begin", time.ctime()
        self.kmeans_output_centers()
        print "kmeans_output_centers success", time.ctime()
        
        print "run SUCCESS"

    def run(self):
        self.load_data()

        self.kmeans()

if __name__ == "__main__":
    conf = "conf.ini"
    K = KMeans(conf)
    K.run()
