#!/usr/bin/env python
#-*- coding:utf-8 -*-
import multiprocessing

 # multi processing function
def multiprocess_function_t(
        target,
        args,
        pre_process_func = False,
        post_process_func = False,
        ret_all = False,
        multi_process_nums = 12):
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

MUP = multiprocess_function_t
if __name__ == "__main__":
    def print_hello(mup_name, ret):
        print mup_name
        ret["data"] = "{1:2}"

    MUP(print_hello, ())
