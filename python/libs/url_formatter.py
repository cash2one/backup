#!/usr/bin/env python
#-*- coding:utf-8 -*-
import re
import json

class UrlNode(object):

    def __init__(self):
        self.protocol = False
        self.host = False
        self.controller = False
        self.parameters = {}

    def add(self, key, value):
        self.parameters[key] = value

    def __str__(self, *args, **kwargs):
        return json.JSONEncoder().encode({"p": self.protocol,
                                          "h": self.host,
                                          "c": self.controller,
                                          "param": self.parameters
                                          })

class UrlFormatter(object):
    def __init__(self):
        self.URL_REGX = r'''(?:^(?P<protocal>https?)://(?P<host>[^/?]+)/*|^/)(?P<controller>[^?]+)?'''
        self.PARAM_REGX = r'''([^&]+)=([^&?]+)'''
        self.STANDARD_URL_PART = 2
        self.STANDARD_URL_SEP = "?"
        self.ILLEGAL_URL_SEP = ["&", "#"]
        self.FAILED = False
        self.SUCCESS = True

        self.HOST_SPLIT = "."

        self.KEY_CODE = "code"
        self.KEY_DATA = "data"
        self.KEY_HOST = "host"
        self.KEY_PARAM = "param"
        self.KEY_CONTROLLER = "controller"

    def is_valid(self, url):
        url = self.split_string2list(url, self.STANDARD_URL_SEP)
        if len(url) == 1:
            for url_sep in self.ILLEGAL_URL_SEP:
                if url[0].find(url_sep) != -1:
                    return self.split_string2list(url[0], url_sep)
        else:
            controller, param = url
            if controller.find(self.ILLEGAL_URL_SEP[0]) != -1:
                controller = self.split_string2list(controller, self.ILLEGAL_URL_SEP[0])
                url = [controller[0], "".join([controller[1], param])]

        return url

    def split_string2list(self, string, sep):
        index = string.find(sep)
        if index == -1:
            return [string]

        return [string[:index], string[index+1:].strip(sep)]

    def analysis(self, url, skip_param = False):
        url_node = UrlNode()
        result = self.is_valid(url)

        if not result:
            return self.response(self.FAILED, url_node)

        ret = re.findall(self.URL_REGX, result[0])
        if len(ret) == 0:
            return self.response(self.FAILED, url_node)
        protocol, host, controller = ret[0]
        if controller == "":
            controller = False

        host = host.split(self.HOST_SPLIT)
        url_node.protocol = protocol
        url_node.host = host
        url_node.controller = controller
        if len(result) == 1 or skip_param:
            return self.response(self.SUCCESS, url_node)

        ret = re.findall(self.PARAM_REGX, result[1])
        if len(ret) == 0:
            return self.response(self.SUCCESS, url_node)

        url_node.parameters = {key: value for key, value in ret}
        return self.response(self.SUCCESS, url_node)

    def response(self, code, msg):
        return {self.KEY_CODE: code, self.KEY_DATA: msg}

class Test:
    def __init__(self):
        self.p = {}

    def add(self, key, value):
        self.p[key] = value

def test(node, key, v):
    node[key]["data"].add("a", v)

if __name__ == "__main__":
    url_f = UrlFormatter()
    for url in ("/haha/hehe?a=b",
                "http://www.baidu.com",
                "www.baidu.com",
                "http",
                "http://cd.genshuixue.com/so/%E8%8B%B1%E8%AF%AD-975_976_977-419706880--3.html?source=search",
                "http://m.genshuixue.com/teacher/detailTextInfo?number=151109775231",
                "http://m.genshuixue.com/activity/mobileCenter?id=100?&zn=zn_shequhuodong_teacherapp&source=cptad&isp=1",
                "http://m.genshuixue.com/static/login?next=/video_course/getcourseshowdetail?number=15082247138",
                "http://www.genshuixue.com/teacher/classCourseDetail/150908816356?zn=zn_150908816356_pc&isp=1",
                "http://m.genshuixue.com/teacher/classCourseDetail/151030516990"):
        ret = url_f.analysis(url)
        print ret["data"]

    url_node_1 = UrlNode()

    url_node_2 = UrlNode()
    url_node_2.add("a", "aaa")
    print url_node_1
    print url_node_2

    test_node = Test()
    a = {1:{"data": test_node}}
    test(a, 1, 10)
    print a[1]["data"].p
    test_node_2 = Test()
    b = {1:{"data": test_node_2}}
    test(b, 1, 100)
    print b[1]["data"].p
    print a[1]["data"].p
