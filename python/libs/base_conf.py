#!/usr/bin/env python
#-*- coding:utf-8 -*-
import tools
import re

'''
Usage  : conf = base_conf_t([conf_path_1, conf_path2, ...])
         conf.load()
         conf_path_1 的内容为 key = value 形式
         
         conf.refresh([conf_path_1, conf_path_2,])
         
'''
class base_conf_t(object):
    
    def __init__(self, conf_path = []):
        self.idx = {}
        self.conf_path = conf_path
        self.col = ['']
        self.count = 0
        self.load()
    
    def clear(self):
        self.idx = {}
        self.col = ['']
        self.count = 0
        
    def load(self):
        self.clear()
        for filepath in self.conf_path:
            for path in tools.get_file_list(filepath):
                self.load_one_file(path)
            
    def load_one_file(self, path):
        for line in open(path):
            line = line.strip()
            if line == "":
                continue
            if line.startswith("#"):
                continue
            if line.find("=") == -1:
                continue
            line = line.split("=")
            if len(line) != 2:
                continue
            key, value = line
            if key.strip() == "data":
                raise KeyError, "key can not be [data]"
            self.count += 1
            key = key.strip()
            if key in self.idx:
                print "same key [%s]" % key
                raise KeyError
            
            self.idx[key] = self.count
            self.col.append(value.strip())
            
        for key in self.idx:
            ret = re.findall(r"\{(\w+)\}", self.col[self.idx[key]])
            for _key in ret:
                self.update(key, self[key].replace("{%s}" % _key, self.col[self.idx[_key]]))
            
    def refresh(self, conf_path):
        self.conf_path = conf_path
        self.load()
            
    def __getitem__(self, key):
        if isinstance(key, int):
            keyidx = key
        elif key not in self.idx:
            print key
            raise KeyError
        else:
            keyidx = self.idx[key]
            
        return self.col[keyidx]
    
    def __getattr__(self, key):
        return self[key]
    
    def dump(self):
        for key in self.idx:
            print key, "=", self[key]
            
    def add(self, key, value):
        if key in self.idx:
            raise KeyError
        self.count += 1
        self.idx[key] = self.count
        self.col.append(value)
        
    def update(self, key, value):
        if key not in self.idx:
            raise KeyError
        count = self.idx[key]
        self.col[count] = value
    
    def __str__(self):
        return "\n".join([self._convert_to_string(key, self.col[self.idx[key]]) for key in self.idx])
    
    def _convert_to_string(self, key, value):
        return "".join(["[", ":".join(map(str, [key, value])), "]"])
    
    def get_idx(self):
        return self.idx

def get_conf(conf_path, opts = {}, is_date = False):
    conf = base_conf_t(conf_path)
    conf_idx = conf.get_idx()
    for key in opts:
        if key in conf_idx:
            conf.update(key, opts[key])
        else:
            conf.add(key, opts[key])
    
    if is_date and "date" in conf.get_idx():
        for key in conf.get_idx():
            conf.update(key, conf[key].replace("%s", conf.date))
    return conf
    
if __name__ == "__main__":
    pass
    