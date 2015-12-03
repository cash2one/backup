#!/usr/bin/env python
import re

class base_parser_t(object):
    def __init__(self, idx, sep = "\t", safe_mode = True):
        self.idx = idx
        if type(idx) == type([]):
            self.idx = {key: idx.index(key) + 1 for key in idx}
        self.colums = len(self.idx)
        self.sep = sep
        self.safe = safe_mode

    def parse(self, line = None):
        if line:
            self.col = ['']
            tmp = self.process(line)
            if self.safe and len(tmp) != self.colums:
                return False
            if tmp:
                self.col.extend(tmp)
                return True

        return False

    def __getitem__(self, key):
        if isinstance(key, int):
            keyidx = key
        elif key not in self.idx:
            raise KeyError
        else:
            keyidx = self.idx[key]

        return self.col[keyidx]

    def __getattr__(self, key):
        return self[key]

    def process(self, line):
        return line.strip().split(self.sep)

if __name__ == "__main__":
    pass


