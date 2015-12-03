#!/user/bin/env python
import struct
import os

class BaseReader(object):
    BINARY_READ = "rb"
    def __init__(self, filepath):
        self.fp = open(filepath, self.BINARY_READ)
        self.count = 0
        self.content = False
        
    def next(self):
        self.content = self.fp.readline()
        if self.content:
            self.count += 1
        else:
            self.fp.close()
        return self.content
    
    def line(self):
        return self.content
    
    def counter(self):
        return self.count

class PbReader(object):
    
    def __init__(self, filepath = "", obj = None):
        self.filepath = filepath
        self.obj = obj
        if os.path.isfile(self.filepath):
            self.handle = open(self.filepath, "rb")
        else:
            self.handle = False
            
        self.header_length = 4
        self.header_type = "i"
        
    def refresh(self, filepath = "", obj = None):
        self.__init__(filepath, obj)
        
    def next(self):
        try:
            header = self.handle.read(self.header_length)
            length = struct.unpack(self.header_type, header)[0]
            self.obj.ParseFromString(self.handle.read(int(length)))
            return True
            
        except Exception as info:
            return False
        
    def data(self):
        return self.obj
    
if __name__ == "__main__":
    pass