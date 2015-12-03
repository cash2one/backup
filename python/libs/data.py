#!/usr/bin/env python,
class BaseDict(object):
    
    def __init__(self, safe = True):
        self.data = {}
        self.keys = []
        self.safe = safe
        
    def set(self, key, field, value):
        if key not in self.data:
            self.data[key] = {}
        self.data[key][field] = value
        
    def inc(self, key, field, offset = 1):
        self.data[key][field] += offset
        
    def get_keys(self):
        return self.data.keys()
    
    def get(self, key):
        return self.data[key]
    
    def get_field(self, key, field):
        try:
            return self.data[key][field]
        except Exception as info:
            return False
        
    def add_to_list(self, key, field, value):
        if key not in self.data:
            self.data[key] = {}
        if field not in self.data[key]:
            self.data[key][field] = []
        self.data[key][field].append(value)
    
    def add_to_dict(self, key, field, value):
        if key not in self.data:
            self.data[key] = {}
        if field not in self.data[key]:
            self.data[key][field] = {}
        self.data[key][field][value] = True
        
    def add(self, key, field, value):
        if key not in self.data:
            self.data[key] = {}
            
        if field not in self.data[key]:
            self.data[key][field] = value
        else:
            self.data[key][field] += value
            
if __name__ == "__main__":
    pass