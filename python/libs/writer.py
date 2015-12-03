#!/usr/bin/env python
import os
import timer

class WriterMode:
    append = "a"
    append_binary = "ab"
    write = "w"
    write_binary = "wb"

writer_mode = WriterMode()

class WriterNode(object):
    def __init__(self, path, mode, bak = False):
        self.path = path
        self.done_path = ".".join([self.path, "done"])
        self.mode = mode
        self.bak = bak

class Writer(object):

    def __init__(self, writer = {}):
        self.writer = writer
        self.writer_instance = {}
        self.init_writer()

    def init_writer(self):
        for key in self.writer:
            writer_node = self.writer[key]
            if os.path.exists(writer_node.path) and writer_node.bak:
                os.system("mv %s %s.%s" % (writer_node.path, writer_node.path, timer.get_today()))

            if os.path.exists(writer_node.done_path) and writer_node.bak:
                os.system("mv %s %s.%s" % (writer_node.done_path, writer_node.done_path, timer.get_today()))

            if not os.path.exists(os.path.dirname(writer_node.path)):
                os.makedirs(os.path.dirname(writer_node.path))
            self.writer_instance[key] = open(writer_node.path, writer_node.mode)

    def close(self):
        for key in self.writer_instance:
            self.writer_instance[key].close()
            writer_node = self.writer.get(key)
            os.system("touch %s" % writer_node.done_path)

    def get(self, key):
        return self.writer_instance.get(key, None)

    def write(self, key, line):
        if key not in self.writer_instance:
            return False, "key[%s] not in writer_instance" % key
        try:
            self.writer_instance.get(key).write(line)
            return True, "success"
        except Exception as info:
            return False, info

if __name__ == "__main__":
    pass
