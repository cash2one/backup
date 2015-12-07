#!/usr/bin/env python
#-*- coding:utf-8 -*-
import xlwt

class Excel(object):
    def __init__(self, xlwt_file = None):
        self.xlwt = xlwt_file
        if not self.xlwt:
            self.xlwt = xlwt.Workbook()

        self.line = 0

    def write_line(self, table, line):
        for index, colum in enumerate(line):
            table.write(self.line, index, colum)
        self.line += 1

    def inc_line(self):
        self.line += 1


    def clear(self):
        self.line = 0

    def add_sheet(self, name):
        return self.xlwt.add_sheet(name, cell_overwrite_ok = True)

    def write(self, data, name):
        self.clear()
        table = self.xlwt.add_sheet(name, cell_overwrite_ok = True)
        for line in data:
            self.write_line(table, line)

    def save(self, path):
        self.xlwt.save(path)

if __name__ == "__main__":
    excel = Excel()
    data = [(1,2,3), (2,3,4), (3,4,5), (4,5,6)]
    excel.write(data, u"基础数据")
    print "here1"

    data = [(u"姓名",u"年龄",u"性别"), (u"罗睿阳",28,u"男"), (u"岳斌",27,u"男11"), (u"金欣", 30, u"男")]
    excel.write(data, u"用户信息")
    print "here2"

    data = [(u"姓名",u"年龄",u"性别"), (u"罗睿阳",28,u"男"), (u"岳斌",27,u"男"), (u"金欣", 30, u"男")]
    table = excel.add_sheet(u"用户信息2")
    excel.clear()
    for item in data:
        excel.write_line(table, item)

    print "here3"

    excel.save("sheet_1.xls")
    print "end"

    excel = Excel()
    data = [(1,2,3), (2,3,4), (3,4,5), (4,5,6)]
    excel.write(data, u"基础数据")
    print "here1"

    data = [(u"姓名",u"年龄",u"性别"), (u"罗睿阳",28,u"男"), (u"岳斌",27,u"男11"), (u"金欣", 30, u"男")]
    excel.write(data, u"用户信息")
    print "here2"

    data = [(u"姓名",u"年龄",u"性别"), (u"罗睿阳",28,u"男"), (u"岳斌",27,u"男"), (u"金欣", 30, u"男")]
    table = excel.add_sheet(u"用户信息2")
    excel.clear()
    for item in data:
        excel.write_line(table, item)

    print "here3"

    excel.save("sheet_2.xls")
    print "end"
