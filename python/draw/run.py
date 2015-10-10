#!/usr/bin/env python
#-*- coding:utf-8 -*-
import gc
gc.disable()
import sys
import time
import draw
WMATCHS = ["all"]
def run(argv):
    filepath = argv[1].strip()
    wmatch = "all"
    if len(argv) > 2:
        wmatch = argv[2]

    data = {"all":[]}
    print "wmatch = [%s]" % wmatch
    count = 0
    with open(filepath) as fin:
        while True:
            line = fin.readline()
            if not line:
                break

            _wmatch, ratio = line.strip().split("\t")
            if float(ratio) < 0.2 or float(ratio) > 1.6 or float(ratio) == 1.0:
                continue

            if _wmatch not in WMATCHS:
                WMATCHS.append(_wmatch)
                data[_wmatch] = []

            #if wmatch != "all":
            #    if wmatch != _wmatch:
            #        continue
            data["all"].append(float(ratio))
            data[_wmatch].append(float(ratio))

            count += 1
            if count % 1000000 == 0:
                print "processing %sth" % count, time.ctime()
    print WMATCHS
    fig = draw.pl.figure("result", figsize = (10, 10))
    rows = len(WMATCHS)
    colums = 1
    index = 1
    for wmatch in WMATCHS:
        print wmatch, rows, colums, index
        draw.Draw().hist(fig, data[wmatch], bins = 200, rows = rows, colum = colums, index = index, title = "wmatch = [%s]" % wmatch, xlim = [0.2, 1.6])
        index += 1

    fig.tight_layout()
    fig.savefig("result.png")
    fig.clf()

if __name__ == "__main__":
    #run(sys.argv)
    pl = draw.pl
    X = []
    Y = []
    t = 0.65
    for x in xrange(1000000+1):
        X.append(x)
        Y.append(x**t)
    pl.plot(X, Y)
    pl.savefig("result.png")
    pl.clf()
