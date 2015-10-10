#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import pylab as pl
import random
import gc
gc.disable()

#from matplotlib.font_manager import FontProperties
#font = FontProperties(fname=r"/home/users/luoruiyang/.jumbo/lib/python2.7/site-packages/matplotlib/mpl-data/fonts/ttf/STIXGeneralItalic.ttf", size=14) 

class Draw(object):

    def __init__(self):
        self.colors = "rbgy"

    def plot(self, lines, output, legend = None, l = 'best', title = None):
        '''散点图，lines为输入的线段条数列表'''
        for X, Y, color in lines:
            pl.plot(X, Y, self.colors[color])

        if legend is not None:
            pl.legend(legend, l)

        if title is not None:
            pl.title(title)

        pl.savefig(output)
        pl.clf()

    def plots(self):
        pass

    def hist(self, fig, data, title = None, legend = None, xlim = None, ylim = None, rows = 1, colum = 1, index = 1, bins = 10, x_label = None, y_label = None):
        '''直方图'''
        #fig = pl.figure(figname, figsize = figsize)
        ax = fig.add_subplot(rows, colum, index)
        ax.hist(data, bins)

        if xlim is not None:
            ax.set_xlim(xlim)

        if ylim is not None:
            ax.set_ylim(ylim)

        if legend is not None:
            ax.legend(legend, 'best')

        if title is not None:
            ax.set_title(title)

        if x_label is not None:
            ax.set_xlabel(x_label)
        
        if y_label is not None:
            ax.set_ylabel(y_label)
        #fig.tight_layout()
        #fig.savefig(output) 
        #fig.clf()

    def hists(self, data):
        pass
        
if __name__ == "__main__":
    types = sys.argv[2].strip()
    data = {"ubmq":[], "lq":[], "pr":[], "hitline":[], "ratio":[]}
    factor = 400.0
    import math
    count = 0.0
    count_0 = 0.0
    count_1 = 0.0
    with open(sys.argv[1]) as fin:
        while True:
            line = fin.readline()
            if not line:
                break

            lines = line.strip().split("\t")
            hit  = float(lines[3]) / 1e6
            if hit > 0:
                data["hitline"].append(hit)
            #ratio = float(lines[4])
            #if ratio > 0 and ratio < 1:
            #    data["ratio"].append(ratio)
            #data["ubmq"].append(float(ubmq)/1e6)
            #pr = float(pr)/1e6
            #if pr > 0 and pr < 1:
            #    data["pr"].append(pr)
            #count += 1
            #if pr == 0:
            #    count_0 += 1
            #if pr == 1:
            #    count_1 += 1
            #ret = math.sqrt(acp/factor)
            #if ret > 0.2:
            #    continue
    print count, count_0, count_1
    data = data[types]
    D = Draw()
    fig = pl.figure("result_%s" % types)
    D.hist (fig, data, bins = 200, title = u"winfo_%s_dis" % types, x_label = u"%s" % types, y_label = u"num")
    fig.savefig("result_%s.png" % types)
    fig.clf()
