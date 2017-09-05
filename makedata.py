#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Xiang Xiao
@contact: btxiaox@gmail.com
@site:  
@file: makedata.py 
@time: 22/8/17 11:04 
"""
import stocks



def make_traning_data(code):
    hists = stocks.get_sotcks_hist(code)
    for hist in hists:
        hindex = hists.index(hist)
        if hindex>19 & (hindex+60) < len(hists):
            #当天交易日时间
            train_data = []
            train_data.append(hist[0])
            #5日平均
            train_data.append()



def get_100_turnover(hists, index):
    """
    获取100%换手率的交易时间
    :param hists:
    :param index:
    :return:
    """
    turnover = 0
    while(index > 0):
        turnover = hists[index][15]+turnover
        index = index - 1
        if turnover >100:
            break
    if turnover>100:
        return index
    else :
        return -1

def get_average_price_by_day():
    pass







if __name__ == '__main__':
    make_traning_data('000001')