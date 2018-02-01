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
import logging
import re

#日志
logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='myapp.log',
                filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def make_traning_data(code):
    hists = stocks.get_sotcks_hist(code)
    #hists_bar = stocks.get_stocks_info(code)
    for hist in hists:
        date = hist[0]

        hindex = hists.index(hist)
        turnover_100 = get_100_turnover(hist,hindex)
        if turnover_100 >0:
            if hindex>39 :
                logging.info("making data of Code: "+code+" in date: "+date)
                train_data = []
                #当天交易日时间
                train_data.append(hist[0])
                #当日平均
                train_data.append(get_average_price_by_day(hist))
                #达到100%换手率的时间
                train_data.append(turnover_100)
                #100%换手率内的平均价格
                train_data.append(get_average_price_by_100(hists,hindex))
                #40个交易日内的涨幅/大盘涨幅
                train_data.append()
            else:
                continue

        else:
            continue



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

def get_average_price_by_day(record):
    """
    通过初始记录记录每日的平均价格
    :param record:
    :return:
    """
    amount = record[16]
    volume = record[5]
    return amount/volume

def get_average_price_by_100(hists,index):
    """
    计算100%换手率的平均价格
    :param hists:
    :param index:
    :return:
    """
    turnover100 = get_100_turnover(hists=hists,index= index)
    if turnover100>0:
        return -1
    else:
        sum = 0
        count = 0
        while  count > turnover100:
            sum = sum + get_average_price_by_day(hists[index])
            index = index + 1
            count = count + 1
        return sum/turnover100


def check_market(code):
    """
    判断深市\沪市\创业板\中小板
    :param code:
    :return:
    """
    szPreCode = '000'
    shPreCode = '60'
    cybPreCode = '300'
    zxbPreCode = '002'
    if re.match(szPreCode,code)!=None:
        print 'sz'
        return 'sz'
    elif re.match(shPreCode,code)!=None:
        print 'sh'
        return 'sh'
    elif re.match(cybPreCode,code)!=None:
        print 'cyb'
        return 'cyb'
    elif re.match(zxbPreCode,code)!=None:
        print 'zxb'
        return 'zxb'
    else:
        raise Exception('unknown market')


def get_amount_of_increase_comparely(code,hists,index,days):
    """
    获取对比大盘信息的涨幅
    :param code: 股票代码
    :param hists: 历史数据集
    :param index: 当天数据
    :param days: 和多少天后的数据相比
    :return:
    """
    try:
        market = check_market(code)
    except:
        logging.error(code + " is in unknown market")














if __name__ == '__main__':
    check_market('002344')