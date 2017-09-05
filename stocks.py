#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Xiang Xiao
@contact: btxiaox@gmail.com
@site:  
@file: stocks.py 
@time: 13/8/17 01:21 
"""

from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy import text
from sqlalchemy import exc
import threading
import logging
import tushare as ts

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

#股票信息数据库
engine_stocks_info = create_engine('mysql://root:123321@127.0.0.1/stocks_info?charset=utf8')
#所有股票历史数据数据库
engine_stocks_hist = create_engine('mysql://root:123321@127.0.0.1/stocks_hist?charset=utf8')
conn_hist = engine_stocks_hist.connect()
conn_info = engine_stocks_info.connect()


def store_stocks_hist(stock_code):
    """
    获取code 为stock_code 的数据 using get_hist_data 为数据源
    :param stock_code:
    :return:
    """
    try:
        df = ts.get_hist_data(stock_code,)
        df.to_sql(stock_code, engine_stocks_hist,if_exists='replace',dtype={'date':types.DATETIME})
    except StandardError,e:
        print e
        print stock_code
        raise e

def store_get_stocks_hist():
    pass


def store_stocks_list():
    """
    获取全部股票信息列表,存储至stocks_info 的 stocks_list 里
    :return:
    """
    try:
        data = ts.get_stock_basics()
        data.to_sql('stocks_list', engine_stocks_info, if_exists='replace',
                dtype={'code': types.VARCHAR(data.index.get_level_values('code').str.len().max())})
    except StandardError,e:
        print e

def store_stock_hist_day_list():
    """
    store the all hsitorical data by day
    :return:
    """
    List = get_stocks_info()
    while len(List)!=0:
        for stock in List:
            try:
               store_stocks_hist(stock[0])
               logging.info("data of "+stock[0]+" has been download!")
               List.remove(stock)
               logging.info( "the list has "+str(len(List))+" left")
            except StandardError,e:
                print logging.stock[0]+" has been failed"


def add_amount_to_hist_tables():
    """
    将成交额度加入每日的交易数据
    :return:
    """
    List = get_stocks_info()
    while len(List)!=0:
        for stock in List:
            try:
                get_sotcks_hist(stock[0])
                _add_amount_to_a_stock(stock[0])
                List.remove(stock)
                logging.info("data of amount "+stock[0]+" has been add")
                logging.info("the list has "+str(len(List))+" left")
            except Exception,e:
                logging.warning(stock[0]+" has been failed")


def _add_amount_to_a_stock(stock):
    """
    为单一股票添加成交额
    :return:
    """
    logging.info( "starting doing stock "+stock+" .......")
    pre_data = get_sotcks_hist(stock)
    count = 0
    while True:
        try:
            new_data  =  ts.get_h_data(stock,start='2005-01-21',end='2017-08-15')
            break;
        except Exception,e:
            count = count+1
            logging.warning("failed in retriving data in stock "+stock+ " in "+str(count)+" times")
            logging.warning("[info] waitng for "+str(60+count*20)+" seconds to re do....")
            threading._sleep(60 +count* 20)
    while len(pre_data)!=0:
        try:
            pre_date = pre_data[0][0]
            date =  pre_date.strftime('%Y-%m-%d')
            amount = new_data.loc[pre_date:pre_date,['amount']]
            if amount.empty == True:
                pre_data.remove(pre_data[0])
                logging.error("[missing] "+date+" @Code : "+ stock+" is missing...")
                continue
            amount=amount.iat[0,0]
            rawsql = "UPDATE stocks_hist.`"+stock+"` SET amount =:amount WHERE date =:date "
            sql = text(rawsql)
            conn_hist.execute(sql,amount=amount,date = pre_date)
            pre_data.remove(pre_data[0])
            logging.debug("[success] "+date+" @Code : "+ stock+" has been updated!")
        except Exception,e:
            logging.error(date+" @Code : "+ stock+" has failed in updating!")


def add_colums_to_hist_tables(column):
    """
    添加列至历史数据表单中.
    :return:
    """
    List = get_stocks_info()
    while len(List)!=0:
        for stock in List:
            try:
                List.remove(stock)
                rawsql = "ALTER TABLE stocks_hist."+stock[0]+" ADD "+column+" varchar(30)"
                sql = text(rawsql)
                conn_hist.execute(sql)
                logging.info( "data of amount "+stock[0]+" has been add")
                logging.info( "the list has "+str(len(List))+" left")
            except Exception,e:
                logging.error( stock[0]+" has been failed")

def get_stocks_info(code=None):
    """
    根据股票代码获取信息
    :param code: if the stock_code is blank,fetch all
    :return:
    """
    rawsql = "SELECT * FROM stocks_info.stocks_list WHERE "
    if code != None:
        rawsql = rawsql + "code=:code"
    else:
        rawsql = rawsql + "1=1"
    sql = text(rawsql)
    return conn_info.execute(sql, code=code).fetchall()

def get_sotcks_hist(code,date=None):
    rawsql = "SELECT * FROM stocks_hist"
    if code != None:
        rawsql = rawsql+"."+str(code)
    if date!=None:
        rawsql = rawsql+" WHERE date=:date"
    sql = text(rawsql)
    return conn_hist.execute(sql,date = date).fetchall()


if __name__ == '__main__':
    # print get_sotcks_hist('000001')
    #  data  =  ts.get_h_data('000001',start='2017-04-30',end='2017-08-30')
    #  print data
    # print data.iloc[0,5]
    #add_colums_to_hist_tables()
    # while True:
    #     try:
    add_amount_to_hist_tables()
    #     except Exception,e:
    #         print "failed"
    # print get_stocks_info('000001')
    # print ts.get_day_all('2017-06-30')

    # '''
    # flush index
    # '''
    # indexs = ['sh','sz','hs300','sz50','zxb','cyb']
    # for index in indexs:
    #     store_stocks_hist(index)


