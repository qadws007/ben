
# 引入技术指标数据
from __future__ import (absolute_import ,division, print_function, unicode_literals)
import datetime  # 用于datetime对象操作
import os  # 用于管理路径
import sys  # 用于在argvTo[0]中找到脚本名称
import backtrader as bt  # 引入backtrader框架
import sys  # 用于在argvTo[0]中找到脚本名称
import backtrader as bt  # 引入backtrader框架
import pandas
from backtrader.feeds import GenericCSVData  # 用于扩展DataFeed
import backtrader.indicators as btind
import pymssql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


# #写入数据
# engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8".format('root', 'root', 'localhost:3306', 'stock_datas_factor'))
#
# conn = engine.connect()
# %matplotlib inline


def get_data(code='sz000001', start_date='2021-01-01', end_date='',what='*' ,where=""):
    if end_date=='':
        end_date=datetime.now().strftime('%Y-%m-%d')
    try:
        # 数据获取函数
        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8".format('root', 'root', '192.168.1.3:3306', 'stock_datas_factor'))
        sql_query = "select {} from `{}` where date>= '{}' and  date<= '{}' {}".format(what,code, start_date, end_date,where)
        #print(sql_query)
        data = pd.read_sql(sql_query, engine)
    except:
        print('sql查询有误',sql_query)
        return pd.DataFrame()

    data.index = pd.to_datetime(data.date)
    data = data.sort_index()
    #data['datetime'] = pd.to_datetime(data.date)
    data = data.fillna(0)
    return data



# date_value_list = []
# 创建策略
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod1', 30),
        ('maperiod2', 55),
        ('maperiod3', 26),
        ('printlog', True),
        ('poneplot', False),  # 是否打印到同一张图
        ('pstake', 100)  # 单笔交易股票数据
    )

    def log(self, txt, dt=None, doprint=False):
        dt = dt or self.datas[0].datetime.date(0)
        # print('%s,%s' % (dt.isoformat(),txt))
        # print(self.datas[0])
        """策略的日志函数"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            # print('%s,%s' % (dt.isoformat(),txt))

    def __init__(self):
        self.inds = dict()

        # 多周期数据的调用
        #         self.ma15m = bt.talib.SMA(self.dnames.hs15m, timeperiod=self.p.period)
        #         self.ma1h = bt.talib.SMA(self.dnames.hs1h, timeperiod=self.p.period)
        #         self.ma1d = bt.talib.SMA(self.dnames.hs1d, timeperiod=self.p.period)

        for i, d in enumerate(self.datas):
            self.inds[d] = dict()
            self.inds[d]['ma1'] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.maperiod1)
            self.inds[d]['ma2'] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.maperiod2)
            #self.inds[d]['ma3'] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.maperiod3)

            self.inds[d]['w1'] = bt.ind.CrossOver(self.inds[d]['ma1'], self.inds[d]['ma2'])  # 交叉信号
            # 跳过第一只股票data，第一只股票data作为主图数据
            # if i >= 0:
            #     if self.p.poneplot:
            #         d.plotinfo.plotmaster = self.datas[0]


    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        # self.log('OPERATION PROFIT,GROSS %.2F,NET %.2F' %(trade.pnl,trade.pnlcomm))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # broker 提交/接受了，买/卖订单则什么都不做
            return

        # 检查一个订单是否完成
        # 注意: 当资金不足时，broker会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                str = '股票:%s, 已买入:%.2f, 数量:%.2f, 价值:%.2f, 手续费:%.2f' % (
                    order.data._name, order.executed.price, order.executed.size, order.executed.value,
                    order.executed.comm)
                self.log(str)

            elif order.issell():
                str = '股票:%s, 已卖出:%.2f, 数量:%.2f, 价值:%.2f, 手续费:%.2f' % (
                    order.data._name, order.executed.price, order.executed.size, order.executed.value,
                    order.executed.comm)
                self.log(str)
            # 记录当前交易数量
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            str = "\nsize:%f\nvalue:%f\ncash:%f" % (order.executed.size, order.executed.value, order.executed.value)
            # self.log('订单取消/保证金不足/拒绝'+str)

        # 其他状态记录为：无挂起订单
        self.order = None

    def prenext(self):
        self.next()

    def next(self):
        # print(self.datas[].data)

        # 获取当前持仓,决定是否继续加仓
        #         for d,self.stock_name in zip(self.datas,self.dnames):
        #             if not self.getposition(d).size:
        #                 if self.crossover[d] > 0:
        #                     self.buy(data=d)  # 买买买
        #             elif self.crossover[d] < 0:
        #                 # for name, value in vars(self.getposition(d)).items():
        #                 #     print('%s=%s' % (name, value),"////////////////")
        #                 # 加减仓规则，可以在next里写，也可以在sizer里写
        #                 self.log("现在是哪个股票：%s,现有持仓：%.2f,现在价格：%.2f,现有价值：%.2f,上次开仓价格：%.2f,当前剩余资金:%.2f"
        #                          %(self.stock_name,self.getposition(d).size,self.getposition(d).price,
        #                            self.getposition(d).size*self.getposition(d).price,self.getposition(d).adjbase,self.broker.getcash()))
        #                 self.close(data=d)  # 卖卖卖

        # 获取当天日期
        date = self.datas[0].datetime.date(0)
        # 获取当天value
        value = self.broker.getvalue()
        for i, d in enumerate(self.datas):

            dt, dn = self.datetime.date(), d._name  # 获取时间及股票代码
            # print(dt)
            pos = self.getposition(d).size

            #print('validday', validday)
            # print('sig1',sig1)
            if not pos:  # 不在场内，则可以买入  vol成交量， ref日前
                if self.inds[d]['w1'][0]==1 and d.close[0]<d.close[-1]*1.1-0.02:  # 如果金叉
                    #self.buy(data=d,size=cerebro.broker.getcash()/5)  # 买
                    self.order_target_value(data=d,target=cerebro.broker.getcash()/5,price=d.close)
                    self.log('%s,BUY CREATE, %.2f ,%s' % (dt, d.close[0], d._name))
                    # self.order = self.buy()
            elif self.inds[d]['w1'][0]==-1 and d.close[0]>d.close[-1]*0.9+0.02:
                    self.close(data=d)  # 卖
                    self.log('%s,SELL CREATE,%.2f,%s' % (dt, d.close[0], d._name))
            elif self.inds[d]['ma1'][0]<self.inds[d]['ma2'][0] and d.close[0]>d.close[-1]*0.9+0.02:
                self.close(data=d)  # 卖
                self.log('%s,SELL CREATE,%.2f,%s' % (dt, d.close[0], d._name))



# 印花税
class stampDutyCommissionScheme(bt.CommInfoBase):
    params = (
        ('stamp_duty', 0.001),  # 印花税率
        ('commission', 0.0002),  # 佣金率
        ('percabs', True),
    )

    def _gotcommission(self, size, price, pseudoexec):
        if size > 0:  # 买入，不考虑印花税
            return size * price * self.p.commission
        elif size < 0:  # 卖出，考虑印花税
            return -size * price * (self.p.stamp_duty + self.p.commission)
        else:
            return 0


from datetime import datetime
import akshare as ak
import baostock as bs
import pandas as pd
# import datetime
import backtrader.feeds as btfeeds

# 获取股票池数据
cerebro = bt.Cerebro()
# 添加策略
cerebro.addstrategy(TestStrategy)



# 获取根目录
path_root = os.getcwd()
def check_file_path(path):
    if not os.path.exists(path):
      os.makedirs(path)

#沪深300成分股
def update_stock_list(code="000300"):
    df_stk_list = ak.index_stock_cons(index="000300")
    check_file_path(path_root+"\datas")
    df_stk_list.to_csv(path_root +"\datas\\"+code+'_list.csv',encoding='utf8',index =False)
update_stock_list()


# 获取股票池数据
df_names=pandas.read_csv(path_root + r"\datas\000300_list.csv",dtype={'品种代码':str},encoding='utf8')
df_names.drop(columns=['品种名称','纳入日期'],inplace=True)
df_names=df_names[~df_names['品种代码'].str.startswith('688')]
#df_names=df_names[0:10]

for i, fname in zip(df_names.index,df_names['品种代码']):
    df = get_data(fname, what="date,open,high,low,close,volume")
    if df.empty:
        continue
    if len(df) < 55:
        continue
    # 转换某些股票含时区，再选取时间区间，然后再判断行高。
    #df['date'] = df['date'].dt.tz_localize(None)
    # if df.iloc[0:22,1].sum==None:
    #     df.iloc[0]=df.loc[df['open']!=None].ilco[0]
    #     print("此条开头全是空")
    try:
        # df.date = pd.to_datetime(df.date)
        data = btfeeds.PandasData(
        dataname=df,
        fromdate = datetime(2020, 1, 1),
        todate = datetime(2021, 12, 31),
        datetime = 0,
        open = 1,
        high = 2,
        low = 3,
        close = 4,
        volume = 5,
        openinterest = -1,
        )

        cerebro.adddata(data, name=fname)
        print("注入了", i, "个数据")
    except:
        print( i,"注入数据失败！")
        #print(df)
        continue


# 设置启动资金
cerebro.broker.setcash((i + 1) * 10000)
# 设置交易单位大小
cerebro.addsizer(bt.sizers.FixedSize,stake = 100)
#cerebro.addsizer(bt.sizers.AllInSizerInt, percents=1 / (i + 2))
# 设置佣金为千分之一
comminfo = stampDutyCommissionScheme(stamp_duty=0.001, commission=0.0002)
cerebro.broker.addcommissioninfo(comminfo)
# 不显示曲线
for d in cerebro.datas:
    d.plotinfo.plot = False
# 打印开始信息
print('Starting Portfolio Value: %.2f 万' % (cerebro.broker.getvalue() / 10000))

# 查看策略效果
# cerebro.run(maxcpus=10)
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
# back  = cerebro.run(maxcpus=12,exactbars=True,stdstats=False)

back = cerebro.run()
#print(back)
import warnings

warnings.filterwarnings('ignore')
strat = back[0]
portfolio_stats = strat.analyzers.getbyname('pyfolio')
returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
returns.index = returns.index.tz_convert(None)

import quantstats

quantstats.reports.html(returns, output='stats.html', title='Stock Sentiment',rf=0)


import webbrowser

f = webbrowser.open('stats.html')
# 打印最后结果
print('Final Profolio Value : %.2f' % cerebro.broker.getvalue())
# 不显示曲线
# for d in cerebro.datas:
#     d.plotinfo.plot = False
