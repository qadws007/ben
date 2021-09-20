import time

import akshare as ak
import baostock as bs
import openpyxl.utils.datetime
import pandas as pd
import datetime
import traceback
import pymssql
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import os
import sys

class Control_data_base():

    def __init__(self):
        # bs.login()
        pd.set_option('max_columns', 1000)
        pd.set_option('max_row', 300)
        pd.set_option('display.float_format', lambda x: '%.5f' % x)
        self.path_root =os.path.dirname(os.getcwd())
        print(self.path_root)
        if not os.path.exists(self.path_root + "\datas"):
            os.makedirs(self.path_root + "\datas")
        # engine = create_engine('mssql+pymssql://sa:test@127.0.0.1/stock?charset=utf8')
        self.engine = create_engine(
            "mysql://{}:{}@{}/{}?charset=utf8".format('root', 'root', '192.168.1.3:3306', 'stock_datas_factor'))
        self.conn = self.engine.connect()

        self.stock_list = self.use_all()

    def use_all(self):
        df = pd.read_csv(self.path_root + r"\datas\list_stock_ak.csv", dtype={'代码': str}, encoding='utf8')
        df.drop(df.loc[df['名称'].str.contains('PT')].index, inplace=True)
        df.drop(columns=['Unnamed: 0', '名称'], inplace=True)
        df.columns = ['code']
        print(df.head(5))
        return df



    # 删除数据库下所有表
    def delete_table_all(self,table='stock_datas_factor'):
        word2 = ''
        word1 = r"SELECT concat('DROP TABLE IF EXISTS ','`',table_name,'`', ';') FROM information_schema.tables WHERE table_schema = '%s';" % table
        ret1 = self.engine.execute(word1).fetchall()
        for i in ret1:
            word2 = word2 + i[0]
        self.engine.execute(word2)


    def select_table_less_rows(self,stock_name:str,table='stock_datas_factor'):
        #stock_name=stock_name[1:]

        word2 = ''
        word1 = r"SELECT count(*) FROM `%s`"%stock_name
        ret1 = self.engine.execute(word1).fetchone()
        ret1[0]
        print("行",ret1[0])

    def select_table_error1(self, stock_name: str, table='stock_datas_factor'):
        # stock_name=stock_name[1:]
        word2 = "DROP TABLE `%s`"%stock_name
        word1 = r"SELECT `date` FROM `%s` order by `date` desc limit 10" % stock_name
        try:
            ret1 = self.engine.execute(word1).fetchall()
        except:
            return
        if 0<len(ret1)<9:
            self.engine.execute(word2)
            print(stock_name,"不满10行，已删除表！")
            return

        if ret1[0][0]-ret1[9][0]>datetime.timedelta(days=50):
            self.engine.execute(word2)
            print(stock_name,"10 id 差距50天，已删除表！")

    def select_table_error2(self, stock_name: str, table='stock_datas_factor'):
        # stock_name=stock_name[1:]
        word2 = "DROP TABLE `%s`"%stock_name
        word1 = r"SELECT `date` FROM `%s` order by `date` desc limit 130" % stock_name
        try:
            ret1 = self.engine.execute(word1).fetchall()
        except:
            return
        df=pd.DataFrame([list(i) for i in ret1]).std()/130
        if df.astype('timedelta64[D]')[0]>1:
            self.engine.execute(word2)
            print(stock_name, "120 std > 1，已删除表！")



def error1():
    a=Control_data_base()
    len1=len(a.stock_list['code'])-1
    for n,i in enumerate(a.stock_list['code']):
        u=i.rjust(6,'0')
        try:
            a.select_table_error2(u)
            print(n,"/",len1)
        except:
            print(traceback.print_exc())
            pass
    a.conn.close()

error1()


def less():
    a=Control_data_base()
    for i in a.stock_list['code']:
        u=i.rjust(6,'0')
        try:
            a.select_table_error1(u)
            #print()
        except:
            pass

