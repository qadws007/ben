import time

import akshare as ak
import baostock as bs
import pandas as pd
import datetime
import traceback
import pymssql
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import os
import sys

bs.login()
# pd.set_option('max_columns', 1000)
# pd.set_option('max_row', 300)
# pd.set_option('display.float_format', lambda x: '%.5f' % x)
path_root = os.getcwd()
print(path_root)
if not os.path.exists(path_root + "\datas"):
    os.makedirs(path_root + "\datas")



# #写入数据
# engine = create_engine('mssql+pymssql://sa:test@127.0.0.1/stock?charset=utf8')
engine = create_engine(
    "mysql://{}:{}@{}/{}?charset=utf8".format('root', 'root', '192.168.1.3:3306', 'stock_datas_factor'))
conn = engine.connect()




# 删除数据库下所有表
def delete_table_all(engine, table='stock_datas_factor'):
    word2 = ''
    word1 = r"SELECT concat('DROP TABLE IF EXISTS ','`',table_name,'`', ';') FROM information_schema.tables WHERE table_schema = '%s';" % table
    ret1 = engine.execute(word1).fetchall()
    for i in ret1:
        word2 = word2 + i[0]
    engine.execute(word2)


# delete_table_all(engine)



def check_file_path(path):
    if not os.path.exists(path):
        os.makedirs(path)




# 沪深300成分股
def update_stock_list(code="000300"):
    df_stk_list = ak.index_stock_cons(index="000300")
    check_file_path(path_root + "\datas")
    df_stk_list.to_csv(path_root + "\datas\\" + code + '_list.csv', encoding='utf8', index=False)


# update_stock_list()



# ak下载所有指数、板块，股票，基金的代码

check_file_path(path_root + "\datas")


def list_stock_ak():
    df = ak.stock_zh_a_spot_em()
    df[['代码', '名称']].to_csv(path_root + r"\datas\list_stock_ak.csv", encoding='utf8')


def list_fund_ak():
    df = ak.fund_em_fund_name()
    # df.to_csv(path_root + r"\datas\list_fund_ak.csv")
    df[['基金代码', '基金简称']].to_csv(path_root + r"\datas\list_fund_ak.csv", encoding='utf8')


def list_index_ak():
    df = ak.stock_zh_index_spot()
    df[['代码', '名称']].to_csv(path_root + r"\datas\list_index_ak.csv", encoding='utf8')


# list_stock_ak()
# list_index_ak()



def use_hs300():
    df = pd.read_csv(path_root + r"\datas\000300_list.csv", dtype={'品种代码': str}, encoding='utf8')
    df.drop(columns=['品种名称', '纳入日期'], inplace=True)
    df.columns = ['code']
    print(df.head(5))
    return df


# df=use_hs300()




def use_all():
    df = pd.read_csv(path_root + r"\datas\list_stock_ak.csv", dtype={'代码': str}, encoding='utf8')
    df.drop(df.loc[df['名称'].str.contains('PT')].index, inplace=True)
    df.drop(columns=['Unnamed: 0', '名称'], inplace=True)
    df.columns = ['code']
    print(df.head(5))
    return df


df = use_all()




# 融资融券信息
# df_rzrq= ak.stock_margin_detail_sse(date="20210201")




def check_trade_day(day=datetime.datetime.now().date(), jg=16):
    start = day - datetime.timedelta(jg)
    #### 获取交易日信息 ####
    rs = bs.query_trade_dates(start_date=str(start), end_date=str(day))
    #### 打印结果集 ####

    result = rs.get_data()
    ret = result.loc[result['is_trading_day'] == '1'].tail(1)
    return ret.iloc[0, 0].replace('-', '')


end = check_trade_day()




# 查询个股在数据库最后一次记录的日期，以便更新 后面的日期的数据
def selcet_last_data(code, start='20100101'):
    global engine, end
    # end=check_trade_day()
    # end=datetime.datetime.now().strftime('%Y%m%d')

    try:
        ret = engine.execute(r"select date from `%s` order by date desc limit 1" % code)
        str = (ret.fetchone()[0] + datetime.timedelta(days=1)).strftime('%Y%m%d')
        return [str, end]
    except:
        return [start, end]




now = datetime.datetime.now()
len_df = len(df['code']) - 1
# 打乱顺序
df.sample(frac=1).reset_index(drop=True)

#df=df.loc[df['code']=='000002']
for n, i in enumerate(df['code'].iloc[::-1]):
    code_date = selcet_last_data(i)

    if code_date[0] == code_date[1]:
        print(i, code_date, '进行了', n, '/', len_df, '项', '无需更新，跳过！ 已用时', datetime.datetime.now() - now)
        continue

    if i[0:1] == '6':
        s = 'sh' + i
    else:
        s = 'sz' + i
    try:
        # 保存数据库
        dd = ak.stock_zh_a_daily(symbol=s, start_date=code_date[0], end_date=code_date[1], adjust="qfq")
        #print(dd)
        if dd.empty:
            print(i, code_date, '进行了', n, '/', len_df, '项', '无需更新，跳过！ 已用时', datetime.datetime.now() - now)
            pass
            continue

        ind = ak.stock_financial_analysis_indicator(stock=s).reset_index().rename(columns={'index': 'date'})
        ind['date'] = ind['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

        last = pd.merge(dd, ind, how='outer')
        last.fillna(method='ffill', inplace=True)
        last = last.sort_values(by='date')

        # 财务报表
        report = ak.stock_financial_report_sina(stock=i, symbol="现金流量表")
        report['报表日期'] = report['报表日期'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d'))
        report = report.rename(columns={'报表日期': 'date'})
        report = report.drop('单位', axis=1)

        last2 = pd.merge(last, report, how='outer')
        last2.replace('--', 'NaN', inplace=True)

        for j in last2.columns[1:]:
            last2[j] = last2[j].astype('float')
        last2.fillna(method='ffill', inplace=True)
        last2.fillna(0, inplace=True)
        last2 = last2.sort_values(by='date')
        last2['openinterset'] = last2['close'].pct_change()

        #去除财务数据的ipo之前的值
        last2=last2.loc[last2['date']>dd['date'].min()]

        # （1）        
        # 保存为csv格式
        # last2.to_csv(str(os.getcwd())+'\data\\'+s+'.csv')

        # （2）        
        # 存入数据库
        last2 = last2.rename(columns=lambda x: x.replace("(", "").replace(')', ''))
        last2.to_sql(name=i, con=engine, index=False, if_exists='replace')
        last2['code']=i
        last2.to_sql(name="datas_daily_all", con=engine, index=False, if_exists='replace')

        print(i, code_date, '进行了', n, '/', len_df, '项', '已用时', datetime.datetime.now() - now)

    except:
        s = sys.exc_info()
        print(i, "Error '%s' happened on line %d" % (s[1], s[2].tb_lineno))
        # print(traceback.print_exc())
        continue


conn.close()




