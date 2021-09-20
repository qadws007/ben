import time
import akshare as ak
import baostock as bs
import pandas as pd
import datetime
import traceback
import pymssql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import tushare as ts
import os
import sys


class Factors():
    def factor_lb(self,df:pd.DataFrame,period:int=5,name:str='volume'):
        #量比
        #如果df的len等于5，将返回4个nan和最后一个平均值。
        ret=df[name].rolling(period).mean()/240
        return ret

