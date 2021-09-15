# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: ldq <liangduanqi@shiyejinrong.com>
# Date: 2020-04-01 15:14

import re
import logging
from datetime import datetime

# from celery import Task
# from celery import group
# from bson.objectid import ObjectId
# from celery.result import allow_join_result
import tushare as ts
import akshare as ak
import talib
# from sqlalchemy import text
import numpy as np
import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


from market_monitor import celery_app

from sycm.log_utils.log_cli import log_setup
from sycm.sentry_tools import capture_exception
from sycm.db_utils.mysql.db import DB as MysqlDB
from sycm.db_utils.mysql.db import retry_on_deadlock_decorator
from sycm.date_utils import DateTimeUtil

from controllers.base import Base
from docs.config.mysql_cfg.config import CONFIG
from controllers.base_task.base_task import BaseMasterTask
from utils.email_util import send_email

log_setup('stock_model')
logger = logging.getLogger(__name__)




class StockModelMaster(BaseMasterTask, Base):
    """
        个股技术分析
    """
    def __init__(self):
        super(StockModelMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_STOCK_TREND"
        self.code_map = {
            "000915": "",
            # "600763": "通策医疗",
            # "601318": "中国平安",
            # "600132": "重庆啤酒",
            # "300782": "卓胜微",
        }

    def run(self, during=None, number=None, date_str = None):
        try:
            self._last_modify_time = datetime.now()
            date_base = DateTimeUtil.get_date_base(datetime.now())
            if not date_str:
                date_str = DateTimeUtil.date_to_str(date_base)
            data_list = self.data_process(date_str)
            if data_list:
                self.__save_data(data_list)
            logger.info("test_task_master solved")
        except Exception as e:
            capture_exception()
            raise e

    def data_process(self, date_str):
        # 1. 获取A股股票列表(过滤创业板和科创板)
        # 2. 求均值
        for st_code in self.code_map:

            df = ts.get_realtime_quotes(symbols = st_code)
            row = df.loc[0]
            code = row["code"]
            st_name = row["name"]
            trade = float(row["price"])
            high = row["high"]
            low = row["low"]
            if high == low:
                '''最高价与最低价相同，可能为停牌'''
                continue
            if code.startswith("688"):
                '''过滤科创板'''
                continue
            his_df = ak.stock_zh_a_hist(st_code, start_date='2021-01-01', end_date= date_str, adjust="qfq")
            if his_df is None:
                continue
            if his_df.empty:
                continue
            # 提取收盘价
            closed = his_df['收盘'].values
            # 获取均线的数据，通过timeperiod参数来分别获取 5,21,55 日均线的数据。
            ma5 = talib.SMA(closed, timeperiod=5)[::-1]
            ma10 = talib.SMA(closed, timeperiod=10)[::-1]
            ma12 = talib.SMA(closed, timeperiod=12)[::-1]
            ma13 = talib.SMA(closed, timeperiod=13)[::-1]
            ma20 = talib.SMA(closed, timeperiod=20)[::-1]
            ma26 = talib.SMA(closed, timeperiod=26)[::-1]
            ma30 = talib.SMA(closed, timeperiod=30)[::-1]
            ma60 = talib.SMA(closed, timeperiod=60)[::-1]
            ma120 = talib.SMA(closed, timeperiod=120)[::-1]
            MACD, MACDsignal,  MACDhist = talib.MACD(closed)
            print(f"ma5:{ma5}")
            print(f"ma10:{ma10}")
            # print(f"MACD:{MACD[::-1]}")
            # print(f"MACDsignal:{MACDsignal[::-1]}")
            # print(f"MACDhist:{MACDhist[::-1]}")


            # print(f"st_name:{st_name}")
            # print(f"trade:{trade}")
            # print(f"ma5:{ma5}")
            # print(f"ma10:{ma10}")
            e_title = f"{st_name}/{code}\n"
            msg = f"{st_name}当前价格为: {trade}\n"
            msg += f"详情页面：http://quote.eastmoney.com/{code}.html\n"
            if ma5[0] < ma10[0] and ma5[1] > ma10[1]:
                msg += "ma5,ma10 死叉\n"
            if ma5[0] < ma10[0] < ma20[0]:
                msg += "均线空头排列\n"
            if trade < ma5[0] and closed[1] > ma5[1]:
                msg += "跌破5日均线\n"
            if trade < ma10[0] and closed[1] > ma10[1]:
                msg += "跌破10日均线\n"
            if trade < ma20[0] and closed[1] > ma20[1]:
                msg += "跌破20日均线\n"
            # send_email(msg, e_title)


    @retry_on_deadlock_decorator
    def __save_data(self, data_list):
        fields = ['TRADE_DATE',"CODE", "NAME", "TREND_TYPE", "FINGER_ID"]
        actual_fields = ['TRADE_DATE',"CODE", "NAME", "TREND_TYPE", "FINGER_ID"]
        self.batch_upsert_by_raw_sql(self._db, self._table, fields,
                                     actual_fields, data_list)


change_info_master = celery_app.register_task(StockModelMaster())

if __name__ == '__main__':
    date_str = "2020-10-23"
    StockModelMaster().run()