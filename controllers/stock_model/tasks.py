# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: ldq <liangduanqi@shiyejinrong.com>
# Date: 2020-04-01 15:14

import re
import logging
from datetime import datetime

from celery import Task
from celery import group
from bson.objectid import ObjectId
from celery.result import allow_join_result
import tushare as ts
import akshare as ak
import talib
from sqlalchemy import text
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
        选股模型
    """
    def __init__(self):
        super(StockModelMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_STOCK_TREND"

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
        df = ak.stock_zh_a_spot()
        data_list = list()
        for _, row in df.iterrows():
            code = row["code"]
            st_name = row["name"]
            trade = row["trade"]
            if code.startswith("688") or code.startswith("300"):
                continue
            his_df = ts.get_hist_data(code, start='2020-08-01', end= date_str,
                                  ktype='D')
            if his_df.empty:
                continue
            # 提取收盘价
            closed = his_df['close'].values
            closed = np.insert(closed, 0, trade)

            # 获取均线的数据，通过timeperiod参数来分别获取 5,21,55 日均线的数据。
            ma5 = talib.SMA(closed[::-1], timeperiod=5)[::-1]
            ma10 = talib.SMA(closed[::-1], timeperiod=10)[::-1]
            ma13 = talib.SMA(closed[::-1], timeperiod=13)[::-1]
            ma20 = talib.SMA(closed[::-1], timeperiod=20)[::-1]
            ma30 = talib.SMA(closed[::-1], timeperiod=30)[::-1]
            ma60 = talib.SMA(closed[::-1], timeperiod=60)[::-1]
            ma120 = talib.SMA(closed[::-1], timeperiod=120)[::-1]
            msg = f"{st_name}当前价格为: {trade}"

            if ma30[0] > ma60[0] and ma30[1] < ma60[1] and ma5[0] > ma10[0] and ma5[1] < ma10[1]:
                e_title = f"{st_name}/{code} ma5,ma10 金叉;ma30,ma60 金叉"
                send_email(msg, e_title)
            if trade > ma5[0] > ma10[0] > ma20[0] > ma30[0] > ma60[0] > ma120[0] and ma5[0] > ma10[0] and ma5[1] < ma10[1]:
                e_title = f"{st_name}/{code} ma5,ma10 金叉; 均线多头排列"
                send_email(msg, e_title)


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