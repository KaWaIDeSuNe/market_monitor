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
import talib
from sqlalchemy import text

from market_monitor import celery_app
import numpy as np
from sycm.log_utils.log_cli import log_setup
from sycm.sentry_tools import capture_exception
from sycm.db_utils.mysql.db import DB as MysqlDB
from sycm.db_utils.mysql.db import retry_on_deadlock_decorator
from sycm.date_utils import DateTimeUtil

from controllers.base import Base
from docs.config.mysql_cfg.config import CONFIG
from controllers.base_task.base_task import BaseMasterTask
from utils.email_util import send_email

log_setup('up_down_forecast')
logger = logging.getLogger(__name__)




class UpDownForecastMaster(BaseMasterTask, Base):
    """
        金叉死叉预警
    """
    def __init__(self):
        super(UpDownForecastMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_STOCK_TREND"
        self.__code_list = [
            "000001",
            "600276",
            "002352",
            "000333",
            "600779",
            "601012",
            "600600",
            "002241",
            # "512850",
            # "501061",
            # "513050",
            # "161005",
            # "163402",
            # "161725",
            # "270023",
            # "161022",
        ]

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
        # 1. 获取开盘价、收盘价、最高价、最低价
        # 2. 求均值
        data_list = list()
        for code in self.__code_list:
            new_df = ts.get_realtime_quotes(code)

            print(new_df)
            df = ts.get_hist_data(code, start='2020-08-01', end= date_str,
                                  ktype='D')
            # 提取收盘价
            closed = df['close'].values
            opens = df['open'].values
            highs = df['high'].values
            lows = df['low'].values
            closed = np.insert(closed, 0, new_df['price'])
            opens = np.insert(opens, 0, new_df['open'])
            highs = np.insert(highs, 0, new_df['high'])
            lows = np.insert(lows, 0, new_df['low'])
            vote_list = list()
            # 获取均线的数据，通过timeperiod参数来分别获取 5,10,20 日均线的数据。
            # 获取20日均线数据
            closed_ma20 = talib.SMA(closed[::-1], timeperiod=20)[::-1]
            opens_ma20 = talib.SMA(opens[::-1], timeperiod=20)[::-1]
            highs_ma20 = talib.SMA(highs[::-1], timeperiod=20)[::-1]
            lows_ma20 = talib.SMA(lows[::-1], timeperiod=20)[::-1]

            # 获取均线的数据，通过timeperiod参数来分别获取 5,21,55 日均线的数据。
            ma5 = talib.SMA(closed[::-1], timeperiod=5)[::-1]
            ma10 = talib.SMA(closed[::-1], timeperiod=10)[::-1]
            ma13 = talib.SMA(closed[::-1], timeperiod=13)[::-1]
            ma20 = talib.SMA(closed[::-1], timeperiod=20)[::-1]
            ma30 = talib.SMA(closed[::-1], timeperiod=30)[::-1]
            ma60 = talib.SMA(closed[::-1], timeperiod=60)[::-1]
            ma100 = talib.SMA(closed[::-1], timeperiod=100)[::-1]
            st_name = new_df["name"][0]
            cur_price = "%.2f" % float(new_df['price'][0])
            # flag =
            e_title = f"{st_name}/{code} ma5,ma10 金叉"
            msg = f"{st_name}当前价格为: {cur_price}"
            # print(ma5)
            # print(ma10)
            if ma5[0] > ma10[0] and ma5[1] < ma10[1]:
                e_title = f"{st_name}/{code} ma5,ma10 金叉"
                send_email(msg, e_title)
            if ma5[0] < ma10[0] and ma5[1] > ma10[1]:
                e_title = f"{st_name}/{code} ma5,ma10 死叉"
                send_email(msg, e_title)

            if ma10[0] > ma20[0] and ma10[1] < ma20[1]:
                e_title = f"{st_name}/{code} ma10,ma20 金叉"
                send_email(msg, e_title)
            if ma10[0] < ma20[0] and ma10[1] > ma20[1]:
                e_title = f"{st_name}/{code} ma10,ma20 死叉"
                send_email(msg, e_title)

            if ma30[0] > ma60[0] and ma30[1] < ma60[1]:
                e_title = f"{st_name}/{code} ma30,ma60 金叉"
                send_email(msg, e_title)
            if ma30[0] < ma60[0] and ma30[1] > ma60[1]:
                e_title = f"{st_name}/{code} ma30,ma60 死叉"
                send_email(msg, e_title)
            # send_email(msg, e_title)

        #     idx = len(vote_list)//2
        #     vote_list.sort()
        #     vote = vote_list[idx]
        #     data = {
        #         "TRADE_DATE": date_str,
        #         "CODE": self.__code_map[code],
        #         "NAME": self.__name_map[code],
        #         "TREND_TYPE": vote,
        #         "FINGER_ID": self._gen_graph_id([date_str, code]),
        #     }
        #     data_list.append(data)
        # return data_list

    @retry_on_deadlock_decorator
    def __save_data(self, data_list):
        fields = ['TRADE_DATE',"CODE", "NAME", "TREND_TYPE", "FINGER_ID"]
        actual_fields = ['TRADE_DATE',"CODE", "NAME", "TREND_TYPE", "FINGER_ID"]
        self.batch_upsert_by_raw_sql(self._db, self._table, fields,
                                     actual_fields, data_list)


change_info_master = celery_app.register_task(UpDownForecastMaster())

if __name__ == '__main__':
    date_str = "2020-10-23"
    UpDownForecastMaster().run()