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
from sycm.log_utils.log_cli import log_setup
from sycm.sentry_tools import capture_exception
from sycm.db_utils.mysql.db import DB as MysqlDB
from sycm.db_utils.mysql.db import retry_on_deadlock_decorator
from sycm.date_utils import DateTimeUtil

from controllers.base import Base
from docs.config.mysql_cfg.config import CONFIG
from controllers.base_task.base_task import BaseMasterTask

log_setup('turnover_forecast')
logger = logging.getLogger(__name__)




class TurnoverForecastMaster(BaseMasterTask, Base):
    """
        成交额预测
    """
    def __init__(self):
        super(TurnoverForecastMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_STOCK_DEAL_INFO"
        self.__code_list = [
            "sh000001",
            "sz399001",
            "sz399606",
            "sh000688",
            "sh000300",
            "sh000905",
            "sh000906",
        ]
        self.__code_map = {
            "sh000001": "000001.XSHG",
            "sz399001": "399001.XSHE",
            "sz399606": "399006.XSHE",
            "sh000688": "000688.XSHG",
            "sh000016": "000016.XSHG",
            "sh000300": "000300.XSHG",
            "sh000905": "000905.XSHG",
            "sh000906": "000906.XSHG",
        }
        self.__name_map = {
            "sh000001": "上证指数",
            "sz399001": "深圳成指",
            "sz399606": "创业板指",
            "sh000688": "科创50",
            "sh000016": "上证50",
            "sh000300": "沪深300",
            "sh000905": "中证500",
            "sh000906": "中证800",
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
        # 1. 获取20个交易日的历史日期和20日总成交额
        # 2. 获取当日累计成交额数据以及最新分钟级时间，获取20日当前分钟级前累计成交总额
        # 3. 计算当日累计比值：20日当前分钟级前累计成交总额/20日总成交额
        # 4. 预测值为：当日累计成交额/累计比值，若当日累计成交额为0则预测值取20日总成交额的平均值
        import pandas as pd
        pd.set_option('display.max_columns', None)

        data_list = list()
        for code in self.__code_list:
            ########
            ff = ts.pro_bar("000001.SZ", ma=[5, 10])
            # print(ff)
            ########
            new_df = ts.get_realtime_quotes(code)
            print(new_df)
            cur_sum_money = new_df["amount"][0]
            cur_sum_volume = new_df["volume"][0]
            start_date_time = f"{new_df['date'][0]} 09:00:00"
            end_date_time = f"{new_df['date'][0]} {new_df['time'][0]}"
            df = ts.get_hist_data(code, start=DateTimeUtil.get_date_str_by_delta(date_str, -40), end= date_str,
                                  ktype='D')
            df["money"] = df["close"] * df["volume"]
            sum_money = sum(df["money"][1:20])
            date_list = df.index[0:19]
            sum_old_min_money = 0
            for his_date_str in date_list:
                end_date_time = re.sub("\d{4}-\d{2}-\d{2}", his_date_str, end_date_time)
                start_date_time = re.sub("\d{4}-\d{2}-\d{2}", his_date_str, start_date_time)


                old_min_df_5 = ts.get_hist_data(code, start=start_date_time, end=end_date_time,
                                 ktype='15')
                old_min_df_5["money"] = old_min_df_5["close"] * old_min_df_5["volume"]
                old_min_money = sum(old_min_df_5["money"])
                sum_old_min_money += old_min_money
            ratio = sum_old_min_money/sum_money
            new_day_money = float(cur_sum_money)/ratio
            new_day_volume = float(cur_sum_volume)/ratio
            data = {
                "AMOUNT": cur_sum_money,
                "TREND_AMOUNT": new_day_money,
                "TRADE_DATE": date_str,
                "VOLUME": cur_sum_volume,
                "TREND_VOLUME": new_day_volume,
                "CODE": self.__code_map[code],
                "NAME": self.__name_map[code],
                "RATIO": ratio,
                "FINGER_ID": self._gen_graph_id([date_str, code]),
            }
            data_list.append(data)
        return data_list

    @retry_on_deadlock_decorator
    def __save_data(self, data_list):
        fields = ['TRADE_DATE',"CODE", "NAME", "VOLUME", "TREND_VOLUME", "AMOUNT", "TREND_AMOUNT", "RATIO", "FINGER_ID"]
        actual_fields = ['TRADE_DATE',"CODE", "NAME", "VOLUME", "TREND_VOLUME", "AMOUNT", "TREND_AMOUNT", "RATIO", "FINGER_ID"]
        self.batch_upsert_by_raw_sql(self._db, self._table, fields,
                                     actual_fields, data_list)


change_info_master = celery_app.register_task(TurnoverForecastMaster())

if __name__ == '__main__':
    date_str = "2020-10-23"
    TurnoverForecastMaster().run()