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
        财务数据导出
    """
    def __init__(self):
        super(StockModelMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_STOCK_TREND"
        self.code_map = {
            "600031": "三一重工",
            "000157": "中联重科",
            "000425": "徐工机械",
            "600763": "通策医疗",
            "603259": "药明康德",
            "600036": "招商银行",
            "000001": "平安银行",
            "601318": "中国平安",
            "000998": "隆平高科",
            "600259": "新农开发",
            "000848": "承德露露",
            "603517": "绝味食品",
            "600519": "贵州茅台",
            "000858": "五粮液",
            "000568": "泸州老窖",
            "600809": "山西汾酒",
            "600702": "舍得酒业",
            "002665": "首航高科",

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
        # writer = pd.ExcelWriter('个股财务信息.xlsx')
        # for st_code, st_name in self.code_map.items():
        #     df = ak.stock_financial_analysis_indicator(st_code)
        #     df["报告期"] = list(map(lambda X:self.report_date_map(X), df.index.values))
        #     df = df[["报告期", "总资产利润率(%)", "主营业务利润率(%)", "总资产净利润率(%)", "主营业务成本率(%)", "非主营比重", "净资产收益率(%)", "主营业务收入增长率(%)", "净利润增长率(%)", "固定资产比重(%)", "资产负债率(%)"]]
        #
        #     df.to_excel(writer, st_name)
        # writer.save()
        df1 = ak.stock_em_hsgt_north_net_flow_in(indicator="沪股通")
        df2 = ak.stock_em_hsgt_north_net_flow_in(indicator="深股通")
        df1["sum"] = df1["value"] + df2["value"]
        df1 = df1[df1.date <= "2020-01-01"]
        print(df1)


    def report_date_map(self, index_name):
        m = index_name.split("-")[1]
        if m == "03":
            return "一季报"
        elif m == "06":
            return "半年报"
        elif m == "09":
            return "三季报"
        else:
            return "年报"





change_info_master = celery_app.register_task(StockModelMaster())

if __name__ == '__main__':
    date_str = "2020-10-23"
    StockModelMaster().run()