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

log_setup('market_temp')
logger = logging.getLogger(__name__)




class MarketTempMaster(BaseMasterTask, Base):
    """
    市场温度
    """
    def __init__(self):
        super(MarketTempMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_MARKET_STATUS'
        self._db = "agucha"
        self._table = "T_MARKET_TEMP"

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
        print(date_str)
        sql_statement = """
            SELECT `PCHANGE_RANGE`, `COUNT` from `T_STOCK_PCHANGE_DISTRIBUTE` where 
            TRADE_DATE = '{}';
            """
        sql_query = sql_statement.format(date_str)

        sql_statement2 = """
            SELECT `CONST_VALUE` as constValue, `CONST_VALUE_DESC` as constValueDesc, 
            `arg1`, `arg2`, `arg3`, `ARG1_DESC`, `ARG2_DESC`, 
            `ARG3_DESC` from `DWD_ALGORITHM_CONS` where CONST_CODE = '1000';
            """
        sql_query2 = sql_statement2
        with MysqlDB(CONFIG[self._db]) as db:
            print(sql_query)
            cur = db.session.execute(text(sql_query))
            cur2 = db.session.execute(text(sql_query2))
            result_list = self.format_result(cur)
            print(result_list)
            arg_result_list = self.format_result(cur2)
            # 上涨股票数、涨停股票数、下跌股票数、跌停股票数、涨幅超3%股票数、总股票数
            rise_num = sum([int(result["COUNT"]) for result in result_list if result["PCHANGE_RANGE"] in ('110', "111", "112", "113")])
            print(rise_num)
            rise_stop_num = sum([int(result["COUNT"]) for result in result_list if result["PCHANGE_RANGE"] in ('100', )])
            fall_stop_num = sum([int(result["COUNT"]) for result in result_list if result["PCHANGE_RANGE"] in ('140', )])
            rise_3_num = sum([int(result["COUNT"]) for result in result_list if result["PCHANGE_RANGE"] in ('110', "111", "112")])
            total_num = sum([int(result["COUNT"]) for result in result_list if result["PCHANGE_RANGE"] in ('110', "111", "112", "113", "120", "130", "131", "132", "133", "150")])
            # 比值
            rise_ratio = rise_num/total_num
            rise_stop_diff_ratio = (rise_stop_num-fall_stop_num)/total_num
            rise_3_ratio = rise_3_num/total_num

            code_desc_map = dict(zip([arg_result["constValue"] for arg_result
                                      in arg_result_list],
                                     [arg_result["constValueDesc"] for
                                      arg_result in arg_result_list]))
            constValueList = list()
            print(rise_ratio)
            print(rise_stop_diff_ratio)
            print(rise_3_ratio)
            for arg_result in arg_result_list:
                constValue = arg_result["constValue"]
                arg1 = arg_result["arg1"]
                arg2 = arg_result["arg2"]
                arg3 = arg_result["arg3"]

                if eval(arg1.replace("X", str(rise_ratio)).replace("&", "and")):
                    constValueList.append(constValue)
                    print(arg1)
                    print(constValue)
                if eval(arg2.replace("X", str(rise_stop_diff_ratio)).replace("&", "and")):
                    constValueList.append(constValue)
                if eval(arg3.replace("X", str(rise_3_ratio)).replace("&", "and")):
                    constValueList.append(constValue)
            print(constValueList)

            constValueList.sort()
            market_temp = constValueList[1]
            data = {
                "TRADE_DATE": date_str,
                "TEMP": market_temp,
                "FINGER_ID": self._gen_graph_id([date_str,])
            }
            return [data,]

    @retry_on_deadlock_decorator
    def __save_data(self, data_list):
        fields = ['TRADE_DATE',"TEMP", "FINGER_ID"]
        actual_fields = ['TRADE_DATE',"TEMP", "FINGER_ID"]
        self.batch_upsert_by_raw_sql(self._db, self._table, fields,
                                     actual_fields, data_list)


change_info_master = celery_app.register_task(MarketTempMaster())

if __name__ == '__main__':
    date_str = "2020-10-19"
    MarketTempMaster().run(date_str=date_str)