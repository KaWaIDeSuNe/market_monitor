# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: ldq <liangduanqi@shiyejinrong.com>
# Date: 2020-04-01 15:14

import re
import logging
from datetime import datetime

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
from copy import deepcopy

log_setup('market_temp')
logger = logging.getLogger(__name__)




class UpDownDispMaster(BaseMasterTask, Base):
    """
    涨跌分布
    """
    def __init__(self):
        super(UpDownDispMaster, self).__init__()
        self.job_status = False
        self._redis_client_key = "redis_1_6"
        self.job_name = 'market_monitor#T_STOCK_PCHANGE_DISTRIBUTE'
        self._db = "agucha"
        self._table = "T_STOCK_PCHANGE_DISTRIBUTE"

    def run(self, during=None, number=None, date_str = None):
        try:
            self._last_modify_time = datetime.now()
            date_base = DateTimeUtil.get_date_base(datetime.now())
            if not date_str:
                date_str = DateTimeUtil.date_to_str(date_base)
            data_list = self.data_process(date_str)
            print(data_list)
            if data_list:
                self.__save_data(data_list)
            logger.info("test_task_master solved")
        except Exception as e:
            capture_exception()
            raise e

    def data_process(self, date_str):
        sql_statement = """
            SELECT `OPEN`, `CLOSE`, `HIGH_LIMIT`, `LOW_LIMIT`, `PCT_CHANGE` from `T_STOCK_1D` where 
            TRADE_DATE = '{}' and PAUSED = 0 order by PCT_CHANGE;
            """
        sql_query = sql_statement.format(date_str)
        print(sql_query)
        sql_statement2 = """
            SELECT count(1) as num from `T_STOCK_1D` where 
            TRADE_DATE = '{}' and PAUSED = 1;
            """
        sql_query2 = sql_statement2.format(date_str)
        with MysqlDB(CONFIG[self._db]) as db:
            cur = db.session.execute(text(sql_query))
            cur2 = db.session.execute(text(sql_query2))
            result_list = self.format_result(cur)

            print(result_list)
            result_list2 = self.format_result(cur2)
            num = result_list2[0]["num"]
            if not result_list:
                return
            data = {"TRADE_DATE": date_str}
            higt_data = deepcopy(data)
            low_data = deepcopy(data)
            up_7_data = deepcopy(data)
            down_7_data = deepcopy(data)
            up_5_7_data = deepcopy(data)
            down_5_7_data = deepcopy(data)
            up_3_5_data = deepcopy(data)
            down_3_5_data = deepcopy(data)
            up_0_3_data = deepcopy(data)
            down_0_3_data = deepcopy(data)
            flat_data = deepcopy(data)
            stop_data = deepcopy(data)
            med_data = deepcopy(data)
            data_list = [
                higt_data, low_data, up_7_data, down_7_data, up_5_7_data,
                down_5_7_data, up_3_5_data, down_3_5_data, up_0_3_data,
                down_0_3_data, flat_data, stop_data, med_data
            ]
            higt_data.setdefault("PCHANGE_RANGE", 100)
            higt_data.setdefault("COUNT", 0)
            low_data.setdefault("PCHANGE_RANGE", 140)
            low_data.setdefault("COUNT", 0)
            up_7_data.setdefault("PCHANGE_RANGE", 110)
            up_7_data.setdefault("COUNT", 0)
            down_7_data.setdefault("PCHANGE_RANGE", 133)
            down_7_data.setdefault("COUNT", 0)
            up_5_7_data.setdefault("PCHANGE_RANGE", 111)
            up_5_7_data.setdefault("COUNT", 0)
            down_5_7_data.setdefault("PCHANGE_RANGE", 132)
            down_5_7_data.setdefault("COUNT", 0)
            up_3_5_data.setdefault("PCHANGE_RANGE", 112)
            up_3_5_data.setdefault("COUNT", 0)
            down_3_5_data.setdefault("PCHANGE_RANGE", 131)
            down_3_5_data.setdefault("COUNT", 0)
            up_0_3_data.setdefault("PCHANGE_RANGE", 113)
            up_0_3_data.setdefault("COUNT", 0)
            down_0_3_data.setdefault("PCHANGE_RANGE", 130)
            down_0_3_data.setdefault("COUNT", 0)
            flat_data.setdefault("PCHANGE_RANGE", 120)
            flat_data.setdefault("COUNT", 0)
            stop_data.setdefault("PCHANGE_RANGE", 150)
            stop_data.setdefault("COUNT", num)
            med_data.setdefault("PCHANGE_RANGE", 160)
            med_data.setdefault("COUNT", result_list[len(result_list)//2]["PCT_CHANGE"])
            for result in result_list:
                OPEN = result["OPEN"]
                CLOSE = result["CLOSE"]
                HIGH_LIMIT = result["HIGH_LIMIT"]
                LOW_LIMIT = result["LOW_LIMIT"]
                PCT_CHANGE = result["PCT_CHANGE"]
                if CLOSE == HIGH_LIMIT:
                    higt_data["COUNT"] += 1
                    print((OPEN, CLOSE, HIGH_LIMIT, LOW_LIMIT))
                    print(PCT_CHANGE)

                if CLOSE == LOW_LIMIT:
                    low_data["COUNT"] += 1
                if PCT_CHANGE > 0.07:
                    up_7_data["COUNT"] += 1
                if PCT_CHANGE < -0.07:
                    down_7_data["COUNT"] += 1
                if (PCT_CHANGE <= 0.07) and (PCT_CHANGE > 0.05):
                    up_5_7_data["COUNT"] += 1
                if (PCT_CHANGE >= -0.07) and (PCT_CHANGE < -0.05):
                    down_5_7_data["COUNT"] += 1
                if (PCT_CHANGE <= 0.05) and (PCT_CHANGE > 0.03):
                    up_3_5_data["COUNT"] += 1
                if (PCT_CHANGE >= -0.05) and (PCT_CHANGE < -0.03):
                    down_3_5_data["COUNT"] += 1
                if (PCT_CHANGE <= 0.03) and (PCT_CHANGE > 0):
                    up_0_3_data["COUNT"] += 1
                if (PCT_CHANGE >= -0.03) and (PCT_CHANGE < 0):
                    down_0_3_data["COUNT"] += 1
                if OPEN == CLOSE:
                    flat_data["COUNT"] += 1
            print(data_list)
            for data in data_list:
                print(data)
                finger_id = self._gen_graph_id([data["TRADE_DATE"], data["PCHANGE_RANGE"]])
                data["FINGER_ID"] = finger_id
            return data_list

    @retry_on_deadlock_decorator
    def __save_data(self, data_list):
        fields = ['TRADE_DATE',"PCHANGE_RANGE", "COUNT", "FINGER_ID"]
        actual_fields = ['TRADE_DATE',"PCHANGE_RANGE", "COUNT", "FINGER_ID"]
        self.batch_upsert_by_raw_sql(self._db, self._table, fields,
                                     actual_fields, data_list)


change_info_master = celery_app.register_task(UpDownDispMaster())


if __name__ == '__main__':
    date_str = "2020-10-23"
    UpDownDispMaster().run(date_str=date_str)

