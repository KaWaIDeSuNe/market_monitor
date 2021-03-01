# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: ldq <liangduanqi@shiyejinrong.com>
# Date: 2020-04-01 17:12

import hashlib

import regex
from sqlalchemy import text
from sqlalchemy.engine import ResultProxy
from sycm.db_utils.mongo.data_services import DataServer
from sycm.db_utils.mysql.db import DB as MysqlDB, retry_on_deadlock_decorator

from docs.config.mongo_cfg.config import mongodb_db_cfg
from docs.config.mysql_cfg.config import CONFIG


class Base(object):
    def __init__(self):
        self._mongodb_data_server = DataServer(mongodb_db_cfg).get_instance()
        self._last_modify_time = None
        self._save_db = None
        self._save_table = None

    def _query_primary_key_range(self):
        sql_statement = """
            SELECT id, cId from {} where 
            id > {} and  modifyTime <= '{}' ORDER BY id ASC limit 1000;
            """
        obj_id, num = None, 0
        while True:
            if obj_id is None:
                sql_query = sql_statement.format(self._save_table, 0, self._last_modify_time)
            else:
                sql_query = sql_statement.format(self._save_table, obj_id, self._last_modify_time)
            with MysqlDB(CONFIG[self._save_db]) as db:
                cur = db.session.execute(text(sql_query))
                result_list = self.format_result(cur)
            if not result_list:
                break
            obj_id = result_list[-1]["id"]
            yield result_list

    def _query_primary_key_range_v2(self):
        sql_statement = """
            SELECT id, fingerId, dataStatus from {} where 
            id > {} and  modifyTime <= '{}' ORDER BY id ASC limit 1000;
            """
        obj_id, num = None, 0
        while True:
            if obj_id is None:
                sql_query = sql_statement.format(self._save_table, 0, self._last_modify_time)
            else:
                sql_query = sql_statement.format(self._save_table, obj_id, self._last_modify_time)
            with MysqlDB(CONFIG[self._save_db]) as db:
                cur = db.session.execute(text(sql_query))
                result_list = self.format_result(cur)
            if not result_list:
                break
            obj_id = result_list[-1]["id"]
            yield result_list

    def _del_data(self, del_set):
        if not del_set:
            return
        sql_query = (
            "update %s set isValid=0 where cId in :primary_key_list ")
        query_ste = sql_query % (self._save_table,)
        with MysqlDB(CONFIG[self._save_db]) as db:
            db.session.execute(text(query_ste), {"primary_key_list": list(del_set)})
            db.session.commit()

    def _del_data_v2(self, del_set):
        if not del_set:
            return
        sql_query = (
            "update %s set dataStatus=3 where fingerId in :primary_key_list ")
        query_ste = sql_query % (self._save_table,)
        with MysqlDB(CONFIG[self._save_db]) as db:
            db.session.execute(text(query_ste), {"primary_key_list": list(del_set)})
            db.session.commit()

    def _update_data(self, update_set):
        if not update_set:
            return
        sql_query = (
            "update %s set dataStatus=2 where fingerId in :primary_key_list")
        query_ste = sql_query % (self._save_table)
        with MysqlDB(CONFIG[self._save_db]) as db:
            db.session.execute(text(query_ste), {"primary_key_list": list(update_set)})
            db.session.commit()

    @staticmethod
    def format_result(result_proxy):
        """
        格式化数据库查出来的结果进行字典化和列表化

        :param result_proxy: 数据库查询出来的结果集
        :return list: 结果集列表，可能为空
        """
        results = None
        if isinstance(result_proxy, ResultProxy):
            if result_proxy.rowcount:
                results = []
                print('current solve data count:', result_proxy.rowcount)
                try:
                    for row in result_proxy:
                        results.append(dict(row))
                except Exception as e:
                    return result_proxy.rowcount
            else:
                results = []

        return results

    def _gen_graph_id(self, raw_value):
        """
        生成指纹标识（仅适用于基础数据类型[number,basestring]嵌套的结构）
        :param raw_value: 关联图
        :return: 指纹标识
        """
        result_str = ""
        if isinstance(raw_value, dict) is True:
            keys = sorted(raw_value.keys())
            result_str += "".join(keys)
            for key in keys:
                value = raw_value[key]
                result_str += self._gen_graph_id(value)
        elif isinstance(raw_value, list) is True:
            raw_value = [str(raw_v) for raw_v in raw_value]
            result_str += "".join(sorted(raw_value))
        else:
            result_str += str(raw_value)
        return self._hash(result_str)

    def _gen_common_graph_id(self, raw_value):
        """
        生成指纹标识 (适用于复杂的基础数据类型)
        :param raw_value: 关联图
        :return: 指纹标识
        """
        result_str = ""
        if isinstance(raw_value, dict) is True:
            keys = sorted(raw_value.keys())
            result_str += "".join(keys)
            for key in keys:
                value = raw_value[key]
                result_str += self._gen_graph_id(value)
        elif isinstance(raw_value, list) is True:
            list_str = list()
            for value in raw_value:
                list_str.append(self._gen_graph_id(value))
            result_str += "".join(sorted(list_str))
        else:
            result_str += str(raw_value)
        return self._hash(result_str)

    @staticmethod
    def _hash(raw_str):
        if isinstance(raw_str, bytes):
            raw_str = str(raw_str, encoding="utf-8")
        raw_str = raw_str.lower().encode('utf-8')
        hash_calculator = hashlib.md5()
        hash_calculator.update(raw_str)
        return hash_calculator.hexdigest()

    def generate_alias_name(self, data_list, field = 'cName'):
        name_list = [data[field] for data in data_list]
        query_schema = {
            "db_name": "raw_data",
            "collection_name": "company_alias_name",
            "query_condition": {'alias_name':{'$in': name_list}},
            "query_field": {"_id": 0, "alias_name": 1, "cur_name": 1},
        }
        query_result = self._mongodb_data_server.call("query_item", query_schema)
        return {_dict["alias_name"]: _dict["cur_name"] for _dict in query_result}

    def _get_a_company_code_list(self):
        query_condition = {
            "category": "ACompany",
            "is_valid": True
        }
        query_schema = {
            "db_name": "category_data",
            "collection_name": "company_category",
            "query_condition": query_condition,
            "query_field": {"_id": 0, "company_code": 1},
            # "limit_n":1
        }
        query_result = self._mongodb_data_server.call("query_item", query_schema)
        return [info["company_code"] for info in query_result]

    @classmethod
    def batch_upsert_by_raw_sql(cls, db_name, table, fields, actual_fields=None, datas=None):

        """
        ON DUPLICATE KEY UPDATE
        eg: table = 't_company' ,fields = ['cId','cFullName','cAbbrName'] actual_fields = ['cFullName','cAbbrName']
            datas = [
                    {'cFullName':'杭州一隅千象科技有限公司','cId':'6bca91a8ccd70d2778a9e4218aefabc4','cAbbrName':'一隅千象1'}
                    {'cFullName':'北京三浦百草绿色植物制剂有限公司','cId':'58bc346bbb498dbfdf8b4a5ce26d7607','cAbbrName':'三浦百草1'}
            ]

        :param session:
        :param table:
        :param fields:
        :param actual_fields:
        :param datas:
        :return:
        """
        if datas is None:
            datas = []
        if actual_fields is None:
            actual_fields = fields
        params = {
            'table': table,
            'fields': ','.join([f"`{v}`" for v in fields]),
            'fields_values': ','.join([f":{v}" for v in fields]),
            'actual_fields': ','.join([f' {v}=Values({v}) ' for v in actual_fields])
        }
        exe_datas = []
        for data in datas:
            _info = dict()
            for field in fields:
                _info[field] = data.get(field)
            exe_datas.append(_info)
        sql = """
              INSERT INTO {table} ( {fields} ) VALUES ( {fields_values} )  ON DUPLICATE KEY UPDATE {actual_fields}     
              """.format(**params)
        with MysqlDB(CONFIG[db_name]) as db:
            db.session.execute(text(sql), exe_datas)
            db.session.commit()

    def update_data(self, db_name, table, fields, actual_fields=None, datas=None):
        if datas is None:
            datas = []
        if actual_fields is None:
            actual_fields = fields

        exe_datas = []
        for data in datas:
            _info = dict()
            for field in fields:
                _info[field] = data.get(field)
            exe_datas.append(_info)
        for data in exe_datas:
            params = {
                'table': table,
                'fields': ','.join([f"`{v}`" for v in fields]),
                'fields_values': ','.join([f":{v}" for v in fields]),
                'actual_fields': ','.join(
                    [f' {v}=Values({v}) ' for v in actual_fields if data.get(v)])
            }
            sql = """
                  INSERT INTO {table} ( {fields} ) VALUES ( {fields_values} )  ON DUPLICATE KEY UPDATE {actual_fields}     
                  """.format(**params)
            with MysqlDB(CONFIG[db_name]) as db:
                db.session.execute(text(sql), [data, ])
                db.session.commit()

    @staticmethod
    def fetch_dict(raw_dict, field_list):
        result = dict()
        for field in field_list:
            if isinstance(field, tuple):
                raw_value = raw_dict.get(field[0], None)
                if raw_value is None:
                    continue
                if len(field) == 3:
                    raw_value = field[2](raw_value)
                if raw_value is not None:
                    result[field[1]] = raw_value
            else:
                raw_value = raw_dict.get(field, None)
                if raw_value is None:
                    continue
                result[field] = raw_value
        return result

    def remove_unicode_gt_3(self, value):
        if value:
            filter_s_list = list(s for s in value if len(s.encode("utf-8")) <= 3)
            return "".join(filter_s_list)

    @staticmethod
    def extract_money_2f(_string):
        patterns = [
            regex.compile(
                '(?P<bit>[千万亿]*)(?P<type>[元圆])|(?P<bit>[千万亿]+)(?P<type>[元圆]?)'),
            regex.compile(
                '(?P<type>人民币|港币|港元|美元|欧元|英镑|日元|韩元|加元|澳元|新西兰元|泰铢|越南盾)'),
            regex.compile('(?P<num>\d*\.?\d+)')
        ]

        if _string is None:
            return None
        _string = str(_string)
        bit, currency_type, number = None, None, None
        for pattern in patterns:
            match = pattern.search(_string)
            if match:
                d = match.groupdict()
                if d.get("type"):
                    currency_type = d["type"]
        return currency_type

    def _generate_invalid_data(self, table_name):
        sql_statement = (
            "SELECT id, fingerId from {} "
            "where id > {} and dataStatus = 3  ORDER BY id ASC limit 1000;")
        obj_id = None
        with MysqlDB(CONFIG[self._save_db]) as db:
            while True:
                if obj_id is None:
                    sql_query = sql_statement.format(table_name, 0)
                else:
                    sql_query = sql_statement.format(table_name, obj_id)
                result_list = db.session.execute(text(sql_query))
                data_list = self.format_result(result_list)
                if not data_list:
                    break
                finger_list = [data["fingerId"] for data in data_list]
                yield finger_list
                obj_id = data_list[-1]["id"]

    def _query_mysql_pre_data(self, table, last_modify_quarter):
        sql_statement = (
            "SELECT id, fingerId from {} "
            "where id > {} and modifyTime <= '{}' and dataStatus in (1, 2) "
            "ORDER BY id ASC limit 1000")
        obj_id, num = None, 0
        with MysqlDB(CONFIG[self._save_db]) as db:
            while True:
                if obj_id is None:
                    sql_query = sql_statement.format(table, 0, last_modify_quarter)
                else:
                    sql_query = sql_statement.format(table, obj_id, last_modify_quarter)
                result_list = db.session.execute(text(sql_query))
                data_list = self.format_result(result_list)
                if not data_list:
                    break
                finger_list = [data["fingerId"] for data in data_list]
                yield finger_list
                obj_id = data_list[-1]["id"]

    @retry_on_deadlock_decorator
    def _del_table_data(self, table, name_set):
        if not name_set:
            return
        sql = "update {} set dataStatus=3 where fingerId in :finger_str".format(
            table)
        with MysqlDB(CONFIG[self._save_db]) as db:
            db.session.execute(text(sql), {
                'finger_str': list(name_set)
            })
            db.session.commit()

    @retry_on_deadlock_decorator
    def _convert_table_data(self, table, finger_set):
        if not finger_set:
            return
        sql = "update {} set dataStatus=2 where fingerId in :finger_str".format(
            table)
        id_list = list(finger_set)
        for idx in range(0, len(id_list), 100):
            with MysqlDB(CONFIG[self._save_db]) as db:
                db.session.execute(text(sql), {
                    'finger_str': id_list[idx: idx+100]
                })
                db.session.commit()

    @retry_on_deadlock_decorator
    def _save_data(self, table, fields, data_list):
        try:
            self.batch_upsert_by_raw_sql(self._save_db, table,
                                         fields, datas=data_list)
        except:
            if len(data_list) > 1:
                for data in data_list:
                    self._save_data(table, fields, [data, ])


def remove_unicode_gt_3(value):
    if value:
        filter_s_list = list(s for s in value if len(s.encode("utf-8")) < 2)
        return "".join(filter_s_list)

if __name__ == '__main__':
    a = Base()._gen_graph_id(["重庆文化艺术职业学院", "2014"])
    print(a)

# 1b0954ed1a7545f62a87d414284ecae3