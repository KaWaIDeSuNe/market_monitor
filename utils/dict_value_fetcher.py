# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: jpwange <wangjunping@shiyedata.com>
# Date:   2017-6-26

import math

from datetime import datetime, date

from sycm.date_utils import DateTimeUtil

def dict_fetch(data, keys, default=None):
    ret = dict()
    for key_item in keys:
        if isinstance(key_item, tuple):
            key_path = key_item[0].split(".")
            value = data.copy()
            for key in key_path:
                value = value.get(key, default)
                if value == default:
                    break

            if len(key_item) > 2:
                value = (key_item[2](value)
                         if value != default else default)
            if value is None:
                continue
            ret[key_item[1]] = value
        else:
            if key_item not in data:
                if default is not None:
                    ret[key_item] = default
            else:
                ret[key_item] = data[key_item]
    return ret

def merge_two_dict(first_dict, second_dict, keys):
    for key in keys:
        if key in second_dict:
            first_dict[key] = second_dict[key]
    return first_dict

def ratio_transfer_mulp(x):
    return format((x * 100), ".2f") + "%"

def ratio_transfer(x):
    return format(x, ".2f") + "%"

def price_transfer(x):
    return (x
            if isinstance(x, basestring)
            else format(x * 100.0 / 100, ".2f"))

def ratio_mul(value):
    return value * 100

def transfer_analysis_score(num):
    if num == -1:
        return "-"
    num *= 10
    if num < 1:
        num = math.ceil(num)
    return format(round(num), ".0f")

def org_float_transfer(num):
    if isinstance(num, basestring):
        return num
    return format(num, ".2f")

def org_int_transfer(num):
    if isinstance(num, basestring):
        return num
    return str(num)

def field_cleaning(res_dict):
    '''
    清洗字段，把无效值去除（None，"无","null"）;
    :return: 去除无效字段后的字典
    '''
    clean_list = [None, ]
    result = dict()
    for k in res_dict:
        if res_dict[k] not in clean_list:
            if isinstance(res_dict[k], int):
                result[k] = res_dict[k]
            elif isinstance(res_dict[k], list):
                result[k] = res_dict[k]
            elif isinstance(res_dict[k], datetime):
                result[k] = DateTimeUtil.date_to_str(res_dict[k])
            elif isinstance(res_dict[k], date):
                result[k] = DateTimeUtil.date_to_str(res_dict[k])
            else:
                result[k] = res_dict[k].lstrip().replace('"', '')
    return result
