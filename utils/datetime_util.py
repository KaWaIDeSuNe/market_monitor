# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: byf <baiyifang@shiyedata.com>
# Date:   2020/8/27

from datetime import datetime, date


class DatetimeUtil(object):

    @staticmethod
    def create_date(year, month, day):
        return datetime(year, month, day)

    @staticmethod
    def str_to_date(date_str, str_format="%Y-%m-%d"):
        return datetime.strptime(date_str, str_format)

    @staticmethod
    def date_to_str(target_date, str_format="%Y-%m-%d"):
        if not isinstance(target_date, (datetime, date)):
            return target_date
        if target_date.year >= 1900:
            return target_date.strftime(str_format)
        return datetime(1900, 1, 1).strftime(str_format)

    @staticmethod
    def get_datetime_now():
        return datetime.now()
