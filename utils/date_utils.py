# -*- coding: utf-8 -*-
# @Time    : 2020/1/7 4:07 下午
# @Author  : Henson
# @Email   : henson_wu@foxmail.com
# @File    : utils.py

import datetime


def genarate_start_time(duration, num):
    start_time = datetime.datetime.now()
    if duration == 'seconds':
        start_time = start_time - datetime.timedelta(seconds=num)
    elif duration == 'minutes':
        start_time = start_time - datetime.timedelta(minutes=num)
    elif duration == 'hours':
        start_time = start_time - datetime.timedelta(hours=num)
    elif duration == 'days':
        start_time = start_time - datetime.timedelta(days=num)

    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    if duration == 'all':
        start_time = None
    return start_time

def genarate_start_datetime(duration, num):
    start_time = datetime.datetime.now()
    if duration == 'seconds':
        start_time = start_time - datetime.timedelta(seconds=num)
    elif duration == 'minutes':
        start_time = start_time - datetime.timedelta(minutes=num)
    elif duration == 'hours':
        start_time = start_time - datetime.timedelta(hours=num)
    elif duration == 'days':
        start_time = start_time - datetime.timedelta(days=num)
    if duration == 'all' or not duration:
        start_time = None
    return start_time


