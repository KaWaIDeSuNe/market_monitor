# -*- coding: utf-8 -*-
# @Time    : 2020/3/27 5:24 下午
# @Author  : Henson
# @Email   : henson_wu@foxmail.com
# @File    : config.py
from market_monitor.settings import redis_host, redis_pwd


redis_url_cfg = {
    # 指纹去重
    "redis_1_6": "redis://:" + redis_pwd + "@" + redis_host + ":8410/6",
}