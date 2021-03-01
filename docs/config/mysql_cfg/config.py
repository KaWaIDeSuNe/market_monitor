# -*- encoding:utf-8 -*-
# Author: ldq <15611213733@163.com>
# Date: 2020-09-14 17:19
# File :
import os

CONFIG = {
    'market_db': {
        'username': os.environ.get('MARKET_DB_USER', 'root'),
        'password': os.environ.get('MARKET_DB_PASSWORD', 'Tom&Jerry'),
        'host': os.environ.get('MARKET_DB_HOST', '172.20.117.55'),
        'port': os.environ.get('MARKET_DB_PORT', 63306),
        'database': os.environ.get('MARKET_DB_NAME', 'market')
    },
}
