# -*- encoding:utf-8 -*-
# Author: ldq <15611213733@163.com>
# Date: 2020-09-14 17:19
# File :

from __future__ import absolute_import, unicode_literals
import pymysql
# 引入celery实例对象
from .celery import app as celery_app

pymysql.install_as_MySQLdb()
__all__ = ('celery_app',)
