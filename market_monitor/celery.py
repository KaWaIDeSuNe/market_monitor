# -*- encoding:utf-8 -*-
# Author: ldq <15611213733@163.com>
# Date: 2020-09-14 17:19
# File :
from __future__ import absolute_import, unicode_literals

from celery import Celery
from django.conf import settings
from celery.backends.redis import RedisBackend
import os
# 获取当前文件夹名，即为该Django的项目名
from kombu import Queue, Exchange

# 获取当前文件夹名，即为该Django的项目名
project_name = 'market_monitor'
project_settings = '%s.settings' % project_name


def patch_celery(proj_name):
    RedisBackend.task_keyprefix = f'celery-task-meta-{proj_name}-'
    RedisBackend.group_keyprefix = f'celery-taskset-meta-{proj_name}-'
    RedisBackend.chord_keyprefix = f'chord-unlock-{proj_name}-'


patch_celery(project_name)

# 设置环境变量
os.environ.setdefault('DJANGO_SETTINGS_MODULE', project_settings)

# 实例化Celery
app = Celery(project_name)

# 使用django的settings文件配置celery
app.config_from_object('django.conf:settings', namespace='CELERY')

# Celery加载所有注册的应用
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.conf.task_queues = (
    Queue("default", Exchange('default'), routing_key="task.#"),
    Queue("market_monitor", Exchange('market_monitor'), routing_key="market_monitor.#"),
    Queue("test", Exchange('test'), routing_key="test.#"),
)

app.conf.task_routes = {
    'controllers.market_temp.tasks.*': {'queue': 'market_monitor'},
    'controllers.up_down_dis.tasks.*': {'queue': 'market_monitor'},
    'controllers.turnover_forecast.tasks.*': {'queue': 'market_monitor'},
    'controllers.up_down_forecast.tasks.*': {'queue': 'market_monitor'},
    'controllers.cross.tasks.*': {'queue': 'market_monitor'},
    'controllers.stock_model.tasks.*': {'queue': 'market_monitor'},
}

