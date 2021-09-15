# -*- coding: utf-8 -*-
# @Time    : 2020/4/8 5:49 下午
# @Author  : Henson
# @Email   : henson_wu@foxmail.com
# @File    : base_task.py
import logging
from datetime import datetime
from abc import ABCMeta

from celery import Task
from sycm.log_utils.const import LoggerHandlerEnum
from sycm.scheduler_utils import scheduler_cli

logger = logging.getLogger(LoggerHandlerEnum.FILEANDCONSOLE.value)


class BaseMasterTask(Task, metaclass=ABCMeta):
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        job_info = {
            'job_name': self.job_name,
            'job_token': self.request.id,
            'job_status': self.job_status,
            'update_time': datetime.now().timestamp()
        }
        scheduler_cli.report_job_state(pro_name='business_system', msg=job_info)
        logger.info(job_info)
        return


class BaseSlaveTask(Task):
    pass
