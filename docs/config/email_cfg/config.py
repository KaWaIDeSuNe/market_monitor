# -*- encoding:utf-8 -*-
# Author: ldq <15611213733@163.com>
# Date: 2020-09-14 17:19
# File :

from collections import namedtuple

MailInfo = namedtuple('MailInfo',
                      ['mail_host', 'mail_user', 'mail_pass', 'mail_to'])

mail_info = MailInfo(
    "smtp.163.com", "15611213733@163.com", "UXQAKNOQFZSJMTUO",
    [
    "liangduanqi@qq.com",
    # "13375132375@163.com",
    # "18600398364@163.com"
    ])