# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: zsx <zhaoshouxin@shiyejinrong.com>
# Date:   2019-03-07

import smtplib
from email.mime.text import MIMEText

from docs.config.email_cfg.config import mail_info as m

class SchedulerError(RuntimeError):
    def __init__(self, time):
        self.time = time


class EmailUtil(object):
    def __init__(self):
        self.__mail_host = m.mail_host
        self.__mail_user = m.mail_user
        self.__mail_pass = m.mail_pass
        self.__mail_to = m.mail_to

    def send_email(self, email_title, email_content):
        if email_content is None or len(email_content) == 0:
            return
        email_struct = MIMEText(email_content, _subtype="plain",
                                _charset="gb2312")
        email_struct["Subject"] = email_title
        email_struct["From"] = "".join(["Datax Err", "<", self.__mail_user, ">"])
        email_struct["To"] = ";".join(self.__mail_to)
        # server = smtplib.SMTP()
        #linux
        server = smtplib.SMTP_SSL(self.__mail_host, 465)
        server.connect(self.__mail_host)
        server.login(self.__mail_user, self.__mail_pass)
        server.sendmail(
            email_struct["From"], self.__mail_to, email_struct.as_string())
        server.close()


def send_email(err_info, email_title="datax job error ."):
    email_content = err_info

    email_util = EmailUtil()
    email_util.send_email(email_title, email_content)

if __name__ == '__main__':
    send_email("2222")

