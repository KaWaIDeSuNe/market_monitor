# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: zsx<zhaoshouxin@shiyejinrong.com>
# Date:   2018-01-09
import regex


class Currency(object):
    def __init__(self):
        self.cn_nums = {
            '〇': 0,
            '一': 1,
            '二': 2,
            '三': 3,
            '四': 4,
            '五': 5,
            '六': 6,
            '七': 7,
            '八': 8,
            '九': 9,
        }
        self.cn_units = {
            '十': 10,
            '百': 10 ** 2,
            '千': 10 ** 3,
            '万': 10 ** 4,
            '亿': 10 ** 8,
        }
        self.patterns = [
            regex.compile(
                r'(?P<bit>[千万亿]*)(?P<type>[元圆])|(?P<bit>[千万亿]+)(?P<type>[元圆]?)'),
            regex.compile(
                r'(?P<type>人民币|港币|港元|美元|欧元|英镑|日元|韩元|加元|澳元|新西兰元|泰铢|越南盾)'),
            regex.compile('(?P<num>\d+\.?\d+)')
        ]
        self.currency_type_dict = {
            '人民币': '人民币',
            '元': '人民币',
            '圆': '人民币',
            '港币': '港币',
            '港元': '港币',
            '美元': '美元',
            '欧元': '欧元',
            '英镑': '英镑',
            '日元': '日元',
            '韩元': '韩元',
            '加元': '加元',
            '澳元': '澳元',
            '泰铢': '泰铢',
            '越南盾': '越南盾',
            '新西兰元': '新西兰元',
        }

    def extract(self, _string):
        """
        提取金额及币种
        :return: 金额（单位：万元；数据类型：字符串）、币种
        """
        if not _string:
            return 0.0, '人民币'
        if _string.find(",") != -1:
            _string = _string.replace(",", "")
        bit, currency_type, number = None, None, None
        for pattern in self.patterns:
            match = pattern.search(_string)
            if match:
                d = match.groupdict()

                if d.get("bit"):
                    bit = self.cn_to_int(d["bit"])

                if d.get("type") in self.currency_type_dict:
                    currency_type = self.currency_type_dict[d["type"]]

                if d.get("num"):
                    number = d["num"]

        if bit is None:
            bit = 1
        if bit is not None and currency_type is None:
            currency_type = ''

        if number is None:
            number = ''
        else:
            number = float(number) * bit / (10 ** 4)

        number = self.float_to_str(number)

        return number, currency_type

    def cn_to_int(self, s):
        if s[0] in self.cn_units:
            s = '一' + s
        match = regex.fullmatch(
            '((?P<nums>[〇一二三四五六七八九]+)(?P<units>[十百千万亿]*))+',
            s)
        if not match:
            return None

        ans = 0
        for nums, units in zip(match.capturesdict()['nums'],
                               match.capturesdict()['units']):
            num = ''
            for n in nums:
                num += str(self.cn_nums[n])
            unit = 1
            for u in units:
                unit *= self.cn_units.get(u, 1)
            ans += int(num) * unit
        return ans

    @staticmethod
    def float_to_str(number):
        if isinstance(number, str):
            return number
        return "%.2f" % number

    def __call__(self, *args, **kwargs):
        return self.extract(args[0])


if __name__ == '__main__':
    txt = """50.000000万人民币	
    1000 万元	
    500 万元	
    100 万元	
    1001.000000万人民币	
    168.000000万人民币	
    500.000000万人民币	
    100 万元	
    200 万元	



    0.5万元人民币	
    500 万元	
    50 万元	
    10.000000万美元
    100 万元	
    60 万元	

    4万元人民币	

    50 万元	
    50.000000万人民币	

    10.000000万人民币	
    50 万元	
    10.000000万人民币	
    10.000000万人民币	
    100.000000万人民币	
    10.000000万人民币	
    5 万	
    50 万元 港元	
    30 万元	
    3.000000万人民币	
    5.000000万人民币	
    100 万元	
    50 万元	

    300.000000万人民币	
    108 万元	
    30.000000万人民币	
    50.000000万人民币	


    50 万元	

    10.000000万人民币	

    108.000000万人民币	
    50 万元	
    30.000000万人民币	
    50 万元	

    50 万元	


    3.000000万人民币	


    3.000000万人民币	


    30.000000万人民币	
    50.000000万人民币	
    3.000000万人民币	

    12.000000万美元	
    30.000000万人民币	

    200 万元	
    200 万元	
    10 万元	
    100.000000万人民币	
    200 万元	


    50.000000万人民币	
    50.000000万人民币	
    600 元 人民币	
    3.000000万人民币	
    10 万	

    60.000000万人民币	
    10.000000万人民币	

    150 万元	
    1000.000000万人民币	
    80.000000万人民币	

    100万元人民币	

    105.000000万人民币	
    50 万元	
    50 万元	
    30.000000万人民币	
    50.000000万人民币	
    300 万	
    (人民币)50万元	
    (人民币)110万元"""

    ext = Currency()
    # for t in txt.split('\n'):
    #     print(ext(t), t)
    print(ext('3.00 元/万元'))
