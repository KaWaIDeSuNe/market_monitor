# -*- encoding:utf-8 -*-
# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: ldq <liangduanqi@shiyejinrong.com>
# Date: 2019-07-26 18:02


import json
import time
from datetime import datetime
import requests


class AreaUtils(object):
    def __init__(self):
        self.__area_request_url_template = (
            "http://restapi.amap.com/v3/geocode/geo?"
            "address={}&output=JSON&key=bcca9ff29f3f34473cf9129eeec173a4")
        self.__area_field_name_list = [
            "province", "city", "district", "adcode"]

    def extract_reg_area(self, address_str):
        if address_str is None:
            return dict()
        address_str = address_str.replace("#", "")
        address_str = "".join(address_str.split())
        if len(address_str) == 0:
            return dict()
        interface_url = self.__area_request_url_template.format(address_str)
        raw_area = self.__request_area_interface(interface_url)
        if raw_area is None:
            return dict()
        return self.__transform_area(raw_area)

    def __request_area_interface(self, interface_url, retry_count=3):
        try:
            response = requests.get(interface_url)
            # response = request.urlopen(raw_request)
            response_content = response.text
            response.close()
            response_content = json.loads(response_content)
            if self.__is_valid_area_response(response_content) is False:
                return None
            return response_content
        except Exception as e:
            print(str(e))
            if retry_count > 0:
                time.sleep(2)
                return self.__request_area_interface(
                    interface_url, retry_count-1)
            return None

    def __is_valid_area_response(self, response_content):
        status_code = response_content.get("status", "0")
        if status_code != "1":
            return False
        geocodes = response_content.get("geocodes", None)
        if geocodes is None or len(geocodes) == 0:
            return False
        for field_name in self.__area_field_name_list:
            if field_name not in geocodes[0]:
                return False
        adcode = geocodes[0]["adcode"]
        if isinstance(adcode, list) is True:
            return False
        return True

    def __transform_area(self, raw_area):
        province_name = raw_area["geocodes"][0]["province"]
        city_name = raw_area["geocodes"][0]["city"]
        district_name = raw_area["geocodes"][0]["district"]
        district_code = raw_area["geocodes"][0]["adcode"]

        reg_province_code = district_code[:2] + "0000"
        reg_province = province_name
        reg_city_code = district_code[:4] + "00"
        reg_city = city_name
        if isinstance(city_name, list) is True or province_name == city_name:
            reg_city_code = district_code
            reg_city = district_name

        result_item = dict()
        if isinstance(reg_province, list) is False:
            result_item["reg_province_code"] = reg_province_code
            result_item["reg_province"] = reg_province
        if isinstance(reg_city, list) is False:
            result_item["reg_city_code"] = reg_city_code
            result_item["reg_city"] = reg_city
        if isinstance(district_name, list) is False:
            result_item["reg_district_code"] = district_code
            result_item["reg_district"] = district_name
        return result_item



if __name__ == '__main__':
    a = AreaUtils().extract_reg_area("龙岩大道1号行政办公中心5楼")


    print(a)