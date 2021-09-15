# Copyright (c) 2015 Shiye Inc.
# All rights reserved.
#
# Author: linquan <linquan@shiyedata.com>
# Date:   15/10/14

import redis

import threading

from docs.config.reids_cfg.config import redis_url_cfg


class RedisClientPool(object):
    _conn_dict = {
        client_key: {"conn_pool": None, "mutex": threading.RLock()}
        for client_key in redis_url_cfg}

    @staticmethod
    def redis_client(client_key):
        redis_url = redis_url_cfg[client_key]
        conn = RedisClientPool._conn_dict[client_key]
        if conn["conn_pool"] is None:
            conn["mutex"].acquire()
            if conn["conn_pool"] is None:
                conn["conn_pool"] = RedisClientPool(redis_url)
            conn["mutex"].release()
        return conn["conn_pool"].create_client()

    def __init__(self, redis_url):
        self.__redis_connection_pool = redis.ConnectionPool.from_url(
            redis_url, decode_responses=True)

    def __del__(self):
        self.__redis_connection_pool.disconnect()

    def create_client(self):
        return redis.Redis(connection_pool=self.__redis_connection_pool)

    @staticmethod
    def string_delete_job_key(redis_code, redis_key):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        try:
            conn = RedisClientPool.redis_client(redis_code)
            redis_key = redis_key.dependents_key.replace(":dependents", "")
            return conn.delete(redis_key)
        except Exception as e:
            raise e
