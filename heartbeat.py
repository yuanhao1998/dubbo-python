# -*- coding: UTF-8 -*-
# @Create   : 2022/7/6 14:30
# @Author   : yh
# @Remark   : 心跳
import time
import logging
from struct import pack

from mxsoftpy.exception import RPCConnError
from .codec.decoder import parse_response_head
from .conn import conn_pool
from .constants import CLI_HEARTBEAT_TAIL, CLI_HEARTBEAT_REQ_HEAD, DEFAULT_READ_PARAMS, CONN_MAX, CONN_TIME_OUT
from .util import get_invoke_id

dubbo_logger = logging.getLogger('dubbo')


def heartbeat():
    """
    心跳监测
    """

    while 1:
        time.sleep(15)
        conn_dict = conn_pool.all_conn()

        for host, queue in conn_dict.items():
            dubbo_logger.debug('dubbo_conn开始心跳：host: %s， queue: %s' % (str(host), queue.qsize()))
            max_conn = CONN_MAX
            while not queue.empty() and max_conn > 0:
                conn = queue.get(timeout=CONN_TIME_OUT)
                try:
                    heartbeat_stream(conn)
                except IOError:
                    conn = conn_pool.new_conn(host)
                finally:
                    conn_pool.release_conn(host, conn)
                    max_conn -= 1


def heartbeat_stream(conn):
    """
    主动请求心跳数据
    """
    conn.write(bytearray(CLI_HEARTBEAT_REQ_HEAD + list(bytearray(pack('!q', get_invoke_id()))) +
                         CLI_HEARTBEAT_TAIL))
    heartbeat_type, body_length = parse_response_head(conn.read(16))
    if heartbeat_type != 1:  # 接收到的数据不是dubbo的心跳响应
        raise RPCConnError('接收dubbo心跳数据错误')
    body_buffer = []
    while body_length > len(body_buffer):
        body_buffer.extend(list(conn.read(DEFAULT_READ_PARAMS)))
