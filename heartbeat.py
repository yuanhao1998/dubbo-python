# -*- coding: UTF-8 -*-
# @Create   : 2022/7/6 14:30
# @Author   : yh
# @Remark   : 心跳
import time
from struct import pack

from mxsoftpy.exception import RPCConnError
from .codec.decoder import parse_response_head
from .conn import conn_pool
from .constants import CLI_HEARTBEAT_TAIL, CLI_HEARTBEAT_REQ_HEAD, DEFAULT_READ_PARAMS
from .util import get_invoke_id


def heartbeat():
    """
    心跳监测
    # todo 异常处理，关闭异常socket
    """

    while 1:
        time.sleep(30)
        for i in conn_pool.all_conn().values():
            for j in i:
                if not j['lock'].locked():  # 对未在使用的socket进行心跳
                    with j['lock']:
                        heartbeat_stream(j['conn'])


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

# def heartbeat():
#     """
#     心跳监测
#     """
#
#     while 1:
#         time.sleep(10)
#         conns = []
#         try:
#             for i in conn_pool.all_conn().values():
#                 for j in i:
#                     if not j['lock'].locked():  # 对未在使用的socket监测心跳
#                         j['lock'].acquire()
#                         conns.append(j)
#
#             readable, writeable, exceptional = select.select([i['conn'] for i in conns], [], [], 0.5)
#         except select.error:
#             print('错误响应')
#             break
#         else:
#             for conn in readable:  # 对服务器的心跳请求进行响应
#                 heart_type, data = parse_response_head(conn.read(16))
#                 if heart_type == 2:
#                     print('响应心跳')
#                     msg_id = data[4:12]
#                     heartbeat_response = CLI_HEARTBEAT_RES_HEAD + list(msg_id) + CLI_HEARTBEAT_TAIL
#                     conn.write(bytearray(heartbeat_response))
#         finally:
#             for i in conns:
#                 i['lock'].release()
#
#         print('循环一轮')
