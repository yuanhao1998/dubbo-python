# -*- coding: UTF-8 -*-
# @Create   : 2022/7/5 16:31
# @Author   : yh
# @Remark   : 连接池

import socket
import threading
import time
from queue import Queue

from .constants import CONN_MAX


class Connection(object):
    """
    对Socket链接做了一些封装
    """

    def __init__(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        self.__sock = sock
        self.__host = '%s:%s' % (host, port)

    def write(self, data) -> None:
        while 1:
            try:
                length = self.__sock.send(data)
                if length == len(data):
                    break
                else:
                    data = data[length:]
            except socket.error as e:
                if e.errno == 35:
                    time.sleep(.01)
                else:
                    raise e

    def read(self, length) -> bytearray:
        return bytearray(self.__sock.recv(length))

    def close(self) -> None:
        self.__sock.shutdown(socket.SHUT_RDWR)
        self.__sock.close()

    def remote_host(self) -> str:
        return self.__host

    def fileno(self):
        return self.__sock.fileno()

    def __del__(self):
        self.__sock.shutdown(socket.SHUT_RDWR)
        self.__sock.close()


class ConnectionPool(object):
    """
    连接池
    """

    def __init__(self):
        self._connection_pool = {}  # 存放连接池的字典

    def __init_conn(self, host: str) -> None:
        ip, port = host.split(':')
        if not self._connection_pool.get(host):
            self._connection_pool[host] = Queue(maxsize=CONN_MAX)
            for _ in range(CONN_MAX):
                self._connection_pool[host].put(Connection(ip, int(port)))

    def new_conn(self, host: str) -> Connection:
        ip, port = host.split(':')
        conn = Connection(ip, int(port))
        self._connection_pool[host].put(conn)
        return conn

    def get_conn(self, host: str, time_out: int) -> Connection:
        if not self._connection_pool.get(host):
            self.__init_conn(host)
        return self._connection_pool[host].get(timeout=time_out)

    def release_conn(self, host: str, conn: Connection) -> None:
        self._connection_pool[host].put(conn)

    def all_conn(self) -> dict:
        return self._connection_pool


conn_pool = ConnectionPool()  # 全局连接池
