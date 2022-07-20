# -*- coding: UTF-8 -*-
# @Create   : 2022/7/5 16:31
# @Author   : yh
# @Remark   : 连接池

import socket
import threading
import time

from mxsoftpy.exception import RPCConnError
from .constants import CONN_MAX

conn_pool_lock = threading.Lock()  # 连接池锁


class Connection(object):
    """
    对Socket链接做了一些封装
    """

    def __init__(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # conn最大超时时间
        sock.connect((host, port))
        self.__sock = sock
        self.__host = '%s:%s' % (host, port)

    def write(self, data) -> None:
        """
        向远程主机send数据
        """
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
        """
        读取远程主机的数据
        """
        return bytearray(self.__sock.recv(length))

    def close(self) -> None:
        """
        关闭连接
        """
        self.__sock.shutdown(socket.SHUT_RDWR)
        self.__sock.close()

    def remote_host(self) -> str:
        """
        返回此conn连接的ip和端口
        """
        return self.__host

    def fileno(self):
        """
        select.select()方法要求必须有fileno方法
        """
        return self.__sock.fileno()

    def __del__(self):
        """
        销毁实例时自动关闭连接
        """
        self.__sock.shutdown(socket.SHUT_RDWR)
        self.__sock.close()


class ConnectionPool(object):
    """
    连接池
    """

    def __init__(self):
        self._connection_pool = {}  # 存放连接池的字典

    def __wait_conn(self, host: str, time_out: int) -> tuple:
        """
        等待连接
        :param host: 要获取的host
        :param time_out: 超时时间
        """
        wait_conn_max = time_out * 2  # 最大连接重试次数：因为0.5秒重试一次，将wait_conn_max粗略设置为time_out的2倍
        while wait_conn_max > 0:
            time.sleep(0.5)
            for index, i in enumerate(self._connection_pool[host]):
                if not i['lock'].locked():
                    return i['conn'], i['lock'], index
            wait_conn_max -= 1
        else:
            raise RPCConnError('连接池已满、且超过最大等待时间')

    def get_conn(self, host: str, time_out: int) -> tuple:
        """
        从连接池获取socket、没有会自动新建
        :param host: 要获取的host
        :param time_out: 超时时间
        """
        if not self._connection_pool.get(host):
            return self.__new_conn(host)
        else:
            for index, i in enumerate(self._connection_pool[host]):
                if not i['lock'].locked():
                    return i['conn'], i['lock'], index
            else:
                with conn_pool_lock:
                    if len(self._connection_pool[host]) < CONN_MAX:
                        return self.__new_conn(host)

                return self.__wait_conn(host, time_out)

    def __new_conn(self, host: str) -> tuple:
        """
        新建连接
        :param host: 要连接的ip和端口
        """
        ip, port = host.split(':')
        conn, lock = Connection(ip, int(port)), threading.Lock()
        if not self._connection_pool.get(host):
            self._connection_pool[host] = [{'conn': conn, 'lock': lock}]
        else:
            self._connection_pool[host].append({'conn': conn, 'lock': lock})
        return conn, lock, len(self._connection_pool[host])

    def all_conn(self) -> dict:
        """
        返回整个连接池对象
        """
        return self._connection_pool


conn_pool = ConnectionPool()  # 全局连接池
from .heartbeat import heartbeat
heartbeat_monitor = threading.Thread(target=heartbeat, daemon=True)  # 开启线程进行心跳监测
heartbeat_monitor.start()
