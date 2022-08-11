# -*- coding: UTF-8 -*-
# @Create   : 2022/7/5 16:09
# @Author   : yh
# @Remark   :
import random
import logging
from struct import unpack

from nacos import NacosClient
from mxsoftpy.exception import DataError, RPCConnError

from .codec.decoder import parse_response_head, Response
from .codec.encoder import Request
from .conn import conn_pool
from .constants import DEFAULT_READ_PARAMS

providers_host_dict = {}  # 存放providers对应host的字典
dubbo_logger = logging.getLogger('dubbo')


class NacosRegister(object):
    """
    根据特定的interface从nacos中取出与之相关的所有provider的host并且监听provider的变化
    """

    def __init__(self, hosts, username=None, password=None, endpoint=None, namespace_id=None, group_name=None):
        """
        创建nacos注册对象
        :param hosts: nacos 所在服务器和端口
        :param username: nacos用户名称
        :param password: 密码
        :param endpoint:
        :param namespace_id: nacos服务所处的命名空间
        :param group_name: 提供服务的接口所处的group（不一定是nacos上分组的名称）
        """
        self.nc = NacosClient(server_addresses=hosts, namespace=namespace_id, endpoint=endpoint,
                              username=username, password=password)
        self.namespace_id = namespace_id
        self.group_name = group_name

    def get_provider_host(self, interface, version):
        """
        根据服务名称获取到此服务某个provider的host
        :param interface: 接口名称
        :param version: 版本
        :return:
        """
        if interface not in providers_host_dict:
            self._get_providers_from_nacos(interface, version)
        return self._routing_with_wight(interface)

    def _get_providers_from_nacos(self, interface, version):
        """
        从nacos中根据interface获取到providers信息，并存入self.hosts
        :param interface: 接口名称
        :param version: 版本
        :return:
        """
        services = self.nc.list_naming_instance("providers:%s:%s:%s" % (interface, version, self.group_name))
        providers_host_dict[interface] = ['%s:%s' % (service.get('ip'), service.get('port')) for service
                                          in services["hosts"]]

    @staticmethod
    def _routing_with_wight(interface):
        """
        根据接口名称获取一个host
        :param interface:
        :return:
        """
        hosts = providers_host_dict.get(interface)
        if not hosts:
            raise DataError('没有获取到接口对应的ip')
        return random.choice(hosts)

    def close(self):
        self.nc.stop_subscribe()


class DubboClient(object):
    """
    用于实现dubbo调用的客户端
    """

    def __init__(self, interface, nacos_register: NacosRegister = None, version='1.0.0', dubbo_version='2.7.6',
                 host=None, group=None):
        """
        :param interface: 接口名，
        :param version: 接口的版本号
        :param dubbo_version
        :param nacos_register
        :param host: 远程主机地址，用于直连，例如：172.21.4.98:20882，但是传入nacos_register时会优先从nacos_register获取
        :param group: 服务名称所具有的分组
        """

        self.__interface = interface
        self.__version = version
        self.__dubbo_version = dubbo_version
        self.__nc_register = nacos_register
        self.__host = host
        self.__group = group

    def call(self, method, args=(), time_out=10):
        """
        执行远程调用
        :param method: 远程调用的方法名
        :param args: 方法参数
                    1. 对于没有参数的方法，此参数不填；
                    2. 对于只有一个参数的方法，直接填入该参数；
                    3. 对于有多个参数的方法，传入一个包含了所有参数的列表；
                    4. 当前方法参数支持以下类型：
                        * bool
                        * int
                        * long
                        * float
                        * double
                        * java.lang.String
                        * java.lang.Object
        :param time_out: 最大超时时间，单位：秒，默认为10秒
        """

        if not isinstance(args, (list, tuple)):
            args = [args]

        if self.__nc_register:
            host = self.__nc_register.get_provider_host(self.__interface, self.__version)
        else:
            host = self.__host

        conn_retry_max = 3  # conn错误连接最大次数
        while conn_retry_max > 0:
            conn = conn_pool.get_conn(host, time_out)
            try:
                # 发送请求
                conn.write(Request({
                    'dubbo_version': self.__dubbo_version,
                    'version': self.__version.replace(':', ''),
                    'path': self.__interface,
                    'method': method,
                    'arguments': args,
                    'group': self.__group
                }).encode())

                body_buffer = self.deal_recv_data(conn)  # 接收响应数据
                conn_pool.release_conn(host, conn)
                break
            except IOError:  # socket错误，重新生成
                del conn
                conn_pool.new_conn(host)
            except BaseException as e:
                conn_pool.release_conn(host, conn)
                raise e
            conn_retry_max -= 1

        return self._parse_response(bytearray(body_buffer))

    def deal_recv_data(self, conn) -> list:
        """
        处理响应数据
        :param conn: socket连接
        :return: 响应数据
        """
        heartbeat, body_length = self._parse_head(conn.read(16))  # 前16为响应头，从中获取响应体长度

        if heartbeat == 0:  # 只需要正常的数据，心跳数据不做处理
            body_buffer = []
            while body_length > len(body_buffer):
                body_buffer.extend(list(conn.read(DEFAULT_READ_PARAMS)))
            return body_buffer

        return []

    @staticmethod
    def _parse_head(data):
        """
        对dubbo响应的头部信息进行解析
        :param data: 要解析的数据
        """
        try:
            return parse_response_head(data)  # heartbeat 0 正常响应、1 心跳响应、2 心跳请求
        except RPCConnError as e:
            print(str(e))
            return 0, unpack('!i', data[12:])[0]

    @staticmethod
    def _parse_response(body):
        """
        对dubbo的响应数据进行解析
        :param body: 响应数据
        """
        res = Response(body)
        res.read_int()
        try:
            return res.read_next()
        except IndexError as e:
            dubbo_logger.error(eval(str(res)).decode())
