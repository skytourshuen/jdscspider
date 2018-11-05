import traceback
import configparser
import  requests
from bs4 import BeautifulSoup
import re
import redis
import sys
import threading
import pymysql
import time
import random

# 获取配置 mysql redis
cfg = configparser.ConfigParser()
cfg.read("../config.ini")

redis_con = ''  # redis连接
counter = 0  # 队列计数

session = None  # 定义会话信息
max_queue_len = int(cfg.get("sys", "max_queue_len"))  # 最大队列长度
sleep_time = float(cfg.get("sys", "sleep_time"))  # 睡眠时间
threadLock = None

# 头部信息带详细的用户信息 不使用缓存
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Host": "www.jdsc35.com",
    "Referer": "http://www.jdsc35.com/",
    "Origin": "http://www.jdsc35.com/",
    "Upgrade-Insecure-Requests": "1",
    "Content-Type": "application/json, text/plain, */*",
    "Pragma": "no-cache",
    "Accept-Encoding": "gzip, deflate",
    'Connection': 'close',
    'authorization': 'oauth c3cef7c66a1843f8b3a9e6a1e3160e20',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}

ua = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
)

class GetJdscUrl(threading.Thread):

    '''
    用线程取出url数据
    '''

    def __init__(self, threadID = 1, name = ''):

        # 多线程
        self.threadLock = threading.Lock()  # 互斥锁声明
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        try:
            print("线程" + str(threadID) + "启动")
        except Exception as err:
            print(err)
            print("线程" + str(threadID) + "启动失败")
        global threadLock
        threadLock = threading.Lock()

    def get_index_page(self):
        '''
        获取首页上的商户信息
        :return:
        '''
        index_url = 'http://www.jdsc35.com/Companys'
        try:
            index_html = session.get(index_url, headers = headers, timeout = 35)
        except Exception as err:
            # 出现异常重试
            print("获取页面失败")
            print(err)
            traceback.print_exc()
            return None
        finally:
            pass
        business = re.findall('\/Corporation\/Index\/\\d+\.html', index_html.text)
        return business

    def get_page_by_city(self):
        '''
        按城市获取url
        :return:
        '''

        index_url = 'http://www.jdsc35.com/index.html'
        try:
            index_html = session.get(index_url, headers = headers, timeout = 35)
        except Exception as err:
            # 出现异常重试
            print("获取页面失败")
            print(err)
            traceback.print_exc()
            return None
        finally:
            pass
        city_url = re.findall('\/Market\/List\/\d*.html', index_html.text)

        for city in city_url:
            time.sleep(sleep_time)  # 休息一下
            cyty = 'http://www.jdsc35.com' + city
            try:
                cyty_html = session.get(cyty, headers = headers, timeout = 35)
                business = re.findall('\/Corporation\/Index\/\\d+\.html', cyty_html.text)
                for s in business:
                    self.add_wait_business('http://www.jdsc35.com' + s)
                    # self.add_wait_business('http://www.jdsc35.com' + s)
                market = re.findall('\/Market\/Market\?market_id=\d+', cyty_html.text)
                for m in market:
                    market_html = session.get('http://www.jdsc35.com' + m, headers = headers, timeout = 35)
                    mbs = re.findall('\/Corporation\/Index\/\\d+\.html', market_html.text)
                    for s in mbs:
                        self.add_wait_business('http://www.jdsc35.com' + s)
                        # self.add_wait_business('http://www.jdsc35.com' + s)
            except Exception as err:
                print('请求出错:' + cyty)
                print(err)
                pass
        return

    def get_page_by_paty(self):
        '''
        获取高品城下的url
        http://www.jdsc35.com/index.html
            http://www.jdsc35.com/Market/List/17/1576.html
                /Market/Market?market_id=1804
                    /Corporation/Index/5628.html+
                    http://tshz.jdsc35.com+
                http://czhsgd.jdsc35.com+
                http://www.jdsc35.com
        :return:
        '''
        index_url = 'http://www.jdsc35.com/index.html'
        try:
            index_html = session.get(index_url, headers = headers, timeout = 35)
        except Exception as err:
            # 出现异常重试
            print("获取页面失败")
            print(err)
            traceback.print_exc()
            return None
        finally:
            pass
        path_url = re.findall('http\:\/\/www\.jdsc35\.com\/Market\/List\/\d+\/\d+\.html', index_html.text)
        for path in path_url:
            time.sleep(sleep_time)  # 休息一下
            try:
                path_html = session.get(path, headers = headers, timeout = 35)
                business = re.findall('http\:\/\/[^www][a-z]+\.jdsc35\.com', path_html.text)
                for s in business:
                    print(s)
                    self.add_wait_business(s)
                market = re.findall('\/Market\/Market\?market_id=\d+', path_html.text)
                for m in market:
                    market_html = session.get('http://www.jdsc35.com' + m, headers = headers, timeout = 35)
                    mbs = re.findall('\/Corporation\/Index\/\\d+\.html', market_html.text)
                    for s in mbs:
                        self.add_wait_business('http://www.jdsc35.com' + s)
                        print('http://www.jdsc35.com' + s)
                    mbst = re.findall('http\:\/\/[^www][a-z]+\.jdsc35\.com', market_html.text)
                    for s in mbst:
                        self.add_wait_business(s)
                        print(s)

            except Exception as err:
                print('请求出错:' + path)
                print(err)
                pass
        return

    def get_page_by_5jscw(self):
        '''
        按城市获取url
        :return:
        '''

        headers['Host'] = "www.5jscw.com"
        headers['Referer'] = "http://www.5jscw.com/"
        headers['Origin'] = "http://www.5jscw.com/"
        print(headers['Host'])
#        start_session()

        index_url = 'http://www.5jscw.com'
        try:
            index_html = session.get(index_url, headers = headers, timeout = 35)
        except Exception as err:
            # 出现异常重试
            print("获取页面失败")
            print(err)
            traceback.print_exc()
            return None
        finally:
            pass

        business_url = re.findall('http:\/\/[^www][a-z]+\.5jscw\.com\/', index_html.text)
        for bs in business_url:  # 首页地址
            self.add_wait_business(bs)  # 保存地址

        area_url = re.findall('http\:\/\/www\.5jscw\.com\/\/market\/\?action=list\&areaid=\d+', index_html.text)
        for area in area_url:  # 地区的url
            time.sleep(sleep_time)  # 休息一下
            # area = 'http://www.jdsc35.com' + area
            try:
                area_html = session.get(area, headers = headers, timeout = 35)
                city_url = re.findall('http\:\/\/www\.5jscw\.com\/market\?action=show\&marketid=\d+&areaid=\d+', area_html.text)
                for m in city_url:
                    city_html = session.get(m, headers = headers, timeout = 35)
                    business_url2 = re.findall('http\:\/\/www\.5jscw\.com\/index\.php\?homepage\=\w+', city_html.text)
                    for s in business_url2:
                        self.add_wait_business(s)  # 保存地址
            except Exception as err:
                print('请求出错:' + area)
                print(err)
                pass
        return

    def get_index_page_business(self):
        '''
        获取首页上的商户列表，存入redis
        :return:
        '''
        jdsc = GetJdscUrl()
        business = jdsc.get_index_page()
        for business in jdsc.get_index_page():
            self.add_wait_business('http://www.jdsc35.com' + business)
        return

    def add_wait_business(self, business_url):
        '''
        加入带抓取页面队列，先用redis判断是否已被抓取
        :param name_url:
        :return:
        '''
        # 判断是否已抓取
        if not redis_con.hexists('already_get_business', business_url):
            global counter
            counter += 1
            print(business_url + " 加入队列")
            redis_con.hset('already_get_business', business_url, 1)
            redis_con.lpush('business_queue', business_url)
            print("添加商户 " + business_url + "到队列")

    def run(self):
        # self.get_index_page_business()
        # self.get_page_by_city()
        # self.get_page_by_5jscw()
        self.get_page_by_paty()
        # print(self.name + ':线程启动')
        # self.entrance()
        # self.entrance()
        print('123')

def start_session():
    global session
    # 初始化session
    requests.adapters.DEFAULT_RETRIES = 5
    session = requests.Session()
    session.cookies = cookielib.LWPCookieJar(filename = 'cookie')
    session.keep_alive = False
    try:
        session.cookies.load(ignore_discard = True)
    except Exception as err:
        print('Cookie 未能加载:' + err)
    finally:
        pass

def start_redis():
    '''
    初始化redis连接
    :return:
    '''
    try:
        redis_host = cfg.get("redis", "host")
        redis_port = cfg.get("redis", "port")
        global redis_con  # 全局变量
        redis_con = redis.Redis(host = redis_host, port = redis_port, db = 0)
        # 刷新redis
        # self.redis_con.flushdb()
        print("redis启动")
    except Exception as err:
        print("请安装redis或检查redis连接配置")
        sys.exit()

def run():
    '''
    启动函数
    :return:
    '''
    start_session()

    start_redis()  # 启动redis连接

    # 启动抓取客户信息线程
    threads = []
    threads_num = int(1)
    for i in range(0, threads_num):
        m = GetJdscUrl(i, "thread" + str(i))
        threads.append(m)

    for i in range(0, threads_num):
        threads[i].start()

    for i in range(0, threads_num):
        threads[i].join()

    # jdcs_atoz.get_index_page_business()

if __name__ == '__main__':
    run()
