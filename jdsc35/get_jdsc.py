import traceback
import configparser
import requests
from bs4 import BeautifulSoup
import re
import redis
import sys
import threading
import pymysql
import time
import random
import http.cookiejar as cookielib

# 获取配置 mysql redis
cfg = configparser.ConfigParser()
cfg.read("../config.ini")

redis_con = '' # redis连接
counter = 0 # 队列计数

session = None  # 定义会话信息
max_queue_len = int(cfg.get("sys", "max_queue_len"))  # 最大队列长度
sleep_time = float(cfg.get("sys", "sleep_time")) # 睡眠时间
threadLock = None

# 头部信息带详细的用户信息 不使用缓存
headers = {
    "User-Agent": """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  Chrome/63.0.3239.108 Safari/537.36""",
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
    '''Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) 
    Chrome/17.0.963.56 Safari/535.11''',
    '''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) 
    Chrome/53.0.2785.143 Safari/537.36''',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
)


class GetJdscCity():
    def get_index_page(self):
        """
        获取首页上的商户信息
        :return:
        """
        index_url = 'http://www.jdsc35.com/Companys/'
        try:
            index_html = session.get(index_url, headers=headers, timeout=35)
        except Exception as err:
            # 出现异常重试
            print("获取页面失败")
            print(err)
            traceback.print_exc()
            return None
        finally:
            pass
        business = re.findall('/Corporation/Index/\\d+\.html', index_html.text)
        return business

    def get_index_page_business(self):
        '''
        获取首页上的商户列表，存入redis
        :return:
        '''
        jdsc = GetJdscCity()
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

class GetRedisData(threading.Thread):
    '''
    用线程将缓存的地址请求取出数据
    '''
    def __init__(self, threadID=1, name=''):

        # 多线程
        self.threadLock = threading.Lock() # 互斥锁声明
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

        '''
        初始化数据库
        :return:
        '''
        # 初始化数据库连接
        try:
            db_host = cfg.get("db", "host")
            db_port = int(cfg.get("db", "port"))
            db_user = cfg.get("db", "user")
            db_pass = cfg.get("db", "password")
            db_db = cfg.get("db", "db")
            db_charset = cfg.get("db", "charset")
            # db.ping(reconnect=True)
            self.db = pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_db,
                                 charset=db_charset)
            self.db_cursor = self.db.cursor()
            print("mysql连接成功")
        except Exception as err:
            print("请检查数据库配置")
            sys.exit()

    def del_already_business(self, business_url):
        '''
         获取页面出错移出redis
        :param name_url:
        :return:
        '''
        global threadLock
        global counter
        threadLock.acquire()
        if not redis_con.hexists('already_get_business', business_url):
            counter -= 1
            redis_con.hdel('already_get_business', business_url)
        threadLock.release()

    def html_resolver_1(self, table, BS, business_url):
        companyName = re.search(r'[\w]+', BS.find(name='title').text).group()  # 公司名
        productCategory_list = BS.find(name='div', attrs={'class': "main_l"}).findAll('a')
        if productCategory_list is not None:
            productCategory = ''
            for i in productCategory_list:
                productCategory += ',' + i.text
            productCategory = productCategory[1:]
        address = to_str(re.search(r'地址[\w\W]+?\n(.+)', table.text))  # 地址
        contacts = re.search(r'联系方式：(.+?)(\d.+)', table.text)
        if contacts != None:
            contact = contacts.group(1)  # 联系人
            contactNumber = contacts.group(2)  # 联系电话
        else:
            contact = ''
            contactNumber = ''
        hotline = to_str(re.search(r'订货热线.+?(\d.+)', table.text))  # 订货热线
        source = business_url  # 来源
        url = to_str(re.search(r'网址[\w\W]+?(http.+)', table.text))  # 网址
        fax = to_str(re.search(r'传真[\w\W]+?(\d.+)', table.text))  # 传真
        createTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 创建时间
        updateTime = createTime  # 修改日期

        replace_data = \
            (companyName, productCategory, address, contact, contactNumber, hotline, source,
             url, fax, createTime, updateTime)
        return replace_data

    def html_resolver1_2(self, table, BS, business_url):
        companyName = re.search(r'[\w]+', BS.find(name='title').text).group()  # 公司名
        productCategory_list = BS.find(name='div', attrs={'class': "pro"}).findAll('a')
        if productCategory_list is not None:
            productCategory = ''
            for i in productCategory_list:
                productCategory += ',' + re.search(r'[\w]+', i.text).group()
            productCategory = productCategory[1:]
        address = to_str(re.search(r'地址[\w\W]+?\n(.+)', table.text)).replace(' ', '')  # 地址

        contact = to_str(re.search(r'联系人[\w\W]+?\n(.+)', table.text)).replace(' ', '')  # 地址

        contactNumber = to_str(re.search(r'手机\W+?(\d.+)', table.text))

        hotline = to_str(re.search(r'电话\W+?(\d.+)', table.text))  # 订货热线
        source = business_url  # 来源
        url = to_str(re.search(r'网址[\w\W]+?(http.+)', table.text))  # 网址
        fax = to_str(re.search(r'传真[\w\W]+?(\d.+)', table.text))  # 传真
        createTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 创建时间
        updateTime = createTime  # 修改日期

        replace_data = \
            (companyName, productCategory, address, contact, contactNumber, hotline, source,
             url, fax, createTime, updateTime)
        return replace_data

    def html_resolver1_3(self, table, BS, business_url):
        # list = soup.find_all("ul", attrs={'class': ['fl', 'f14', 'list-box', 'best-list', 'mt10', 'li']})
        # re.search(r'[\w]+', BS.find(name='title').text).group()
        test = BS.find(name='div', attrs={'class': 'noLogo'})
        companyName = re.search(r'[\w]+', test.text).group() # 公司名
        productCategory_list = BS.find(name='ul', attrs={'class': "nav-city"}).findAll('a')
        if productCategory_list is not None:
            productCategory = ''
            for i in productCategory_list:
                productCategory += ',' + re.search(r'[\w]+', i.text).group()
            productCategory = productCategory[1:]
        address = to_str(re.search(r'地址：(.+)', table.text)).replace(' ', '')  # 地址

        contact = to_str(re.search(r'联系人：(\w+)', table.text)).replace(' ', '')  # 联系人

        contactNumber = to_str(re.search(r'手机：(.+)?）', table.text))

        hotline = to_str(re.search(r'服务热线[\w\W]+?(\d.+)', table.text))  # 订货热线
        source = business_url  # 来源
        url = to_str(re.search(r'更多信息(.+)', table.text))  # 网址
        fax = to_str(re.search(r'传真[\w\W]+?(\d.+)', table.text))  # 传真
        createTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 创建时间
        updateTime = createTime  # 修改日期

        replace_data = \
            (companyName, productCategory, address, contact, contactNumber, hotline, source,
             url, fax, createTime, updateTime)
        return replace_data

    def get_business_info(self, business_url):
        time.sleep(sleep_time)
        '''
        获得页面信息并解析
        :param url:
        :return:
        '''
        if re.search(r'jdsc35', business_url) != None:
            headers['Host'] = "www.jdsc35.com"
            headers['Referer'] = "http://www.jdsc35.com/"
            headers['Origin'] = "http://www.jdsc35.com/"
            if re.search(r'http://(?!www)\w+\.jdsc35\.com', business_url) != None:
                re.search(r'http://(?!www)\w+\.jdsc35\.com', business_url)
                headers['Host'] = business_url.replace('http://', '')
                headers['Referer'] = business_url
                headers['Origin'] = business_url
        elif re.search(r'5jscw', business_url) != None:
            headers['Host'] = "www.5jscw.com"
            headers['Referer'] = "http://www.5jscw.com/"
            headers['Origin'] = "http://www.5jscw.com/"
            if re.search(r'http://(?!www)\w+\.5jscw\.com', business_url) != None:
                business_url = 'http://www.5jscw.com/index.php?homepage=' + re.search(r'http\:\/\/(\w+)\.5jscw\.com/', business_url).group(1)
        else:
            return

        business_html = session.get(business_url, headers=headers, timeout=35)
        #business_html = requests.get(business_url)
        # business_html.encoding ='utf-8'
        # text = str(business_html.text, 'utf-8')
        BS = BeautifulSoup(business_html.text, "html.parser")
        table = BS.find(name='div', attrs={'class': "main_r_contact"})
        if table != None:
            replace_data = self.html_resolver_1(table, BS, business_url)
        else:
            table = BS.find(name='div', attrs={'class': "IContact"})
            if table == None:
                table = BS.find(name='section', attrs={'class': ['m-footer-other', 'm-s-footer-other']})
                if table == None:
                    return
                if BS.find(name='div', attrs={'class': 'noLogo'}) == None:
                    return
                replace_data = self.html_resolver1_3(table, BS, business_url)
            else:
                replace_data = self.html_resolver1_2(table, BS, business_url)

        try:
            replace_sql = '''
                    REPLACE INTO `csc_spider`.`jdsc_companys` (
                    `companyName`, `productCategory`, `address`, `contact`, `contactNumber`, `hotline`, `source`,
                    `url`, `fax`, `createTime`, `updateTime`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
            print(replace_data)
            self.db_cursor.execute(replace_sql, replace_data)
            self.db.commit()
        except Exception as err:
            print("获取数据出错，跳过")
            redis_con.hdel("already_get_business", business_url)
            self.del_already_business(business_url)
            print(err)
            traceback.print_exc()
            pass

        time.sleep(sleep_time)
        return business_html

    # 开始抓取商户
    def entrance(self):
        while 1:
            if int(redis_con.llen("business_queue")) <= 0: # 如果队列为0条
                return
            else:
                # 出队列获取用户name_url redis取出的是byte，要decode成utf-8
                name_url = str(redis_con.rpop("business_queue").decode('utf-8'))
                print("正在处理name_url" + name_url)
                self.get_business_info(name_url)
            self.set_random_ua()
            global session
            session.cookies.save()

    def set_random_ua(self):
        global headers
        length = len(ua)
        rand = random.randint(0, length - 1)
        headers['User-Agent'] = ua[rand]

    def run(self):
        self.entrance()
        # self.entrance()

def start_session():
    global session
    # 初始化session
    requests.adapters.DEFAULT_RETRIES = 5
    session = requests.Session()
    session.cookies = cookielib.LWPCookieJar(filename='cookie')
    session.keep_alive = False
    try:
        session.cookies.load(ignore_discard=True)
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
        global redis_con # 全局变量
        redis_con = redis.Redis(host=redis_host, port=redis_port, db=0)
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

    start_redis() #启动redis连接

    # jdcs_atoz = GetJdscCity() #启动抓取主页客户列表线程

    # jdcs_atoz.get_index_page_business()

    #启动抓取客户信息线程
    threads = []
    threads_num = int(cfg.get("sys", "thread_num"))
    for i in range(0, threads_num):
        m = GetRedisData(i, "thread" + str(i))
        threads.append(m)

    for i in range(0, threads_num):
        threads[i].start()

    for i in range(0, threads_num):
        threads[i].join()

def to_str(object):
    '''
    返回string对象, 原方法没有对象时会报借
    :param object:
    :return:
    '''
    if object != None:
        return object.group(1)
    else:
        return ''

if __name__ == '__main__':
    run()
    # print(ss[0])
    # print(ss[0])
    # ss = start_mysql()
    # db_cursor
    #start_session()
    #start_redis()  # 启动redis连接
    # start_mysql()  # 启动mysql连接
    #test = GetRedisData()
    #test.get_business_info('http://www.jdsc35.com/Corporation/Index/258768.html')
    #test.get_business_info('http://www.jdsc35.com/Corporation/Index/60584.html')
    # test.get_business_info('http://ydhf.jdsc35.com')
    # ss = '上海雷普电器有限公司 -- 中国五金机电市场网'
    # print(ss[:-13])
