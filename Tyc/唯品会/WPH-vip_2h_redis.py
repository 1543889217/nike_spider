import os
import requests
from lxml import etree
import json
import re
import xlrd
import math
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
from with_hdfs import HdfsClient
import redis
import threading

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./log_data/{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)

# 代理服务器
proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = "HL89Q19E86E2987D"
proxyPass = "71F33D94CE5F7BF2"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
}

proxies = {
    # "http": proxyMeta,
    "https": proxyMeta
}


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self, redis_example):
        self.headers = {
            'Content-Type': 'text/html; charset=utf-8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            # 'Cookie': 'vip_rip=101.86.55.85; vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; mars_sid=a369b0e73f9656dbd3eda470968f6cd2; _smt_uid=5d4156d3.52d69d05; user_class=a; VipUINFO=luc%3Aa%7Csuc%3Aa%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A1; vipte_viewed_=6917921732696396695%2C793920209978892%2C2161644495; visit_id=4C5B033907F8247A18F2811FF8D147F0; _jzqco=%7C%7C%7C%7C%7C1.15943944.1564563154491.1564740333894.1564740386032.1564740333894.1564740386032.0.0.0.24.24; mars_cid=1564563151837_048422ec87f93127ee1eced568a171af',
            'Host': 'category.vip.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
        }
        # 时间部分,按小时抓取
        date_time = str(datetime.now() - timedelta(days=1)).split('.')[0]
        start_time_test = time.strftime('%Y-%m-%d 00:00:00')

        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        a = end_time.split(' ')[1].split(':')[0]

        if a == '00':
            start_time_data = date_time
            hours_name = '22_24'
            wen_jian_jia_date = str(datetime.now() - timedelta(days=1)).split('.')[0].split(' ')[0].replace('-', '')
        else:
            two_hours_ago = int(a) - 2
            if len(str(two_hours_ago)) == 1:
                two_hour_ago = '0' + str(two_hours_ago)
            else:
                two_hour_ago = str(two_hours_ago)
            hours_name = str(two_hour_ago) + '_' + str(a)
            start_time_data = start_time_test
            wen_jian_jia_date = time.strftime('%Y%m%d')
        print('爬取时间段：{}到{}'.format(start_time_data, end_time))
        logging.info('爬取时间段：{}到{}'.format(start_time_data, end_time))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = start_time_data
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = end_time
        # 标记爬虫工作
        self.is_break = False
        self.redis_example = redis_example
        self.pid = os.getpid()

        self.h2_name = hours_name
        self.date_time = wen_jian_jia_date
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_2h/ecommerce/{}/{}'.format(wen_jian_jia_date, hours_name))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

    # 时间戳转换时间
    def time_change(self, data):
        # 替换抓取数据中的html标签
        try:
            timeStamp = float(int(data) / 1000)
            timeArray = time.localtime(timeStamp)
            otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            return otherStyleTime
        except:
            pass

    # 过滤月销量里面的非数字
    def re_not_number(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'\D', '', message)
            return ret1
        except:
            pass

    # 获取评论量
    def parse_comments_num(self, goods_dict):
        try:
            headers = {
                # 'Cookie': 'vip_rip=101.86.55.85; vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; mars_sid=a369b0e73f9656dbd3eda470968f6cd2; _smt_uid=5d4156d3.52d69d05; VipDFT=1; visit_id=2221152ECC2AD948DF7AB8D56322CE59; vipAc=cf3c0da6d5b52c0f6088b0148efbdb22; vipshop_passport_src=https%3A%2F%2Fdetail.vip.com%2Fdetail-1710618487-6918048587083491095.html; PASSPORT_ACCESS_TOKEN=1FDEBDAAF470FFB2C3C6A9EEAF7256FBA60D1F08; VipRUID=298018734; VipUID=0f94f94cc1ea26b39e78438380499d64; VipRNAME=152*****067; VipLID=0%7C1564973676%7C4b447f; VipDegree=D1; user_class=c; VipUINFO=luc%3Ac%7Csuc%3Ac%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A1; PHPSESSID=b9bnc95dlt7r4eg2r196td02i4; vipte_viewed_=6917921732696396695%2C793920209978892%2C2161644495%2C6918048587083491095%2C6917922115290256471; VipCI_te=0%7C%7C1564974326; _jzqco=%7C%7C%7C%7C%7C1.15943944.1564563154491.1564974076993.1564974326073.1564974076993.1564974326073.0.0.0.39.39; waitlist=%7B%22pollingId%22%3A%22F90BE7CF-3F21-4012-800F-E1F26000E5BF%22%2C%22pollingStamp%22%3A1564974516121%7D; mars_cid=1564563151837_048422ec87f93127ee1eced568a171af',
                'Host': 'detail.vip.com',
                'Pragma': 'no-cache',
                'Referer': goods_dict['url'],
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            url = 'https://detail.vip.com/v2/mapi?_path=rest/content/reputation/getCountBySpuId&spuId={}&brandId={}&app_name=shop_pc'.format(goods_dict['spuId'], goods_dict['brandId'])
            try:
                time.sleep(0.1)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            achieve_num_data = json.loads(response.text)['data']
            goods_dict['achieve_num'] = achieve_num_data
            if int(achieve_num_data) == 0:
                page_num = int(math.ceil(float((int(achieve_num_data)+1) / 10)))
                # logger.log(31, '评论数是: %s , 评论页数是: %s ' % (goods_dict['achieve_num'], str(page_num)))
                # print(goods_dict)
                self.parse_comments(goods_dict, page_num)
            else:
                page_num = int(math.ceil(float(int(achieve_num_data) / 10)))
                # logger.log(31, '评论数是: %s , 评论页数是: %s ' % (goods_dict['achieve_num'], str(page_num)))
                # print(goods_dict)
                self.parse_comments(goods_dict, page_num)
        except:
            print(222222222222222222222222, traceback.format_exc())

    # 抓取商品评论
    def parse_comments(self, goods_dict, page_num):
        try:
            if page_num == 0:
                pass
                # logger.log(31, '0000000000000000没有商品评论信息000000000000000000')
            else:
                is_break = self.is_break
                headers = {
                    # 'Cookie': 'vip_rip=101.86.55.85; vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; mars_sid=a369b0e73f9656dbd3eda470968f6cd2; _smt_uid=5d4156d3.52d69d05; VipDFT=1; visit_id=2221152ECC2AD948DF7AB8D56322CE59; vipAc=cf3c0da6d5b52c0f6088b0148efbdb22; vipshop_passport_src=https%3A%2F%2Fdetail.vip.com%2Fdetail-1710618487-6918048587083491095.html; PASSPORT_ACCESS_TOKEN=1FDEBDAAF470FFB2C3C6A9EEAF7256FBA60D1F08; VipRUID=298018734; VipUID=0f94f94cc1ea26b39e78438380499d64; VipRNAME=152*****067; VipLID=0%7C1564973676%7C4b447f; VipDegree=D1; user_class=c; VipUINFO=luc%3Ac%7Csuc%3Ac%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A1; PHPSESSID=b9bnc95dlt7r4eg2r196td02i4; vipte_viewed_=6917921732696396695%2C793920209978892%2C2161644495%2C6918048587083491095%2C6917922115290256471; VipCI_te=0%7C%7C1564974326; _jzqco=%7C%7C%7C%7C%7C1.15943944.1564563154491.1564974076993.1564974326073.1564974076993.1564974326073.0.0.0.39.39; waitlist=%7B%22pollingId%22%3A%22F90BE7CF-3F21-4012-800F-E1F26000E5BF%22%2C%22pollingStamp%22%3A1564974516121%7D; mars_cid=1564563151837_048422ec87f93127ee1eced568a171af',
                    'Host': 'detail.vip.com',
                    'Pragma': 'no-cache',
                    'Referer': goods_dict['url'],
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
                for i in range(1, int(page_num)+1):
                    # logger.log(31, '*************************抓取评论第：%s 页' % i)
                    url = 'https://detail.vip.com/v2/mapi?_path=rest/content/reputation/queryBySpuId&spuId={}&brandId={}&page={}&pageSize=10&app_name=shop_pc&keyWordNlp=%E6%9C%80%E6%96%B0'.format(goods_dict['spuId'], goods_dict['brandId'], i)
                    try:
                        time.sleep(0.1)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        try:
                            time.sleep(0.1)
                            response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                        except:
                            time.sleep(0.1)
                            response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    # 商品评价列表
                    comments_list = json.loads(response.text)['data']
                    if int(len(comments_list)) == 0:
                        break
                    else:
                        comment_dict = dict()
                        for item in comments_list:
                            date_data = self.time_change(item['reputation']['postTime'])
                            # print(date_data)
                            if self.start_time <= date_data:
                                comment_dict['platform'] = goods_dict['platform']
                                comment_dict['date'] = date_data.split(' ')[0]
                                comment_dict['time'] = date_data.split(' ')[1]
                                comment_dict['keyword'] = goods_dict['keyword']
                                comment_dict['name'] = goods_dict['name']
                                comment_dict['imageurl'] = goods_dict['商品图片']
                                comment_dict['audiourl'] = ''
                                comment_dict['url'] = goods_dict['url']
                                comment_dict['shop_name'] = ''
                                comment_dict['user_name'] = item['reputationUser']['authorName']
                                comment_dict['author_id'] = str(item['reputationUser']['userIdentity'])
                                comment_dict['content'] = item['reputation']['content']
                                comment_dict['content_id'] = str(item['reputation']['reputationId'])
                                comment_dict['brand'] = goods_dict['brand']
                                comment_dict['price'] = goods_dict['price']
                                comment_dict['sales'] = goods_dict['sales']
                                comment_dict['focus_count'] = ''
                                comment_dict['comment_num'] = goods_dict['achieve_num']
                                comment_dict['views'] = ''
                                comment_dict['likes'] = ''
                                comment_dict['comments_count'] = ''
                                comment_dict['reposts_count'] = ''
                                comment_dict['topic_id'] = str(goods_dict['url'].split('-')[2].replace('.html', ''))
                                try:
                                    comment_dict['type'] = item['reputationProduct']['colorInfo']
                                except:
                                    comment_dict['type'] = ''
                                try:
                                    comment_dict['size'] = item['reputationProduct']['size']
                                except:
                                    comment_dict['size'] = ''
                                comment_dict['file_code'] = '179'
                                # logger.log(31, '---------------正在写入符合时间的商品评论---------------------')
                                # print(comment_dict)
                                # self.write_Nike_jsonfile(comment_dict)
                                item = json.dumps(dict(comment_dict), ensure_ascii=False) + '\n'
                                self.hdfsclient.new_write('/user/cspider_daily/nike_2h/ecommerce/{}/{}/179_{}_WPH_nike{}.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.pid), item, encoding='utf-8')
                            if self.start_time > date_data.split(' ')[0].strip():
                                is_break = True
                        if is_break:
                            break
        except:
            print(33333333333333333333, traceback.format_exc())

    # def parse_xlsx(self):
    #     # 设置路径
    #     path = './快消采集关键词_0916_v3-1.xlsx'
    #     # 打开execl
    #     workbook = xlrd.open_workbook(path)
    #
    #     # 根据sheet索引或者名称获取sheet内容
    #     Data_sheet = workbook.sheets()[0]  # 通过索引获取
    #
    #     # print(Data_sheet.name)  # 获取sheet名称
    #     rowNum = Data_sheet.nrows  # sheet行数
    #     colNum = Data_sheet.ncols  # sheet列数
    #
    #     # 获取所有单元格的内容
    #     list = []
    #     for i in range(rowNum):
    #         rowlist = []
    #         for j in range(colNum):
    #             rowlist.append(Data_sheet.cell_value(i, j))
    #         list.append(rowlist)
    #
    #     for data in list[1::]:
    #         brand = data[0]
    #         # print(brand)
    #         yield {
    #             '关键词': brand,
    #         }

    def run(self, lock):
        for num in range(1000000):
            lock.acquire()
            redis_url_num = self.redis_example.llen('WPH_nike_url')
            if str(redis_url_num) == '0':
                print('**********Redis消息队列中url为空.....进程 {} 抓取结束.....***********'.format(str(os.getpid())))
                
            item = self.redis_example.brpop('WPH_nike_url', timeout=3600)[1]
            lock.release()
            item1 = json.loads(item.decode())
            # print(item)
            self.parse_comments_num(item1)


pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


def app_run():
    lock = threading.Lock()
    spider = Spider(redis_example)
    try:
        spider.run(lock)
    except:
        logger.error('pid={}\n错误为{}'.format(str(os.getpid()), str(traceback.format_exc())))
        print(traceback.format_exc())


if __name__ == "__main__":
    pid = os.getpid()
    hour = str(datetime.now()).split(' ')[1].split(':')[0]
    if 8 <= int(hour) <= 23:
        num = 3
    else:
        num = 1
    pool = multiprocessing.Pool(processes=int(num))
    for i in range(int(num)):
        pool.apply_async(app_run, args=())
    pool.close()
    # pool.join()

    # 程序计时，两小时后结束任务
    py_start_time = time.time()
    while True:
        if (float(time.time()) - float(py_start_time)) > 7193:
            logger.log(31, u'爬取时间已经达到两小时，结束进程任务：' + str(pid))
            pool.terminate()
            break
        time.sleep(1)