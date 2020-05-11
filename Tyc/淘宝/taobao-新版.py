# -*- coding: utf-8 -*-
import os
import requests
from lxml import etree
import json
import re
import random
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
import urllib3
import redis
import threading
import math
from with_hdfs import HdfsClient
urllib3.disable_warnings()

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

user_agent_list = [
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
]


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self, redis_example):
        # 时间部分
        # 爬虫开始抓取的日期
        date = datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]
        # 爬虫结束的抓取日期
        current_time = datetime.now()  # 当前日期
        current_day = str(current_time).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, current_day))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, current_day))
        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = current_day

        # 标记爬虫工作
        self.is_break = False
        self.redis_example = redis_example
        self.pid = os.getpid()
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

    # 替换所有的HTML标签
    def re_html(self, data):
        # 替换抓取数据中的html标签
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub(r'\n', '', ret1)
            ret3 = re.sub(r'\u3000', '', ret2)
            ret4 = re.sub(r'品牌:', '', ret3)
            ret5 = re.sub(r'\xa0', '', ret4)
            ret6 = re.sub(r'&rarr;_&rarr;', '', ret5)
            ret7 = re.sub(r'&hellip;&hellip;', '', ret6)
            ret8 = re.sub(r'":', '', ret7)
            return ret8
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

    # 过滤url里面的#detail
    def re_detail(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'#detail', '', message)
            return ret1
        except:
            pass

    # 过滤品牌
    def re_pin_pai(self, data):
        # 替换抓取数据中的html标签
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub(r'<li title.*?>', '', ret1)
            ret3 = re.sub(r'品牌:&nbsp;', '', ret2)
            return ret3
        except:
            pass

    # 解析请求得到的商品信息
    def parse_goods_url(self, data):
        goods_dict = dict()
        goods_dict['平台'] = '淘宝'
        goods_dict['URL'] = data['URL']
        goods_dict['商品名'] = data['商品名']
        try:
            goods_dict['品牌'] = data['品牌']
        except:
            goods_dict['品牌'] = ''
        goods_dict['价格'] = data['价格']
        goods_dict['shop_name'] = data['shop_name']
        goods_dict['关键词'] = data['关键词']
        goods_dict['itemId'] = data['itemId']
        goods_dict['sellerId'] = data['sellerId']
        goods_dict['imageurl'] = data['商品图片']
        goods_dict['audiourl'] = ''
        # logger.log(31, '************************正在抓取的商品是:%s................' % goods_dict)
        self.goods_data(goods_dict)

    # 抓取商品详情
    def goods_data(self, goods_dict):
        try:
            id = goods_dict['itemId']
            url = 'https://h5api.m.taobao.com/h5/mtop.taobao.detail.getdetail/6.0/?jsv=2.5.7&appKey=12574478&sign=fdd00ab68c3566e514d61534041592d7&api=mtop.taobao.detail.getdetail&v=6.0&isSec=0&ecode=0&AntiFlood=true&AntiCreep=true&H5Request=true&ttid=2018%40taobao_h5_9.9.9&type=jsonp&dataType=jsonp&data=%7B%22spm%22%3A%22a230r.1.14.48.6c1d4af9UmOqpx%22%2C%22id%22%3A%22{}%22%2C%22ns%22%3A%221%22%2C%22abbucket%22%3A%226%22%2C%22itemNumId%22%3A%22{}%22%2C%22itemId%22%3A%22{}%22%2C%22exParams%22%3A%22%7B%5C%22spm%5C%22%3A%5C%22a230r.1.14.48.6c1d4af9UmOqpx%5C%22%2C%5C%22id%5C%22%3A%5C%22{}%5C%22%2C%5C%22ns%5C%22%3A%5C%221%5C%22%2C%5C%22abbucket%5C%22%3A%5C%226%5C%22%7D%22%2C%22detail_v%22%3A%228.0.0%22%2C%22utdid%22%3A%221%22%7D'.format(id, id, id, id)

            headers = {
                'User-Agent': random.choice(user_agent_list)
            }

            try:
                time.sleep(0.3)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.3)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.3)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            json_data = json.loads(json.loads(response.text, strict=False)['data']['apiStack'][0]['value'])['item']  # 通过是否有apiStack判断商品是否下架

            commentCount = json.loads(response.text)['data']['item']['commentCount']
            favcount = json.loads(response.text)['data']['item']['favcount']
            SellCount = json_data['vagueSellCount']
            goods_dict['人气数'] = favcount
            goods_dict['评价人数'] = commentCount
            goods_dict['月销量'] = SellCount

            if int(self.re_not_number(goods_dict['评价人数'])) == 0:
                # logger.log(31, '-----------该商品没有评论数据--------------')
                pass
            else:
                pages_num = int(math.ceil(float(int(goods_dict['评价人数']) / 20)))
                response.close()
                self.goods_comments(goods_dict, pages_num)
        except:
            print(5555555555555555555555, traceback.format_exc())

    # 解析商品评论
    def goods_comments(self, goods_dict, pages_num):
        try:
            is_break = self.is_break
            # print(goods_dict)
            itemId = goods_dict['itemId']
            sellerId = goods_dict['sellerId']

            headers = {
                'cookie': 't=b5285c592f5c5d2760bbc606138d8cf0; UM_distinctid=16a1fadfa62540-0819221c6d91c7-47e1137-232800-16a1fadfa636f7; thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; enc=vn%2BuDgMTTmiEXbq1S%2Byw3qmgOc2O1Fw5PzezL1S7UyTFAqMoepiGRIdTY9msHIOrzffqeq9FLJt5WAGM7ENyvA%3D%3D; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; mt=ci=0_0; cna=At46FUkyQjACAWVWN1V9/Wdy; v=0; cookie2=1ae734c4e8a03d4591a230e3913026b6; _tb_token_=f46b387e3f77e; uc1=cookie14=UoTaHP3MzJiakA%3D%3D; x5sec=7b22726174656d616e616765723b32223a223438663436333231316138653834636332653635613664633664666437363037434b654168656f4645495062705a43337566765a6d51453d227d; _m_h5_tk=b2a5536512217126c542d930817469b0_1564567924778; _m_h5_tk_enc=9e3f2f1eca52726de7c74dd14a9869fa; l=cBaOcPD7qg21z1C9BOCwlurza77ORIRAguPzaNbMi_5dk6Ls857OkSG2UFp6cjWd9pTB41hTyPJ9-etkmI1E1Cmj2s7V.; isg=BC4udOYY_1h3OAv3hZZEU1m4f4Qwh_Mi8XPFVFj3ujHsO8-VwL_0ODk59-dy4-pB',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': 'https://item.taobao.com/item.htm?spm=a230r.1.14.31.26804e4c03W4qw&id=563490255667&ns=1&abbucket=1',
                'User-Agent': random.choice(user_agent_list)
            }

            if int(pages_num) >= 99:
                pages = 99
            else:
                pages = pages_num
            # print('----------------商品评论总页数是： %s -----------------------' % pages_num)
            # 抓取商品评论链接(总共99页,从1开始)
            for i in range(1, int(pages)+1):
                comment_url = 'https://rate.taobao.com/feedRateList.htm?auctionNumId={}&userNumId={}&currentPageNum={}&pageSize=20&rateType=&orderType=feedbackdate&attribute=&sku=&hasSku=false&folded=0'.format(itemId, sellerId, i)
                # print(comment_url)
                # response = requests.get(url=comment_url, headers=headers, proxies=random.choice(proxies), timeout=10)
                try:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                comment_data = response.text
                # print('开始抓取评论')
                # print(comment_data)
                comment = re.search(r'{"qnaDisabled":true,"watershed":.*"maxPage":.*}', comment_data)
                # print(comment.group())
                items = json.loads(comment.group())['comments']
                # print(items)
                goods_data = dict()
                # logger.log(31, '--------********开始写入商品数据********--------')
                for item in items:
                    # if item['date'] != None:
                    # time_test = item['date'].split(' ')[0].replace('年', '-').replace('月', '-').replace('日', '') + ' ' + item['date'].split(' ')[1] + ':00'
                    date_data = item['date'].split(' ')[0].replace('年', '-').replace('月', '-').replace('日', '')
                    try:
                        time_data = item['date'].split(' ')[1] + ':00'
                    except:
                        time_data = ''
                    # print('评论时间', date_data, time_data)
                    try:
                        content = item['content']
                    except:
                        content = ''
                    # 追加评论
                    try:
                        comments_2 = item['appendList'][0]['content']
                    except:
                        comments_2 = ''

                    # 判断评论时间是否在规定的抓取时间内
                    if self.start_time <= date_data.strip():
                        goods_data['platform'] = goods_dict['平台']
                        goods_data['date'] = date_data.strip()
                        goods_data['time'] = time_data.strip()
                        goods_data['keyword'] = goods_dict['关键词']
                        goods_data['name'] = goods_dict['商品名']
                        goods_data['url'] = goods_dict['URL']
                        goods_data['shop_name'] = goods_dict['shop_name']
                        goods_data['user_name'] = item['user']['nick']
                        goods_data['content'] = content + ';' + comments_2
                        goods_data['content_id'] = str(item['rateId'])
                        goods_data['brand'] = goods_dict['品牌']
                        goods_data['price'] = goods_dict['价格']
                        goods_data['sales'] = goods_dict['月销量']
                        goods_data['focus_count'] = goods_dict['人气数']
                        goods_data['comment_num'] = goods_dict['评价人数']
                        goods_data['views'] = ''
                        goods_data['likes'] = item['useful']
                        goods_data['comments_count'] = ''
                        goods_data['author_id'] = ''
                        goods_data['reposts_count'] = ''
                        goods_data['topic_id'] = str(goods_dict['itemId'])
                        # 判断size和type 颜色分类:黑色高帮&nbsp;&nbsp尺码:37
                        test_data = item['auction']['sku']
                        if '码' in test_data:
                            goods_data['type'] = test_data.split(':')[1].replace('尺码', '').replace('&nbsp;&nbsp', '').replace('鞋码', '').replace(';尺码', '')
                            goods_data['size'] = test_data.split(':')[2]
                        else:
                            goods_data['type'] = ''
                            goods_data['size'] = ''
                        goods_data['imageurl'] = goods_dict['imageurl']
                        goods_data['audiourl'] = goods_dict['audiourl']
                        goods_data['file_code'] = '55'
                        # print(goods_data)
                        # items = json.dumps(dict(goods_data), ensure_ascii=False) + '\n'
                        # with open('./bu_cai/55_{}_taobao_nike_{}_1.json'.format(time.strftime('%Y%m%d'), self.pid), 'ab') as f:
                        #     f.write(items.encode("utf-8"))
                        item = json.dumps(dict(goods_data), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/55_{}_TaoBao_nike{}.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.pid), item, encoding='utf-8')
                    if date_data.strip() < self.start_time:
                        is_break = True
                if is_break:
                    break
        except:
            print(66666666666666666666, traceback.format_exc())

    def run(self, lock):
        for num in range(1000000):
            lock.acquire()
            redis_url_num = self.redis_example.llen('taobao_day_url')
            if str(redis_url_num) == '0':
                print('*******Redis消息队列中url为空，程序等待中...进程 {} 等待中...*******'.format(str(os.getpid())))
            item = self.redis_example.brpop('taobao_day_url', timeout=600)[1]
            lock.release()
            item1 = json.loads(item.decode())
            # print(item)
            self.parse_goods_url(item1)
        # item1 = {'URL': 'https://item.taobao.com/item.htm?id=602185373342&ns=1&abbucket=7', '商品名': '123', '品牌': '123', '价格': '123456', 'shop_name': '123', '关键词': '123', 'itemId': '602185373342', 'sellerId': '355159670', '商品图片': '123'}
        # self.parse_goods_url(item1)


pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
# pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


def app_run():

    lock = threading.Lock()
    spider = Spider(redis_example)
    try:
        spider.run(lock)
    except:
        logger.error('pid={}错误为{}'.format(str(os.getpid()), str(traceback.format_exc())))
        print(traceback.format_exc())


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=18)
    for i in range(18):
        pool.apply_async(app_run)
    pool.close()
    pool.join()
