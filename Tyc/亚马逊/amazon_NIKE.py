# -*- coding:UTF-8 -*-
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
import datetime
import traceback
import xlrd
import math
import multiprocessing
from multiprocessing import Pool, Manager, Queue, Process
from with_hdfs import HdfsClient
import redis
import threading

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./log_data/{}-{}.log".format(name, str(datetime.datetime.now()).split(' ')[0])
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
        # 时间判断部分
        date = datetime.datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]

        now = datetime.datetime.now() - timedelta(days=0)  # 昨天时间
        now_date = str(now).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, now_date))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, now_date))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = now_date
        # 标记爬虫工作
        self.is_work = False
        self.redis_example = redis_example
        self.pid = os.getpid()

        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

    # 替换所有的HTML标签
    def re_html(self, data):
        # 替换抓取数据中的html标签
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub(r'\[', '', ret1)
            ret3 = re.sub(r'\]', '', ret2)
            ret4 = re.sub(r'广告这些是您在亚马逊上看到的商品广告。点击广告，您将前往商品所在页面。了解更多关于广告的信息', '', ret3)
            ret5 = re.sub(r'\\xa0', '', ret4)
            ret6 = re.sub(r'海外购满200元免运费', '', ret5)
            ret7 = re.sub(r'更多购买选择', '', ret6)
            ret8 = re.sub(r'品牌', '', ret7)
            ret9 = re.sub(r'"append","#cm_cr-review_list",', '', ret8)
            ret10 = re.sub(r'"', '', ret9)
            return ret10
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

    # 过滤user_name
    def re_user_name(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'前\d+名评论人', '', message)
            ret2 = re.sub(r'\n', '', ret1)
            return ret2
        except:
            pass

    # 匹配具体时间
    def clean_date(self, x):
        now = datetime.datetime.now()
        if str(x).find('昨天') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(days=-1), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('前天') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(days=-2), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('天前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(days=-int(str(x).replace('天前', ''))),
                                           '%Y-%m-%d %H:%M:%S')
        elif str(x).find('小时前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(hours=-int(str(x).replace('小时前', ''))),
                                           '%Y-%m-%d %H:%M:%S')
        elif str(x).find('分钟前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(minutes=-int(str(x).replace('分钟前', ''))),
                                           '%Y-%m-%d %H:%M:%S')
        elif str(x).find('今天') != -1:
            x = str(x).replace('今天', now.strftime('%Y-%m-%d') + ' ')
        elif str(x).find('刚刚') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('秒前') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('月前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-4 * int(str(x).replace('月前', ''))),
                                           '%Y-%m-%d %H:%M:%S')
        elif str(x).find('周前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-int(str(x).replace('周前', ''))),
                                           '%Y-%m-%d %H:%M:%S')
        elif str(x).find('[') != -1:
            x = x.replace('[', '').replace(']', '')
        elif str(x).find('月') != -1:
            x = x.replace('月', '-').replace('日', '')
        return x

    # 抓取商品品牌信息
    def parse_goods_brand(self, goods_dict):
        try:
            # print(goods_dict)
            url = goods_dict['url']
            # print('*************************商品详情页' + url)
            headers = {
                # 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                # 'Cookie': 'x-wl-uid=1TZhgwHTJAuLn8oreMzKQn1F14u+yWLnkVnV1mHxoFBZVluB35GzI3vNZyOaUXm1eXxDdVSvG/jk=; session-id=461-0953337-2517812; ubid-acbcn=462-0558053-9620064; i18n-prefs=CNY; lc-acbcn=zh_CN; x-amz-captcha-1=1565689220474259; x-amz-captcha-2=O0HfV0HAdNq8q0k6ODz5yA==; session-token=Masvfy+QDMESO49Iacs+I77sObVPwrSbsVNucyNsgXupKeHI3zVO2/zgQTAUsJUOOcC8swOMHILZfrVmo85e45fYuETObv3I2N3CYtSgBaET4WZ1l7qnzkzQ0yWNVcqvgtSbNDZXWNii93OIcke5QSx0Y3kmJZaGk5+H9Nn2rD7c2YStoxaV/0yQ0UsfRfwj; csm-hit=tb:s-SKSGNJDF9HE5MK9C3DDT|1566530133484&t:1566530133820&adb:adblk_yes; session-id-time=2082729601l',
                'Host': 'www.amazon.cn',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                # time.sleep(0.1)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            # response.encoding = 'utf-8'
            # print(response.text)
            asin_id = response.url.split('/')[4]
            # print(asin_id)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 获取商品评论数
            re_comments_num = html.xpath('//span[@class="a-size-base a-color-secondary"]/text()')
            # print(re_comments_num)
            if re_comments_num == []:
                # logger.log(31, '--------------没有商品评论信息----------------')
                pass
            else:
                comments_num = self.re_not_number(re_comments_num[0])
                # print('评论数: ', comments_num)
                # 评价人数
                goods_dict['achieve_num'] = comments_num
                # 获取商品品牌信息
                brand_data1 = re.search(r'品牌</td><td class="value">.*?</td>', response.text)

                if brand_data1 != None:
                    brand_name = self.re_html(brand_data1.group())
                else:
                    brand_data2 = html.xpath('//div[@id="ppd"]/div[2]/div[2]/div/div/div[1]/div[1]/div/a/text()')
                    if brand_data2 == []:
                        brand_name = ''
                    else:
                        try:
                            brand_name = brand_data2[0].split(' ')[0]
                        except:
                            brand_name = ''
                # 商品品牌
                goods_dict['brand'] = brand_name
                # 销量
                goods_dict['sales'] = ''
                goods_dict['asin_id'] = asin_id
                # print(goods_dict)

                # 抓取页数
                page_num = int(math.ceil(float(int(self.re_not_number(goods_dict['achieve_num'])) / 10)))
                # print('***---回复数: %s,页数：%s ***---' % (comments_num, page_num))
                # 抓取评论量
                self.parse_amazon_comment(page_num, goods_dict)

        except:
            print(22222222222222222222, traceback.format_exc())

    # 抓取页数大于0的评论
    def parse_amazon_comment(self, page_num, goods_dict):
        try:
            is_break = self.is_work
            # print(goods_dict['url'])
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                'Cookie': 'session-id=457-6049818-5407509; i18n-prefs=CNY; ubid-acbcn=461-1543774-5730813; x-wl-uid=1D2HfAfNoe4eUdJ6ZzyM2fnvna5QixxATqyW5m655FgD9MFQ0BQOrYAub+2t2juEPWKvSIO9wETU=; lc-acbcn=zh_CN; session-token=q7jDZTzYPSN0ujucLEDRVnx7QbLwQdbfOyVVn5sdYL1XaQm73hM1Kf01JGRuR/3AZ1IX24BUDL3mq5DGmIiN1UAQ/DtCP/HhHNLIw9ct8KzziVH+J5r2FrvA0ObuVLGlqYbghQbq2Ddhm8zB/AHX7OHvGD0LWTIaDpfYJ62e2fz813rIz0IkwKLvoFjSiT+G; session-id-time=2082729601l; csm-hit=tb:Q0KNXH65T2X9SHESP2YH+s-9R0M13527VFRJHPP284C|1574144443485&t:1574144443485&adb:adblk_yes',
                'Host': 'www.amazon.cn',
                'Origin': 'https://www.amazon.cn',
                'Pragma': 'no-cache',
                'Referer': 'https://www.amazon.cn/Nike-%E8%80%90%E5%85%8B-Revolution-4-%E7%94%B7%E5%A3%AB%E8%B7%91%E6%AD%A5%E9%9E%8B/product-reviews/B079QP634Q/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }
            if int(page_num) >= 50:
                pages = 50
            else:
                pages = page_num
            for i in range(1, int(pages)+1):
                # print('***---抓取评论页为第%s页***---' % i)
                url = 'https://www.amazon.cn/hz/reviews-render/ajax/reviews/get/ref=cm_cr_getr_d_paging_btm_prev_{}'.format(i)
                # print(url)
                form_data = {
                    'sortBy': 'recent',
                    'reviewerType': 'all_reviews',
                    'formatType': '',
                    'mediaType': '',
                    'filterByStar': '',
                    'pageNumber': i,
                    'filterByLanguage': '',
                    'filterByKeyword': '',
                    'shouldAppend': 'undefined',
                    'deviceType': 'desktop',
                    'reftag': 'cm_cr_getr_d_paging_btm_prev_{}'.format(i),
                    'pageSize': '10',
                    'asin': goods_dict['asin_id'],
                    'scope': 'reviewsAjax{}'.format(int(i)-1)
                }
                try:
                    # time.sleep(0.1)
                    response = requests.post(url=url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        # time.sleep(0.1)
                        response = requests.post(url=url, headers=headers, data=form_data,  proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        # time.sleep(0.1)
                        response = requests.post(url=url, headers=headers, data=form_data,  proxies=proxies, allow_redirects=False, timeout=30)
                comment_data = response.text.split('&&&')[5:-5]
                # print(comment_data)
                comment_dict = dict()
                for item in comment_data:
                    # print(goods_dict['url'])
                    data = self.re_html(item.replace(' ', ''))
                    # print(data)
                    # 帖子id
                    topic_id = re.search(r'<divid=\\".*?\\', item.replace(' ', '')).group().replace('<divid=\\"', '').replace('\\', '')
                    # 评价用户名
                    user_name = self.re_user_name(re.sub(r'\d.\d颗星，最多5颗星', '-', data).split('-')[0].replace('\\n', ''))
                    # 评论日期
                    date_data_test = re.search(r'\d{1,4}年\d{1,4}月\d{1,4}日', data).group().replace('年', '-').replace('月', '-').replace('日', '')
                    a = date_data_test.split('-')[1]
                    b = date_data_test.split('-')[2]
                    if int(len(a)) == 1 and int(len(b)) != 1:
                        date_data = date_data_test.split('-')[0] + '-0' + date_data_test.split('-')[1] + '-' + date_data_test.split('-')[2]
                    elif int(len(a)) != 1 and int(len(b)) == 1:
                        date_data = date_data_test.split('-')[0] + '-' + date_data_test.split('-')[1] + '-0' + date_data_test.split('-')[2]
                    elif int(len(a)) == 1 and int(len(b)) == 1:
                        date_data = date_data_test.split('-')[0] + '-0' + date_data_test.split('-')[1] + '-0' + date_data_test.split('-')[2]
                    else:
                        date_data = date_data_test
                    # 评价时间
                    time_data = ''
                    # 作者id
                    author_id = ''
                    # print(achieve_content_data)
                    # print(data)
                    test_type_sisz = re.search('\d{1,4}年\d{1,2}月\d{1,2}日.*?有帮助', data)
                    # print(test_type_sisz.group())
                    # 鞋子类型
                    try:
                        type_data = test_type_sisz.group().split(':')[2].split('已确认购买')[0].replace('颜色', '')
                    except:
                        type_data = ''
                    # 鞋子尺码
                    try:
                        size = data.split(':')[1].replace('颜色', '')
                    except:
                        size = ''
                    # print(type)
                    # print(size)
                    # 判断评论时间是否在规定的抓取时间内
                    if self.start_time <= date_data.strip():
                        comment_dict['platform'] = goods_dict['platform']
                        comment_dict['date'] = date_data.strip()
                        comment_dict['time'] = time_data.strip()
                        comment_dict['keyword'] = goods_dict['keyword']
                        comment_dict['name'] = goods_dict['name']
                        comment_dict['imageurl'] = goods_dict['商品图片']
                        comment_dict['audiourl'] = ''
                        comment_dict['url'] = goods_dict['url']
                        comment_dict['shop_name'] = ''
                        comment_dict['user_name'] = self.re_user_name(user_name)
                        try:
                            comment_dict['content'] = data.split('已确认购买')[1].split('有帮助')[0].split('\\n')[0]
                        except:
                            comment_dict['content'] = ''
                        comment_dict['content_id'] = str(topic_id)
                        comment_dict['brand'] = goods_dict['brand']
                        comment_dict['price'] = goods_dict['price']
                        comment_dict['sales'] = goods_dict['sales']
                        comment_dict['focus_count'] = ''
                        comment_dict['comment_num'] = goods_dict['achieve_num']
                        comment_dict['views'] = ''
                        comment_dict['likes'] = ''
                        comment_dict['comments_count'] = ''
                        comment_dict['reposts_count'] = ''
                        comment_dict['author_id'] = str(author_id)
                        comment_dict['topic_id'] = str(goods_dict['url'].split('/')[4])
                        comment_dict['type'] = type_data
                        comment_dict['size'] = size
                        comment_dict['file_code'] = '54'
                        # print('***********正在写入符合时间的评论*******************')
                        # print(comment_dict)
                        # items = json.dumps(dict(comment_dict), ensure_ascii=False) + '\n'
                        # with open('./json_data/54_{}_{}_amazon_nike{}.json'.format(time.strftime('%Y%m%d'), self.time_data, self.pid), 'ab') as f:
                        #     f.write(items.encode("utf-8"))
                        item = json.dumps(dict(comment_dict), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/54_{}_{}_amazon_nike{}.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data, self.pid), item, encoding='utf-8')
                    if date_data.strip() < self.start_time:
                        is_break = True
                if is_break:
                    break
        except:
            print(444444444444444444444, traceback.format_exc())

    def run(self, lock):
        for num in range(100000):
            lock.acquire()
            redis_url_num = self.redis_example.llen('anazon_day_url')
            if str(redis_url_num) == '0':
                print('********************\nRedis消息队列中url为空.....\n进程 {} 抓取结束......\n********************'.format(str(os.getpid())))

            item = self.redis_example.brpop('anazon_day_url', timeout=600)[1]
            lock.release()
            item1 = json.loads(item.decode())
            # print(item1)
            self.parse_goods_brand(item1)


pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
# pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
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
    pool = multiprocessing.Pool(processes=3)
    for i in range(3):
        # spider = Spider(redis_example)
        pool.apply_async(app_run)
    pool.close()
    pool.join()