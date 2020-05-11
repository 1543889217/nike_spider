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
from collections import OrderedDict
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
        self.hdfsclient = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_2h/ecommerce/{}/{}'.format(wen_jian_jia_date, hours_name))  # 创建每日文件夹
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

    # 解析请求得到的商品信息
    def parse_goods_url(self, data):
        goods_dict = dict()
        goods_dict['平台'] = '天猫'
        goods_dict['URL'] = data['URL']
        goods_dict['商品名'] = data['商品名']
        goods_dict['价格'] = data['价格']
        goods_dict['shop_name'] = data['shop_name']
        goods_dict['月销量'] = data['月销量'].replace('人付款', '')
        goods_dict['关键词'] = data['关键词']
        goods_dict['品牌'] = data['品牌']
        goods_dict['itemId'] = data['itemId']
        goods_dict['sellerId'] = data['sellerId']
        goods_dict['imageurl'] = data['商品图片']
        goods_dict['audiourl'] = ''
        # logger.log(31, '***************************正在抓取的商品是:%s.................' % goods_dict)
        self.parse_goods_details(goods_dict)

    # 解析商品品牌信息
    def parse_goods_details(self, goods_dict):
        try:
            url = goods_dict['URL']
            headers = {
                'content-type': 'text/html;charset=GBK',
                # 'authority': 'detail.tmall.com',
                # 'method': 'GET',
                # 'path': path,
                # 'scheme': 'https',
                # 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                # 'accept-encoding': 'gzip, deflate, br',
                # 'accept-language': 'zh-CN,zh;q=0.9',
                # 'cache-control': 'no-cache',
                'cookie': 'cq=ccp%3D1; cookie2=1f727f7b9a023d3336775fda77aa6c64; t=e0b1a5b3f801e8ad7974356321ff2384; _tb_token_=33173e5033eee; csg=0cc83e88; dnk=tb9619067_2012; tracknick=tb9619067_2012; lid=tb9619067_2012; enc=x%2FOcdI7JLsr6CpzvtdqWU16H2R3aBtna09TzEkBv9ziurQwe2F0J9TpnrtPlgFZW95bG4nGzcBhhVk6VKxhnTw%3D%3D; hng=CN%7Czh-CN%7CCNY%7C156; uc1=cookie16=VT5L2FSpNgq6fDudInPRgavC%2BQ%3D%3D&cookie21=UtASsssme%2BBq&cookie15=URm48syIIVrSKA%3D%3D&existShop=false&pas=0&cookie14=UoTaECbNEKyHyQ%3D%3D&tag=8&lng=zh_CN; UM_distinctid=16d142a6ebc31-00f1b1ad240fa2-37c143e-1fa400-16d142a6ebda91; cna=MQj5FQMZD0sCAXxONRZeF0y0; isg=BAgI5_cywVJRgi3av8Xnqb_92XYasWy7u5UjZsK5VAN2nagHasE8S56fETVItiST; l=cBQfGYbVq1scMYebBOCanurza77OSIRYYuPzaNbMi_5pY6TsXPQOkP9r_F96VjWd9vYB41hTyPJ9-etkqPrdT9dbHZ9R.; CNZZDATA1000427971=618518977-1568000449-https%253A%252F%252Fs.taobao.com%252F%7C1568254167; pnm_cku822=098%23E1hvuvvUvbpvUvCkvvvvvjiPRFLUljtVn2sUQjivPmPWljYWRF5vQjDvRLLyAjlhiQhvCvvvpZpEvpCW9HKm93zEn1m65kx%2FQjZJ%2Bu0XjobyACuXjBrQpWkQRqJ6WeCpqU0QKfUpwy2IAfUTKFyK2ixrV4TJVVQHYWpXei%2BffCuYiLUpVE6FpdyCvm9vvhCvvvvvvvvvBGwvvUjZvvCj1Qvvv3QvvhNjvvvmmvvvBGwvvvUUkphvC9hvpyPwl8yCvv9vvhh%2BFHx7CphCvvOv9hCvvvvPvpvhvv2MMsyCvvpvvhCv',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.3)
                response = requests.get(url=url, headers=headers, proxies=proxies, timeout=30)
            except:
                try:
                    time.sleep(0.4)
                    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=30)
                except:
                    time.sleep(0.5)
                    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=30)
            # response.encoding = 'gbk'
            # print("正在抓取的链接是: %s" % url)
            # print(response.text)
            spuid = re.search(r'"spuId":".*","categoryId"|"spuId":.*?,|amp;spuId=.*?"|spuId=.*?"', response.text)
            re_spuid = self.re_not_number(spuid.group())
            # logger.log(31, 'spuid:', re_spuid)
            # 将响应转换成一个element对象
            # html = etree.HTML(response.text)
            # # print(html)
            # # 获取所有品牌节点列表
            # pin_pai_list = html.xpath('//li[@id="J_attrBrandName"]/text()|//ul[@class="attributes-list"]/li[1]/text()')[0]
            # if '品牌' not in pin_pai_list:
            #     pin_pai = html.xpath('//li[@id="J_attrBrandName"]/text()|//ul[@class="attributes-list"]/li[2]/text()')[0]
            # else:
            #     pin_pai = pin_pai_list
            # print(goods_url, pin_pai_list[0])
            # data_list = []
            goods_dict['spuid'] = re_spuid
            # print(goods_dict)
            response.close()
            self.goods_collection_num(goods_dict)
        except:
            print(33333333333333333333, traceback.format_exc())

    # 抓取商品收藏数（人气）
    def goods_collection_num(self, goods_dict):
        try:
            url = 'https://count.taobao.com/counter3?callback=jsonp243&keys=SM_368_dsr-{},ICCP_1_{}'.format(goods_dict['sellerId'], goods_dict['itemId'])
            headers = {
                'content-type': 'application/x-javascript',
                'cookie': 't=b5285c592f5c5d2760bbc606138d8cf0; UM_distinctid=16a1fadfa62540-0819221c6d91c7-47e1137-232800-16a1fadfa636f7; thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; enc=vn%2BuDgMTTmiEXbq1S%2Byw3qmgOc2O1Fw5PzezL1S7UyTFAqMoepiGRIdTY9msHIOrzffqeq9FLJt5WAGM7ENyvA%3D%3D; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; _uab_collina=155540168306791903478476; mt=ci=0_0; cna=At46FUkyQjACAWVWN1V9/Wdy; v=0; cookie2=1ae734c4e8a03d4591a230e3913026b6; _tb_token_=f46b387e3f77e; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; _m_h5_tk=f32553e159b195a4f17c00010f2bcd2e_1564547678304; _m_h5_tk_enc=3268f7bf49fd78b94768c96e3ef51817; uc1=cookie14=UoTaHP3MzJiakA%3D%3D; x5sec=7b227365617263686170703b32223a2266303666326434356635336264366335613639393662663834646632366531644349582b684f6f46454c47473571714e726f325955526f4c4f4459314d5467794d6a4d334f7a453d227d; l=cBaOcPD7qg21z_uyBOfZKurza779uIdf1sPzaNbMiICPO_fh5wONWZFb8t8MCnGVLsI2535t6zUaBXYaGyUIh2nk8b8CgsDd.; isg=BOzsOXiZnf59JomJ--wm9a9SvcreDZFEZ8nHSkYsbxe0UY9baraQ30WjcVnMWcin; JSESSIONID=A9F406FD84CDFD576728A12ECBD98A53',
                'upgrade-insecure-requests': '1',
                'user-agent': random.choice(user_agent_list)
            }

            try:
                time.sleep(0.3)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.4)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.5)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            re_collection_num = re.search(r'":\d{1,20}', response.text)
            # print(re_collection_num.group())
            goods_dict['人气数'] = self.re_html(re_collection_num.group())
            # print(goods_dict)
            response.close()
            self.parse_goods_comment_num(goods_dict)
        except:
            print(444444444444444444, traceback.format_exc())

    # 抓取商品评论数
    def parse_goods_comment_num(self, goods_dict):
        try:
            url = 'https://dsr-rate.tmall.com/list_dsr_info.htm?itemId={}&spuId={}&sellerId={}&groupId&_ksTS=1564105737969_212&callback=jsonp213'.format(goods_dict['itemId'], goods_dict['spuid'], goods_dict['sellerId'])
            headers = {
                'cookie': 't=b5285c592f5c5d2760bbc606138d8cf0; UM_distinctid=16a1fadfa62540-0819221c6d91c7-47e1137-232800-16a1fadfa636f7; thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; enc=vn%2BuDgMTTmiEXbq1S%2Byw3qmgOc2O1Fw5PzezL1S7UyTFAqMoepiGRIdTY9msHIOrzffqeq9FLJt5WAGM7ENyvA%3D%3D; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; _uab_collina=155540168306791903478476; mt=ci=0_0; cna=At46FUkyQjACAWVWN1V9/Wdy; v=0; cookie2=1ae734c4e8a03d4591a230e3913026b6; _tb_token_=f46b387e3f77e; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; _m_h5_tk=f32553e159b195a4f17c00010f2bcd2e_1564547678304; _m_h5_tk_enc=3268f7bf49fd78b94768c96e3ef51817; uc1=cookie14=UoTaHP3MzJiakA%3D%3D; x5sec=7b227365617263686170703b32223a2266303666326434356635336264366335613639393662663834646632366531644349582b684f6f46454c47473571714e726f325955526f4c4f4459314d5467794d6a4d334f7a453d227d; l=cBaOcPD7qg21z_uyBOfZKurza779uIdf1sPzaNbMiICPO_fh5wONWZFb8t8MCnGVLsI2535t6zUaBXYaGyUIh2nk8b8CgsDd.; isg=BOzsOXiZnf59JomJ--wm9a9SvcreDZFEZ8nHSkYsbxe0UY9baraQ30WjcVnMWcin; JSESSIONID=A9F406FD84CDFD576728A12ECBD98A53',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'Referer': 'https://detail.tmall.com/item.htm?spm=a230r.1.14.6.17c44e4c0Tr15d&id=572069385580&cm_id=140105335569ed55e27b&abbucket=1',
                'User-Agent': random.choice(user_agent_list)
            }
            try:
                time.sleep(0.3)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.4)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.5)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            re_comment_num = re.search(r'{"dsr":{.*}}', response.text)
            goods_dict['评价人数'] = json.loads(re_comment_num.group())['dsr']['rateTotal']
            if int(self.re_not_number(goods_dict['评价人数'])) == 0:
                # logger.log(31, '---------该商品没有评价数据-------')
                response.close()
                pass
            else:
                headers1 = {
                    'cookie': 't=b5285c592f5c5d2760bbc606138d8cf0; UM_distinctid=16a1fadfa62540-0819221c6d91c7-47e1137-232800-16a1fadfa636f7; thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; enc=vn%2BuDgMTTmiEXbq1S%2Byw3qmgOc2O1Fw5PzezL1S7UyTFAqMoepiGRIdTY9msHIOrzffqeq9FLJt5WAGM7ENyvA%3D%3D; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; mt=ci=0_0; cna=At46FUkyQjACAWVWN1V9/Wdy; v=0; cookie2=1ae734c4e8a03d4591a230e3913026b6; _tb_token_=f46b387e3f77e; uc1=cookie14=UoTaHP3MzJiakA%3D%3D; x5sec=7b22726174656d616e616765723b32223a223438663436333231316138653834636332653635613664633664666437363037434b654168656f4645495062705a43337566765a6d51453d227d; _m_h5_tk=b2a5536512217126c542d930817469b0_1564567924778; _m_h5_tk_enc=9e3f2f1eca52726de7c74dd14a9869fa; l=cBaOcPD7qg21z1C9BOCwlurza77ORIRAguPzaNbMi_5dk6Ls857OkSG2UFp6cjWd9pTB41hTyPJ9-etkmI1E1Cmj2s7V.; isg=BC4udOYY_1h3OAv3hZZEU1m4f4Qwh_Mi8XPFVFj3ujHsO8-VwL_0ODk59-dy4-pB',
                    'pragma': 'no-cache',
                    'upgrade-insecure-requests': '1',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Referer': 'https://detail.tmall.com/item.htm?spm=a230r.1.14.16.26804e4ck29eWS&id=597034992998&ns=1&abbucket=1',
                    'User-Agent': random.choice(user_agent_list)
                }
                comment_url = 'https://rate.tmall.com/list_detail_rate.htm?itemId={}&spuId={}&sellerId={}&order=1&currentPage=1'.format(goods_dict['itemId'], goods_dict['spuid'], goods_dict['sellerId'])
                time.sleep(0.3)
                try:
                    response1 = requests.get(url=comment_url, headers=headers1, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.4)
                        response1 = requests.get(url=comment_url, headers=headers1, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.5)
                        response1 = requests.get(url=comment_url, headers=headers1, proxies=proxies, allow_redirects=False, timeout=30)
                comment_data = response1.text
                comment = re.search(r'{"rateDetail":{"rateCount":{"total":.*"tags":\[]}}', comment_data)
                pages_data = json.loads(comment.group())['rateDetail']['paginator']['items']
                pages_num = int(math.ceil(float(int(pages_data) / 20)))
                response1.close()
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
            spuId = goods_dict['spuid']

            headers = {
                'cookie': 't=b5285c592f5c5d2760bbc606138d8cf0; UM_distinctid=16a1fadfa62540-0819221c6d91c7-47e1137-232800-16a1fadfa636f7; thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; enc=vn%2BuDgMTTmiEXbq1S%2Byw3qmgOc2O1Fw5PzezL1S7UyTFAqMoepiGRIdTY9msHIOrzffqeq9FLJt5WAGM7ENyvA%3D%3D; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; mt=ci=0_0; cna=At46FUkyQjACAWVWN1V9/Wdy; v=0; cookie2=1ae734c4e8a03d4591a230e3913026b6; _tb_token_=f46b387e3f77e; uc1=cookie14=UoTaHP3MzJiakA%3D%3D; x5sec=7b22726174656d616e616765723b32223a223438663436333231316138653834636332653635613664633664666437363037434b654168656f4645495062705a43337566765a6d51453d227d; _m_h5_tk=b2a5536512217126c542d930817469b0_1564567924778; _m_h5_tk_enc=9e3f2f1eca52726de7c74dd14a9869fa; l=cBaOcPD7qg21z1C9BOCwlurza77ORIRAguPzaNbMi_5dk6Ls857OkSG2UFp6cjWd9pTB41hTyPJ9-etkmI1E1Cmj2s7V.; isg=BC4udOYY_1h3OAv3hZZEU1m4f4Qwh_Mi8XPFVFj3ujHsO8-VwL_0ODk59-dy4-pB',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': 'https://detail.tmall.com/item.htm?spm=a230r.1.14.16.26804e4ck29eWS&id=597034992998&ns=1&abbucket=1',
                'User-Agent': random.choice(user_agent_list)
            }
            if int(pages_num) >= 99:
                pages = 99
            else:
                pages = pages_num
            # logger.log(31, '-------------评论总页数是：%s --------------' % pages)
            # 抓取商品评论链接(总共99页,从1开始)
            for i in range(1, int(pages)+1):
                comment_url = 'https://rate.tmall.com/list_detail_rate.htm?itemId={}&spuId={}&sellerId={}&order=1&currentPage={}'.format(itemId, spuId, sellerId, i)
                print(comment_url)
                # response = requests.get(url=comment_url, headers=headers, proxies=random.choice(proxies), timeout=10)
                time.sleep(0.3)
                try:
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.4)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.5)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                comment_data = response.text
                # logger.log(31, '开始抓取评论')
                # print(comment_data)
                comment = re.search(r'{"rateDetail":{"rateCount":{"total":.*"tags":\[]}}', comment_data)
                # print(comment.group())
                items = json.loads(comment.group())['rateDetail']['rateList']
                # print(items)
                goods_data = dict()
                for item in items:
                    date_data = item['rateDate'].split(' ')[0]
                    time_data = item['rateDate'].split(' ')[1]
                    # print('评论时间', date_data, time_data)
                    try:
                        content = item['appendComment']['content']
                    except:
                        content = ''

                    # 判断评论时间是否在规定的抓取时间内
                    if self.start_time <= item['rateDate']:
                        goods_data['platform'] = goods_dict['平台']
                        goods_data['date'] = date_data.strip()
                        goods_data['time'] = time_data.strip()
                        goods_data['keyword'] = goods_dict['关键词']
                        goods_data['name'] = goods_dict['商品名']
                        goods_data['url'] = goods_dict['URL']
                        goods_data['shop_name'] = goods_dict['shop_name']
                        goods_data['user_name'] = item['displayUserNick']
                        goods_data['content'] = self.re_html(item['rateContent']) + ';' + str(self.re_html(content))
                        goods_data['content_id'] = str(item['id'])
                        goods_data['brand'] = goods_dict['品牌']
                        goods_data['price'] = goods_dict['价格']
                        goods_data['sales'] = goods_dict['月销量']
                        goods_data['focus_count'] = goods_dict['人气数']
                        goods_data['comment_num'] = goods_dict['评价人数']
                        goods_data['views'] = ''
                        goods_data['likes'] = ''
                        goods_data['comments_count'] = ''
                        goods_data['author_id'] = ''
                        goods_data['reposts_count'] = ''
                        goods_data['topic_id'] = str(goods_dict['itemId'])
                        # 判断size和type
                        test_data = item['auctionSku']
                        if '分类' in test_data:
                            goods_data['type'] = test_data.split(':')[1].replace(';尺码:', '').replace(';鞋码', '').replace(';尺码', '')
                            try:
                                goods_data['size'] = test_data.split(':')[2].split(';')[0]
                            except:
                                try:
                                    goods_data['size'] = test_data.split(':')[2]
                                except:
                                    goods_data['size'] = ''
                        else:
                            goods_data['type'] = ''
                            goods_data['size'] = ''
                        goods_data['imageurl'] = goods_dict['imageurl']
                        goods_data['audiourl'] = goods_dict['audiourl']
                        goods_data['file_code'] = '50'
                        # logger.log(31, '--------********开始写入商品数据********--------')
                        # print(goods_data)
                        item = json.dumps(dict(goods_data), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_2h/ecommerce/{}/{}/50_{}_{}_Tmall_nike{}.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.time_data, str(self.pid)), item, encoding='utf-8')
                    if date_data.strip() <= self.start_time:
                        is_break = True
                if is_break:
                    break
        except:
            print(7777777777777777777, traceback.format_exc())

    def run(self, lock):
        for i in range(1000000):
            lock.acquire()
            redis_url_num = self.redis_example.llen('tian_mao_2h')
            if str(redis_url_num) == '0':
                print('********Redis消息队列中url为空，程序等待中.....进程{}等待中.....*****'.format(str(os.getpid())))
            item = self.redis_example.brpop('tian_mao_2h', timeout=3600)[1]
            lock.release()
            item1 = json.loads(item.decode())
            # print(item)
            self.parse_goods_url(item1)


pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


def app_run():
    lock = threading.Lock()
    spider = Spider(redis_example)
    try:
        spider.run(lock)
    except:
        logger.error('错误为{}'.format(str(traceback.format_exc())))
        print(traceback.format_exc())


if __name__ == "__main__":
    pid = os.getpid()
    hour = str(datetime.now()).split(' ')[1].split(':')[0]
    if 8 <= int(hour) <= 23:
        num = 10
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