# -*- coding: utf-8 -*-
import os
import requests
from lxml import etree
import json
import re
import xlrd
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import math
import multiprocessing
import redis
from wei_pin_hui.with_hdfs import HdfsClient
import urllib3
import scrapy
urllib3.disable_warnings()


pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class ParseVipGoodsSpider(scrapy.Spider):
    name = 'parse_vip_goods'
    allowed_domains = ['vip.com']
    start_urls = ''

    # 时间部分
    # 爬虫开始抓取的日期
    date = datetime.now() - timedelta(days=1)
    py_start_time = str(date).split(' ')[0]
    # 爬虫结束的抓取日期
    current_time = datetime.now()  # 当前日期
    current_day = str(current_time).split(' ')[0]
    print('爬取时间段：{}到{}'.format(py_start_time, current_day))

    # 定义开始时间 y-m-d  离现在时间远  news_start_time
    start_time = py_start_time
    # 定义结束时间 y-m-d  离现在时间近  yesterday
    end_time = current_day
    # 标记爬虫工作
    is_work = True

    # 链接hdfs
    hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
    hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
    time_data = str(time.time()).split('.')[0]

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

    def start_requests(self):
        for i in range(1000000):
            redis_url_num = redis_example.llen('WPH_day_url')
            if str(redis_url_num) == '0':
                print('******************************Redis消息队列中url为空，程序等待中.....***********************')
                return
            item_data = redis_example.brpop('WPH_day_url', timeout=60)[1]
            goods_dict = json.loads(item_data.decode())

            # 将去重的链接存入Redis数据库待抓取
            # for goods_dict in vip_nike_url:
            headers = {
                # 'Cookie': 'vip_rip=101.86.55.85; vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; mars_sid=a369b0e73f9656dbd3eda470968f6cd2; _smt_uid=5d4156d3.52d69d05; VipDFT=1; visit_id=2221152ECC2AD948DF7AB8D56322CE59; vipAc=cf3c0da6d5b52c0f6088b0148efbdb22; vipshop_passport_src=https%3A%2F%2Fdetail.vip.com%2Fdetail-1710618487-6918048587083491095.html; PASSPORT_ACCESS_TOKEN=1FDEBDAAF470FFB2C3C6A9EEAF7256FBA60D1F08; VipRUID=298018734; VipUID=0f94f94cc1ea26b39e78438380499d64; VipRNAME=152*****067; VipLID=0%7C1564973676%7C4b447f; VipDegree=D1; user_class=c; VipUINFO=luc%3Ac%7Csuc%3Ac%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A1; PHPSESSID=b9bnc95dlt7r4eg2r196td02i4; vipte_viewed_=6917921732696396695%2C793920209978892%2C2161644495%2C6918048587083491095%2C6917922115290256471; VipCI_te=0%7C%7C1564974326; _jzqco=%7C%7C%7C%7C%7C1.15943944.1564563154491.1564974076993.1564974326073.1564974076993.1564974326073.0.0.0.39.39; waitlist=%7B%22pollingId%22%3A%22F90BE7CF-3F21-4012-800F-E1F26000E5BF%22%2C%22pollingStamp%22%3A1564974516121%7D; mars_cid=1564563151837_048422ec87f93127ee1eced568a171af',
                'Host': 'detail.vip.com',
                'Pragma': 'no-cache',
                'Referer': goods_dict['url'],
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            url = 'https://detail.vip.com/v2/mapi?_path=rest/content/reputation/getCountBySpuId&spuId={}&brandId={}&app_name=shop_pc'.format(goods_dict['spuId'], goods_dict['brandId'])
            yield scrapy.Request(
                dont_filter=True,
                url=url,
                headers=headers,
                callback=self.parse_comments_num,
                meta={'meta_1': goods_dict}
            )

    # 获取评论量
    def parse_comments_num(self, response): 
        try:
            goods_dict = response.meta['meta_1']
            # print(goods_dict)
            achieve_num_data = json.loads(response.text, strict=False)['data']
            goods_dict['achieve_num'] = achieve_num_data

            page_num = int(math.ceil(float(int(achieve_num_data) / 10)))
            if int(page_num) == 0:
                pages = int(page_num) + 1
            else:
                # logger.log(31, '评论数是: %s , 评论页数是: %s ' % (goods_dict['achieve_num'], str(page_num)))
                # print(goods_dict)
                pages = int(page_num)

            headers = {
                # 'Cookie': 'vip_rip=101.86.55.85; vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; mars_sid=a369b0e73f9656dbd3eda470968f6cd2; _smt_uid=5d4156d3.52d69d05; VipDFT=1; visit_id=2221152ECC2AD948DF7AB8D56322CE59; vipAc=cf3c0da6d5b52c0f6088b0148efbdb22; vipshop_passport_src=https%3A%2F%2Fdetail.vip.com%2Fdetail-1710618487-6918048587083491095.html; PASSPORT_ACCESS_TOKEN=1FDEBDAAF470FFB2C3C6A9EEAF7256FBA60D1F08; VipRUID=298018734; VipUID=0f94f94cc1ea26b39e78438380499d64; VipRNAME=152*****067; VipLID=0%7C1564973676%7C4b447f; VipDegree=D1; user_class=c; VipUINFO=luc%3Ac%7Csuc%3Ac%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A1; PHPSESSID=b9bnc95dlt7r4eg2r196td02i4; vipte_viewed_=6917921732696396695%2C793920209978892%2C2161644495%2C6918048587083491095%2C6917922115290256471; VipCI_te=0%7C%7C1564974326; _jzqco=%7C%7C%7C%7C%7C1.15943944.1564563154491.1564974076993.1564974326073.1564974076993.1564974326073.0.0.0.39.39; waitlist=%7B%22pollingId%22%3A%22F90BE7CF-3F21-4012-800F-E1F26000E5BF%22%2C%22pollingStamp%22%3A1564974516121%7D; mars_cid=1564563151837_048422ec87f93127ee1eced568a171af',
                'Host': 'detail.vip.com',
                'Pragma': 'no-cache',
                # 'Referer': goods_dict['url'],
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            for i in range(1, 4):
                # logger.log(31, '*************************抓取评论第：%s 页' % i)
                url = 'https://detail.vip.com/v2/mapi?_path=rest%2Fcontent%2Freputation%2FqueryBySpuId&spuId={}&brandId={}&page={}&pageSize=10&app_name=shop_pc&app_version=4&source_app=pc&mobile_platform=1&keyWordNlp=%E6%9C%80%E6%96%B0'.format(goods_dict['spuId'], goods_dict['brandId'], i)
                yield scrapy.Request(
                    dont_filter=True,
                    url=url,
                    headers=headers,
                    callback=self.parse_comments,
                    meta={'meta_1': goods_dict}
                )
        except:
            print(1111111111111111111, traceback.format_exc())

    # 获取评论
    def parse_comments(self, response):
        try:
            goods_dict = response.meta['meta_1']

            # 商品评价列表
            comments_list = json.loads(response.text, strict=False)
            if 'data' not in response.text:
                pass
            elif int(len(comments_list['data'])) == 0:
                pass
            else:
                comment_dict = dict()
                for item in comments_list['data']:
                    date_data = self.time_change(item['reputation']['postTime'])
                    # print(date_data)
                    if self.start_time <= date_data.split(' ')[0].strip():
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
                        comment_dict['author_id'] = item['reputationUser']['userIdentity']
                        comment_dict['content'] = item['reputation']['content']
                        comment_dict['content_id'] = item['reputation']['reputationId']
                        comment_dict['brand'] = goods_dict['brand']
                        comment_dict['price'] = goods_dict['price']
                        comment_dict['sales'] = goods_dict['sales']
                        comment_dict['focus_count'] = ''
                        comment_dict['comment_num'] = goods_dict['achieve_num']
                        comment_dict['views'] = ''
                        comment_dict['likes'] = ''
                        comment_dict['comments_count'] = ''
                        comment_dict['reposts_count'] = ''
                        comment_dict['topic_id'] = goods_dict['url'].split('-')[2].replace('.html', '')
                        try:
                            comment_dict['type'] = item['reputationProduct']['colorInfo']
                        except:
                            comment_dict['type'] = ''
                        try:
                            comment_dict['size'] = item['reputationProduct']['size']
                        except:
                            comment_dict['size'] = ''
                        comment_dict['file_code'] = '179'

                        # print('---------------正在写入符合时间的商品评论---------------------')
                        # print(comment_dict)
                        # items = json.dumps(dict(comment_dict), ensure_ascii=False) + '\n'
                        # with open('./{}_vip_goods_NIKE.json'.format(time.strftime('%Y_%m_%d')), 'ab') as f:
                        #     f.write(items.encode("utf-8"))
                        item = json.dumps(dict(comment_dict), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/179_{}_{}_WPH_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
        except:
            print(2222222222222222222, traceback.format_exc())

