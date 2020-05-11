# -*- coding: utf-8 -*-
import scrapy
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
import math
import redis
import threading
from su_ning_keyword.with_hdfs import HdfsClient

pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class ParseKeywordSuNingSpider(scrapy.Spider):
    name = 'parse_keyword_su_ning'
    allowed_domains = ['suning.com']
    start_urls = ''

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
    start_time = news_start_time
    # 定义结束时间 y-m-d  离现在时间近  yesterday
    end_time = current_day
    # 标记爬虫工作
    is_break = False
    # 链接hdfs
    hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
    hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
    time_data = str(time.time()).split('.')[0]

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
            ret7 = re.sub(r'&hellip;', '', ret6)
            ret8 = re.sub(r'https:', '', ret7)
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

    def start_requests(self):
        # 读取京东url链接以便去重
        # f_suning = open('./{}_Suning_url.json'.format('pk'), 'r', encoding='utf-8')
        #
        # for line in f_suning.readlines():
        #     item_data_dict = json.loads(line)
        # print('去重前苏宁商品链接数：', len(goods_suning_list))
        for i in range(150000):
            redis_url_num = redis_example.llen('suning_day_url')
            if str(redis_url_num) == '0':
                print('******************************Redis消息队列中url为空，程序等待中.....***********************')
            item_data = redis_example.brpop('suning_day_url', timeout=300)[1]
            item_data_dict = json.loads(item_data.decode())
            goods_dict = dict()
            goods_dict['平台'] = item_data_dict['平台']
            goods_dict['关键词'] = item_data_dict['关键词']
            goods_dict['URL'] = item_data_dict['URL']
            goods_dict['商品名'] = item_data_dict['商品名']
            goods_dict['商品图片'] = item_data_dict['商品图片']
            goods_dict['shop_name'] = item_data_dict['shop_name']
            # goods_dict['品牌'] = item_data_dict['brand']
            headers = {
                'Content-Type': 'text/html;charset=utf-8',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            yield scrapy.Request(
                dont_filter=True,
                url=goods_dict['URL'],
                headers=headers,
                callback=self.parse_goods_details,
                meta={'meta_1': goods_dict}
            )

    # 解析商品品牌信息
    def parse_goods_details(self, response):
        try:
            # 接收meta传参
            goods_dict = response.meta['meta_1']
            # 用正则匹配商品品牌
            re_brand = re.search(r'"brandName":".*?"', response.text)
            try:
                brand_name = re_brand.group().replace('"brandName":', '').replace('&amp;', '').replace('"', '')
            except:
                brand_name = ''
            # 用正则截取价格和评论链接里需要的两串ID  "clusterId":"33397412"
            partNumber = re.search(r'"partNumber".*?,', response.text)
            vendorCode = re.search(r'"vendorCode".*?,', response.text)
            # clusterId = re.search(r'"clusterId":".*?"', response.text)
            goods_dict['品牌'] = brand_name
            goods_dict['月销量'] = ''
            goods_dict['partNumber'] = self.re_not_number(partNumber.group())
            goods_dict['vendorCode'] = self.re_not_number(vendorCode.group())
            # goods_dict['clusterId'] = self.re_not_number(clusterId.group())

            headers = {
                'Content-Type': 'text/html; charset=UTF-8',
                'Host': 'pas.suning.com',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            partNumber = goods_dict['partNumber']
            vendorCode = goods_dict['vendorCode']
            price_url = 'https://pas.suning.com/nspcsale_0_{}_{}_{}_20_021_0210101_315587_1000267_9264_12113_Z001___R9000361_1.39_0___000165956__.html?callback=pcData'.format(
                partNumber, partNumber, vendorCode)
            yield scrapy.Request(
                dont_filter=True,
                url=price_url,
                headers=headers,
                callback=self.goods_price,
                meta={'meta_1': goods_dict}
            )
        except:
            print(response.url)
            print(11111111111111, traceback.format_exc())

    # 抓取商品价格
    def goods_price(self, response):
        try:
            # 接收meta传参
            goods_dict = response.meta['meta_1']
            re_price_data = response.text.replace('pcData(', '').replace(')', '')
            price_data = json.loads(re_price_data)['data']['price']['saleInfo'][0]['promotionPrice']
            # print(price_data)
            if price_data.find('-') > 0:
                goods_dict['价格'] = price_data.split('-')[0]
            else:
                goods_dict['价格'] = price_data
            # print(goods_dict)
            headers = {
                'Content-Type': 'application/javascript;charset=UTF-8',
                'Host': 'review.suning.com',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            comment_num_url = 'https://review.suning.com/ajax/cluster_review_satisfy/style--{}-{}-----satisfy.htm?callback=satisfy'.format(goods_dict['partNumber'], goods_dict['vendorCode'])
            yield scrapy.Request(
                dont_filter=True,
                url=comment_num_url,
                headers=headers,
                callback=self.parse_comment_num,
                meta={'meta_1': goods_dict}
            )
        except:
            print(222222222222, traceback.format_exc())

    # 解析商品评价人数
    def parse_comment_num(self, response):
        try:
            # 接收meta传参
            goods_dict = response.meta['meta_1']
            if 'totalCount' in response.text:
                re_comment_num = re.search(r'{"reviewCounts".*"reCloudDrill":0}', response.text)
                goods_dict['评论人数'] = json.loads(re_comment_num.group())['reviewCounts'][0]['totalCount']
                # print(goods_dict)
                if int(self.re_not_number(goods_dict['评论人数'])) == 0:
                    # print('-------------没有商品评论--------------')
                    pass
                else:
                    # 获取评论页数
                    page_num = int(math.ceil(float(int(self.re_not_number(goods_dict['评论人数'])) / 10)))
                    headers = {
                        'Content-Type': 'application/javascript;charset=UTF-8',
                        'Host': 'review.suning.com',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
                    }
                    if int(page_num) >= 50:
                        pages = 50
                    else:
                        pages = page_num
                    partNumber = goods_dict['partNumber']
                    vendorCode = goods_dict['vendorCode']
                    # clusterId = goods_dict['clusterId']
                    # 抓取商品评论链接(总共50页,第一页从1开始)
                    for i in range(1, 4):
                        comment_url = 'https://review.suning.com/ajax/cluster_review_lists/style--{}-{}-newest-{}-default-10-----reviewList.htm?callback=reviewList'.format(partNumber, vendorCode, i)
                        yield scrapy.Request(
                            dont_filter=True,
                            url=comment_url,
                            headers=headers,
                            callback=self.goods_comments,
                            meta={'meta_1': goods_dict}
                        )
        except:
            print(3333333333333, traceback.format_exc())

    # 解析商品评论
    def goods_comments(self, response):
        try:
            # 接收meta传参
            goods_dict = response.meta['meta_1']
            comment_data = response.text
            comment = re.search(r'{"commodityReviews":.*"reCloudDrill":0}', comment_data)
            items = json.loads(comment.group())['commodityReviews']
            if int(len(items)) != 0:
                goods_comment = dict()
                for data in items:
                    # print(data)
                    date_data = data['publishTime'].split(' ')[0]
                    time_data = data['publishTime'].split(' ')[1]
                    # print(date_data.strip(), time_data.strip())
                    try:
                        content = self.re_html(data['content'])
                    except:
                        content = ''
                    # 追加评论
                    try:
                        content_add = data['againReview']['againContent']
                    except:
                        content_add = ''

                    # 判断评论时间是否在规定的抓取时间内
                    if self.start_time <= date_data.strip():
                        goods_comment['platform'] = goods_dict['平台']
                        goods_comment['date'] = date_data.strip()
                        goods_comment['time'] = time_data.strip()
                        goods_comment['keyword'] = goods_dict['关键词']
                        goods_comment['name'] = goods_dict['商品名']
                        goods_comment['imageurl'] = goods_dict['商品图片']
                        goods_comment['audiourl'] = ''
                        goods_comment['url'] = goods_dict['URL']
                        goods_comment['shop_name'] = goods_dict['shop_name']
                        goods_comment['user_name'] = data['userInfo']['nickName']
                        goods_comment['content'] = content + ';' + content_add
                        goods_comment['content_id'] = data['commodityReviewId']
                        goods_comment['brand'] = goods_dict['品牌']
                        goods_comment['price'] = goods_dict['价格']
                        goods_comment['sales'] = goods_dict['月销量']
                        goods_comment['focus_count'] = ''
                        goods_comment['comment_num'] = goods_dict['评论人数']
                        goods_comment['views'] = ''
                        goods_comment['author_id'] = ''
                        goods_comment['reposts_count'] = ''
                        goods_comment['topic_id'] = goods_dict['URL'].split('/')[4].replace('.html', '')
                        try:
                            test_data = data['commodityInfo']['charaterId1']
                            if test_data == '尺码':
                                goods_comment['type'] = data['commodityInfo']['charaterDesc2']
                                goods_comment['size'] = data['commodityInfo']['charaterDesc1']
                            else:
                                goods_comment['type'] = data['commodityInfo']['charaterDesc1']
                                goods_comment['size'] = data['commodityInfo']['charaterDesc2']
                        except:
                            goods_comment['type'] = ''
                            goods_comment['size'] = ''
                        goods_comment['likes'] = '0'
                        goods_comment['comments_count'] = '0'
                        goods_comment['file_code'] = '53'
                        # item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
                        # with open('E:/chance/电商2/平台测试/苏宁/su_ning_keyword/53_{}_{}_Suning_nike.json'.format(time.strftime('%Y%m%d'), self.time_data), 'ab') as f:
                        #     f.write(item.encode("utf-8"))
                        item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/53_{}_{}_Suning_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                    #     headers = {
                    #         'Content-Type': 'application/javascript;charset=UTF-8',
                    #         'Host': 'review.suning.com',
                    #         'Pragma': 'no-cache',
                    #         'Upgrade-Insecure-Requests': '1',
                    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                    #     }
                    #     comment_id = goods_comment['topic_id']
                    #     url = 'https://review.suning.com/ajax/useful_count/{}-usefulCnt.htm'.format(comment_id)
                    #     yield scrapy.Request(
                    #         dont_filter=True,
                    #         url=url,
                    #         headers=headers,
                    #         callback=self.likes_comments,
                    #         meta={'meta_1': goods_comment}
                    #     )
                    # else:
                    #     break
        except:
            print(444444444444, traceback.format_exc())

    # # 解析商品评论的点赞数和回复数
    # def likes_comments(self, response):
    #     try:
    #         # 接收meta传参
    #         goods_comment = response.meta['meta_1']
    #         likes_comments_data = json.loads(response.text.replace('usefulCnt(', '').replace(')', ''))
    #         goods_comment['likes'] = likes_comments_data['reviewUsefuAndReplylList'][0]['usefulCount']
    #         goods_comment['comments_count'] = likes_comments_data['reviewUsefuAndReplylList'][0]['replyCount']
    #         goods_comment['file_code'] = '53'
    #         # print(goods_comment)
    #         # item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
    #         # with open('E:/chance/电商2/平台测试/苏宁/su_ning_keyword/53_{}_{}_Suning_nike.json'.format(time.strftime('%Y%m%d'), self.time_data), 'ab') as f:
    #         #     f.write(item.encode("utf-8"))
    #         item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
    #         self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/53_{}_{}_Suning_nike.json'.format(
    #             time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
    #     except:
    #         print(5555555555555, traceback.format_exc())