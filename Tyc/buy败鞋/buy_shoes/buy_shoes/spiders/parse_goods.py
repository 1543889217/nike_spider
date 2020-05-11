# -*- coding: utf-8 -*-
import scrapy
import os
import json
import re
import xlrd
import math
import time
from datetime import datetime
from datetime import timedelta
import logging
import random
import traceback
import multiprocessing
import urllib.parse
from buy_shoes.with_hdfs import HdfsClient


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


class ParseGoodsSpider(scrapy.Spider):
    name = 'parse_goods'
    allowed_domains = ['buyshoe.com']
    start_urls = ['http://buyshoe.com/']

    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
    ]

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
        # 设置路径
        path = './快消采集关键词_v3_20200330.xlsx'
        # 打开execl
        workbook = xlrd.open_workbook(path)
        # 根据sheet索引或者名称获取sheet内容
        Data_sheet = workbook.sheets()[0]  # 通过索引获取
        rowNum = Data_sheet.nrows  # sheet行数
        colNum = Data_sheet.ncols  # sheet列数
        # 获取所有单元格的内容
        list = []
        # print(random.choice(self.user_agent_list))
        for i in range(rowNum):
            rowlist = []
            for j in range(colNum):
                rowlist.append(Data_sheet.cell_value(i, j))
            list.append(rowlist)
        for data in list[1::]:
            key_word = data[0]
            brand = data[2]
            # print(key_word)
            url = 'http://buy.flightclub.cn/lists/{}'.format(key_word)
            headers = {
                'Content-Type': 'text/html; charset=UTF-8',
                'Host': 'buy.flightclub.cn',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': random.choice(self.user_agent_list)
            }
            yield scrapy.Request(
                dont_filter=True,
                url=url,
                headers=headers,
                callback=self.parse_pages,
                meta={'meta_1': key_word, 'meta_2': brand}
            )

    # 根据关键词搜索请求得到商品信息
    def parse_pages(self, response):
        try:
            # 接收meta传参
            key_word = response.meta['meta_1']  # 关键词
            brand = response.meta['meta_2']  # 品牌

            # 商品总数
            goods_num = self.re_not_number(response.xpath('//div[@class="bf_body"]/div/div[1]/div[1]/text()').extract_first())
            if int(goods_num) == 0:
                logger.log(31, '该关键词：%s 搜索不到商品信息' % key_word)
            else:
                # 商品总页数
                pages_num = int(math.ceil(float(int(goods_num) / 39)))
                print('该关键词：%s 搜索到商品总数是: %s ，商品总页数是: %s' % (key_word, goods_num, pages_num))
                for i in range(0, int(pages_num)):
                    goods_url = 'http://buy.flightclub.cn/lists/{}/{}?&'.format(key_word, i)
                    headers = {
                        'Content-Type': 'text/html; charset=UTF-8',
                        'Host': 'buy.flightclub.cn',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': random.choice(self.user_agent_list)
                    }
                    yield scrapy.Request(
                        dont_filter=True,
                        url=goods_url,
                        headers=headers,
                        callback=self.parse_goods,
                        meta={'meta_1': key_word, 'meta2': brand}
                    )
        except:
            print(111111111111111111111, traceback.format_exc())

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, response):
        try:
            # 接收meta传参
            key_word = response.meta['meta_1']  # 关键词
            brand = response.meta['meta2']  # 品牌
            # print(key_word, brand)
            # 获取商品列表
            goods_list = response.xpath('//div[@class="bf_list_content"]/div[@class="bf_list_product_item bf_item_fl"]')
            # print(len(goods_list))
            for item in goods_list:
                goods_dict = dict()
                goods_dict['platform'] = 'BUY败鞋'
                goods_dict['keyword'] = key_word
                goods_dict['name'] = item.xpath('./div[2]/a/text()').extract_first()
                goods_dict['url'] = 'http://buy.flightclub.cn' + item.xpath('./div[2]/a/@href').extract_first()
                goods_dict['imageurl'] = item.xpath('./div[@class="item_img"]/a/img/@src').extract_first()
                goods_dict['audiourl'] = ''
                try:
                    goods_dict['price'] = item.xpath('./div[3]/div[1]/text()').extract_first().replace('￥', '')
                except:
                    goods_dict['price'] = ''
                goods_dict['sales'] = self.re_not_number(item.xpath('./div[3]/div[2]/text()').extract())
                goods_dict['brand'] = brand
                # print(goods_dict)

                url = goods_dict['url']
                headers = {
                    'Content-Type': 'text/html; charset=UTF-8',
                    'Host': 'buy.flightclub.cn',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': random.choice(self.user_agent_list)
                }
                yield scrapy.Request(
                    dont_filter=True,
                    url=url,
                    headers=headers,
                    callback=self.parse_goods_data,
                    meta={'meta_1': goods_dict}
                )
        except:
            print(222222222222222222222, traceback.format_exc())

    def parse_goods_data(self, response):
        try:
            # 接收meta传参
            goods_dict = response.meta['meta_1']
            # 店铺名
            try:
                shop_name = response.xpath('//div[@class="shop_name"]/text()').extract_first()
            except:
                shop_name = ''
            # 获取评论列表
            comment_list = response.xpath('//div[@class="item_user_rates"]/div[@class="item_urate_item"]')
            comment_num = len(comment_list)
            if int(comment_num) == 0:
                # logger.log(31, '该商品没有评论')
                pass
            else:
                goods_comment_dict = dict()
                for item in comment_list:
                    date_time_data = item.xpath('./div[2]/div[2]/text()').extract_first().replace(' ', '')
                    date_data = date_time_data.replace('年', '-').replace('月', '-').replace('日', '').strip()
                    # print(date_data)
                    if self.start_time <= date_data:
                        goods_comment_dict['platform'] = goods_dict['platform']
                        goods_comment_dict['date'] = date_data
                        goods_comment_dict['time'] = ''
                        goods_comment_dict['keyword'] = goods_dict['keyword']
                        goods_comment_dict['name'] = goods_dict['name']
                        goods_comment_dict['imageurl'] = goods_dict['imageurl']
                        goods_comment_dict['audiourl'] = goods_dict['audiourl']
                        goods_comment_dict['url'] = goods_dict['url']
                        goods_comment_dict['shop_name'] = shop_name
                        goods_comment_dict['user_name'] = item.xpath('./div[1]/text()').extract_first()
                        goods_comment_dict['author_id'] = ''
                        goods_comment_dict['content'] = item.xpath('./div[2]/div[1]/text()').extract_first()
                        goods_comment_dict['content_id'] = ''
                        goods_comment_dict['brand'] = goods_dict['brand']
                        goods_comment_dict['price'] = goods_dict['price']
                        goods_comment_dict['type'] = ''
                        goods_comment_dict['size'] = ''
                        goods_comment_dict['sales'] = goods_dict['sales']
                        goods_comment_dict['focus_count'] = ''
                        goods_comment_dict['comment_num'] = comment_num
                        goods_comment_dict['views'] = ''
                        goods_comment_dict['likes'] = ''
                        goods_comment_dict['comments_count'] = ''
                        goods_comment_dict['reposts_count'] = ''
                        goods_comment_dict['topic_id'] = goods_dict['url'].split('product/')[1]
                        goods_comment_dict['file_code'] = '190'
                        # print('正在写入商品评论信息')
                        print(goods_comment_dict)
                        item = json.dumps(dict(goods_comment_dict), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/190_{}_{}_Buyshoes_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                    else:
                        continue
        except:
            print(333333333333333333333, traceback.format_exc())