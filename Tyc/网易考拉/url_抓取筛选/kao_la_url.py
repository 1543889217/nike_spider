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
# from with_hdfs import HdfsClient
import urllib3
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
proxyUser = "HEW657EL99F83S9D"
proxyPass = "8916B1F3F10B1979"

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

    def __init__(self):
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
        self.is_work = True
        # 链接hdfs
        # self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        # self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        # self.time_data = str(time.time()).split('.')[0]

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

    # 13位时间戳转换成日期
    def time_change(self, data):
        timeStamp = float(int(data) / 1000)
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return otherStyleTime

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, key_word):
        try:
            # 输入关键词,例如：洗发水,抓取商品信息
            logger.log(31, '---------------关键词是: %s---------------' % key_word)
            url_index = 'https://search.kaola.com/search.html?key={}&searchRefer=searchbutton&zn=top'.format(key_word)
            headers = {
                'pragma': 'no-cache',
                'referer': 'https://www.kaola.com/?spm=a2v0d.13659821.0.0.7e37b0d87MDuNZ&zn=top',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                # time.sleep(0.2)
                response = requests.get(url=url_index, headers=headers, proxies=proxies, allow_redirects=False, timeout=10)
            except:
                try:
                    # time.sleep(0.2)
                    response = requests.get(url=url_index, headers=headers, proxies=proxies, allow_redirects=False, timeout=10)
                except:
                    # time.sleep(0.2)
                    response = requests.get(url=url_index, headers=headers, proxies=proxies, allow_redirects=False, timeout=10)
            # print(response.text)
            html = etree.HTML(response.text)
            # 判断是否有商品
            test_keyword = html.xpath('//div[@class="correction"]/p[1]//text()')
            if test_keyword != []:
                logger.log(31, '------关键词:%s 搜索不到对应的商品------' % key_word)
            else:
                # 获取商品页数
                pages = self.re_not_number(html.xpath('//div[@class="simplePage"]/span[@class="num"]/text()')[0])
                if int(pages) == 1:
                    # 获取商品列表
                    goods_data_list = html.xpath('//div[@class="resultwrap"]/div[@id="searchresult"]/ul/li')
                    # 遍历商品列表
                    for item in goods_data_list:
                        goods_dict = dict()
                        goods_dict['platform'] = '网易考拉'
                        goods_dict['keyword'] = key_word
                        goods_dict['name'] = item.xpath('./div/a/@title')[0]
                        goods_dict['商品图片'] = 'https:' + item.xpath('.//div[@class="img"]/img/@data-src')[0]
                        re_url = re.search(r'https', item.xpath('./div/a/@href')[0])
                        if re_url:
                            goods_dict['url'] = item.xpath('./div/a/@href')[0]
                        else:
                            goods_dict['url'] = 'https:' + item.xpath('./div/a/@href')[0]
                        try:
                            goods_dict['shop_name'] = item.xpath('//p[@class="selfflag"]/a/text()|//p[@class="selfflag"]/span/text()')[0]
                        except:
                            goods_dict['shop_name'] = ''
                        goods_dict['goods_id'] = self.re_not_number(goods_dict['url'].split('.html')[0])
                        goods_dict['price'] = item.xpath('./div/div[2]/p[1]/span[1]/text()|./div/div/p[1]/span[1]/text()')[0]
                        goods_dict['sales'] = ''
                        goods_dict['achieve_num'] = item.xpath('./div/div[2]/p[3]/a/text()|./div/div/p[3]/a/text()')[0]
                        # print(goods_dict)
                        if goods_dict['achieve_num'] == '' or int(goods_dict['achieve_num']) == 0:
                            pass
                        else:
                            print(goods_dict)
                            self.write_NIKE_jsonfile(goods_dict)
                else:
                    for i in range(1, int(pages)+1):
                        logger.log(31, '。。。。。。关键词： %s  搜索到的商品总页数是：%s , 开始抓取第 %s 页数据。。。。。。。。' % (key_word, pages, i))
                        url = 'https://search.kaola.com/search.html?key={}&pageNo={}&type=&pageSize=60&isStock=false&isSelfProduct=false&isDesc=true&brandId=&proIds=&isSearch=0&isPromote=false&isTaxFree=false&factoryStoreTag=-1&isCommonSort=false&backCategory=&country=&headCategoryId=&needBrandDirect=true&searchRefer=searchbutton&referFrom=searchbutton&referPosition=&lowerPrice=-1&upperPrice=-1&searchType=synonym&changeContent=pageNo'.format(key_word, i)
                        self.parse_kaola(url, headers, key_word)
        except:
            print(111111111111111111111, traceback.format_exc())

    def parse_kaola(self, url, headers, key_word):
        try:
            try:
                # time.sleep(0.2)
                response1 = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.2)
                    response1 = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.2)
                    response1 = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            html1 = etree.HTML(response1.text)
            # 检查商品搜索情况 ：小考拉为你推荐以下商品，表示此句话下面是关键词没有相关的商品
            # test_data = html1.xapth('//div[@class="tip fontype"]/text()')
            # if test_data != []:
            #     goods_data_list = html1.xpath('//div[@class="resultwrap"]/div[@id="searchresult"]/ul/li')
            # 获取商品列表
            goods_data_list = html1.xpath('//div[@class="resultwrap"]/div[@id="searchresult"]/ul/li')
            # 遍历商品列表
            for item in goods_data_list:
                goods_dict = dict()
                goods_dict['platform'] = '网易考拉'
                goods_dict['keyword'] = key_word
                goods_dict['name'] = item.xpath('./div/a/@title')[0]
                goods_dict['商品图片'] = 'https:' + item.xpath('.//div[@class="img"]/img/@data-src')[0]
                re_url = re.search(r'https', item.xpath('./div/a/@href')[0])
                if re_url:
                    goods_dict['url'] = item.xpath('./div/a/@href')[0]
                else:
                    goods_dict['url'] = 'https:' + item.xpath('./div/a/@href')[0]
                try:
                    goods_dict['shop_name'] = item.xpath('//p[@class="selfflag"]/a/text()|//p[@class="selfflag"]/span/text()')[0]
                except:
                    goods_dict['shop_name'] = ''
                goods_dict['goods_id'] = self.re_not_number(goods_dict['url'].split('.html')[0])
                goods_dict['price'] = item.xpath('./div/div[2]/p[1]/span[1]/text()|./div/div/p[1]/span[1]/text()')[0]
                goods_dict['sales'] = ''
                goods_dict['achieve_num'] = item.xpath('./div/div[2]/p[3]/a/text()|./div/div/p[3]/a/text()')[0]
                # print(goods_dict)
                if goods_dict['achieve_num'] == '' or int(goods_dict['achieve_num']) == 0:
                    pass
                else:
                    print(goods_dict)
                    self.write_NIKE_jsonfile(goods_dict)
        except:
            self.parse_kaola(url, headers, key_word)
            print(111111111111111111111, traceback.format_exc())

    # 写入json文件
    def write_NIKE_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_kaola_img_url_{}.json'.format('new', time.strftime('%Y%m%d')), 'ab') as f:
            f.write(item.encode("utf-8"))

    # 读取excel获取关键词
    def parse_xlsx(self):
        # 设置路径
        path = './../快消采集关键词_v3_20200330.xlsx'
        # 打开execl
        workbook = xlrd.open_workbook(path)

        # 根据sheet索引或者名称获取sheet内容
        Data_sheet = workbook.sheets()[0]  # 通过索引获取

        rowNum = Data_sheet.nrows  # sheet行数
        colNum = Data_sheet.ncols  # sheet列数

        # 获取所有单元格的内容
        list = []
        for i in range(rowNum):
            rowlist = []
            for j in range(colNum):
                rowlist.append(Data_sheet.cell_value(i, j))
            list.append(rowlist)

        for data in list[1::]:
            brand = data[0]
            # print(brand)
            yield {
                '关键词': brand,
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for item_data in key_word_list:
            self.parse_goods(item_data['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()


