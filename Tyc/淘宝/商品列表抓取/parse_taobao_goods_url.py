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
import xlrd
import urllib3
import redis
urllib3.disable_warnings()

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
file_name = r"./log_data/{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=file_name,  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
logger.addHandler(headle)

# 代理服务器
proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = "H7307T4706B25G4D"
proxyPass = "05B4877CC39192C0"

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

pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        self.headers = {
            'cookie': 'thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; cna=JJwIFm88vHoCAXxONRZK/wh4; _uab_collina=156879623585972000867667; miid=1519547469416794501; enc=Ou2C4F5cq1kzQbZAdekGrkraJr1Z%2BBqrFrNEfRXzXBRZib0HRdQTu%2BytryPOW6%2Br2W6bdTYAzNcgsEbYDdlXRg%3D%3D; t=dcfe1fe9a0c536637c7622eb6723bb18; cookie2=7405deb2c9bd334711ef9d038576074b; v=0; _tb_token_=f17e8b5e7795b; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; mt=ci%3D-1_1; uc1=cookie14=UoTUPc%2FEV%2F7zeQ%3D%3D; _samesite_flag_=true; _m_h5_tk=a75683c309f0b7a3669997875af8aabe_1587728232493; _m_h5_tk_enc=f269844c15dcfb9aaf8b1ae7bc81b013; x5sec=7b227365617263686170703b32223a223638313230396636623037303836366234626561386131343830646463663564434b57436d665546454b7a666b4a4350345a6d447a774561444445344f4467344d7a59324e4467374d773d3d227d; l=eBxVg1gnqBsf7vh2BO5aourza7795IRb8sPzaNbMiIHca1of9FTV4NQc43AJWdtjgt5A_etPijrhsR3H7kU38KgKqelyRs5mpUp68e1..; isg=BNPTB8EQiVxQBUavtzmfGBfCYlf9iGdK353nFoXwzPIpBPKmDVs3mztWPnRqpL9C; JSESSIONID=D1B28804FDBCC0F7F63F55016082FAEB',
            'pragma': 'no-cache',
            # 'referer': 'https://www.taobao.com/',
            'upgrade-insecure-requests': '1',
            'user-agent': random.choice(user_agent_list)
        }

        # 时间部分
        # 爬虫开始抓取的日期
        date = datetime.now() - timedelta(days=7)
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

    # 根据关键词搜索请求得到商品信息
    def parse_url(self, url, key_word, brand):
        try:
            try:
                time.sleep(3)
                response1 = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(3)
                    response1 = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(3)
                    response1 = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            time.sleep(3)
            # print(response1.text)
            pattern = re.search(r'{"pageName":"mainsrp","mods":{"shopcombotip":.*"feature":{"webpOff":false,"retinaOff":false,"shopcardOff":true}}', response1.text)
            data0 = pattern.group()
            # print(data0)
            pages_num = json.loads(data0)['mods']['p4p']['data']['p4pconfig']['pageNum']
            # if int(pages_num) == 1:
            #     print('-------关键词:%s 搜索不到相关商品数据-------')
            if int(pages_num) >= 100:
                pages = 100
            else:
                pages = pages_num
            print('关键词搜索商品总页数：%s页' % pages)
            for i in range(0, int(pages)):
                # time.sleep(10)
                print('*********正在抓取第: %d页商品数据*********' % (int(i)+1))
                url1 ='https://s.taobao.com/search?q={}&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_{}&ie=utf8&bcoffset=27&ntoffset=27&p4ppushleft=%2C44&sort=default&s={}'.format(key_word, time.strftime('%Y%m%d'), str(int(i)*44))
                self.parse_goods(url1, key_word, brand)
        except:
            print('请求获得总页数失败', key_word, url)
            time.sleep(3600)
            self.parse_url(url, key_word, brand)
            print(111111111111111111111111111111111, traceback.format_exc())

    def parse_goods(self, url1, key_word, brand):
        try:
            try:
                time.sleep(3)
                response1 = requests.get(url=url1, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(3)
                    response1 = requests.get(url=url1, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(3)
                    response1 = requests.get(url=url1, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            time.sleep(3)
            pattern = re.search(r'{"pageName":"mainsrp","mods":{"shopcombotip":.*"feature":{"webpOff":false,"retinaOff":false,"shopcardOff":true}}', response1.text)
            data0 = pattern.group()
            # 判断是否有商品信息
            is_goods = json.loads(data0)['mods']['itemlist']['status']
            if is_goods == 'hide':
                print('没有商品信息')
            else:
                items = json.loads(data0)['mods']['itemlist']['data']['auctions']
                # print(len(items))
                goods_data = dict()
                for item in items:
                    URL = item['detail_url']
                    re_url = re.findall(r'https:', URL)
                    if re_url == []:
                        url = 'https:' + URL
                        # print(url)
                        goods_data['平台'] = '淘宝网'
                        goods_data['关键词'] = key_word
                        goods_data['品牌'] = brand
                        goods_data['商品名'] = self.re_html(item['title'])
                        goods_data['URL'] = self.re_detail(url)
                        goods_data['商品图片'] = 'https:' + item['pic_url']
                        goods_data['价格'] = item['view_price']
                        goods_data['shop_url'] = 'https:' + item['shopLink']
                        try:
                            goods_data['月销量'] = item['view_sales'].replace('人收货', '').replace('人付款', '')
                        except:
                            pass
                        goods_data['itemId'] = item['nid']
                        goods_data['sellerId'] = item['user_id']
                        if goods_data['URL'].find('taobao.com') > 0:
                            print(goods_data)
                            redis_example.lpush('taobao_test_url', json.dumps(goods_data))
                            self.write_taobao_Nike_jsonfile(goods_data)
                        else:
                            pass
        except:
            print(key_word, url1)
            print('翻页重试中。。。。。。。。。。。。。。')
            time.sleep(3600)
            self.parse_goods(url1, key_word, brand)
            print(2222222222222222222222222222222222, traceback.format_exc())

    # 写入json文件
    def write_taobao_Nike_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./tao_bao_url/{}_taobao_img_url-1.json'.format('new'), 'ab') as f:
            f.write(item.encode("utf-8"))

    # 读取excel获取关键词
    def parse_xlsx(self):
        # 设置路径
        path = './快消采集关键词_v3_20200330.xlsx'
        # 打开execl
        workbook = xlrd.open_workbook(path)
        # 根据sheet索引或者名称获取sheet内容
        Data_sheet = workbook.sheets()[0]  # 通过索引获取
        # print(Data_sheet.name)  # 获取sheet名称
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
            keyword = data[0]
            brand = data[2]
            
            # print(brand)
            yield {

                '关键词': keyword,
                '品牌': brand
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for key_word in key_word_list[0::]:
            # 商品数据，总共100页，从0开始(每页拼接商品数为44)
            # time.sleep(1)
            print('*********正在抓取关键词: %s*********' % key_word['关键词'])
            url = 'https://s.taobao.com/search?q={}&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_{}&ie=utf8&bcoffset=-294&ntoffset=-294&p4ppushleft=%2C44&sort=default&s=4356'.format(key_word['关键词'], time.strftime('%Y%m%d'))
            # print(url)
            self.parse_url(url, key_word['关键词'], key_word['品牌'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
