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


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        self.headers = {
            'content-type': 'text/html;charset=UTF-8',
            'cookie': 'thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; _uab_collina=156899617481622782959318; ali_ab=124.78.53.29.1571196106895.9; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0; t=8bb0f31c4dfc0a7574d7bab31300c3a5; enc=wJuSe%2FCz3CDzzpv3uW2P8y0JaZZxy2BQVH6MiTXHRQ%2B39U3vqhglmUC66QwzFTEC%2FL5KFFXyuaOVOMPbV07OeQ%3D%3D; cookie2=1131bcacef81a767c6a68429c564a471; _tb_token_=e516e337a5e3e; _samesite_flag_=true; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; sgcookie=EOR4W6tbL0l8qEGtQp7zB; tracknick=; cna=MQj5FQMZD0sCAXxONRZeF0y0; v=0; tfstk=cwgVBSV_3EL2ReT-vrUaG2BF_GGAZDGmsaPQmDRk_ggcnPEciz6TN4XLa5wprof..; x5sec=7b227365617263686170703b32223a223566326265636561616463663533613039323061623337333930363566336664434f666f302f5546454d5341754c66716b75695146786f4c4f4459314d5467794d6a4d334f7a4d3d227d; isg=BLe3XjaWpTqghCL6irIYN-D9RqsBfIvekNBsCwlkDgbtuNT6EU9gL3ramhjmUGNW; l=eBg8HxCHqvIDJwitBO5Nourza77OUIRf1sPzaNbMiIHca18N1Fsc-NQcyS16Rdtjgt5YNetP6fU87RhJSla38xgKqelyRs5mpC968e1..; JSESSIONID=9F4A2A8AF36789A76005A01FB785B4CA',
            'pragma': 'no-cache',
            # 'referer': 'https://s.taobao.com/search?spm=a230r.1.1998181369.d4919860.33a11dc9v7S3ZC&q=%E5%80%92%E9%97%AD%E8%93%9D+%E4%B9%94%E4%B8%B9&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&ie=utf8&initiative_id=tbindexz_20170306&tab=mall',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
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

    def parse_pages(self, key_word, url1, brand):
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
            pattern = re.search(r'{"pageName":"mainsrp","mods":{"shopcombotip":.*"feature":{"webpOff":false,"retinaOff":false,"shopcardOff":true}}',response1.text)
            pages_num = json.loads(pattern.group())['mods']['p4p']['data']['p4pconfig']['pageNum']
            if int(pages_num) >= 100:
                pages = 100
            else:
                pages = int(pages_num)
            print('关键词: %s 搜索到商品总页数是: %s ' % (key_word, pages))
            for i in range(0, int(pages)):
                # print(i)
                print('*********正在抓取第: %s 页商品数据*********' % (int(i)+1))
                url = 'https://s.taobao.com/search?q={}&imgfile=&js=1&stats_click=search_radio_tmall%3A1&initiative_id=staobaoz_{}&tab=mall&ie=utf8&sort=default&bcoffset=0&p4ppushleft=%2C44&s={}'.format(key_word, time.strftime('%Y%m%d'), str(int(i)*44))
                # print('111111111111111111', url)
                self.parse_url(key_word, url, brand)
        except:
            print(key_word, url1)
            print('商品总页数获取失败，重新请求。。。。。。。。。。。。。。')
            time.sleep(3600)
            self.parse_pages(url1, key_word, brand)
            print(2222222222222222222222222222222222, traceback.format_exc())

    # 根据关键词搜索请求得到商品信息
    def parse_url(self, key_word, url1, brand):
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
            # 判断是否有数据
            item_list = json.loads(data0)['mods']['itemlist']['status']
            if item_list == 'show':
                items = json.loads(data0)['mods']['itemlist']['data']['auctions']
                # print(len(items))
                goods_data = dict()
                for item in items:
                    URL = item['detail_url']
                    re_url = re.findall(r'https:', URL)
                    if re_url == []:
                        url = 'https:' + URL
                        # print(url)
                        goods_data['平台'] = '天猫'
                        goods_data['关键词'] = key_word
                        goods_data['品牌'] = brand
                        goods_data['商品名'] = self.re_html(item['title'])
                        goods_data['URL'] = url.replace('#detail', '')
                        goods_data['商品图片'] = 'https:' + item['pic_url']
                        goods_data['价格'] = item['view_price']
                        goods_data['shop_name'] = item['nick']
                        goods_data['itemId'] = item['nid']
                        goods_data['sellerId'] = item['user_id']
                        try:
                            goods_data['月销量'] = item['view_sales'].replace('人收货', '').replace('人付款', '')
                        except:
                            pass
                        if goods_data['URL'].find('taobao.com') > 0:
                            pass
                        else:
                            print(goods_data)
                            self.write_tmall_Nike_jsonfile(goods_data)
            else:
                print('没有商品数据')
        except:
            print('商品翻页失败，翻页重试中。。。。。。。。。。。。。。')
            print(key_word, url1)
            time.sleep(3600)
            self.parse_url(url1, key_word, brand)
            print(2222222222222222222222222222222222, traceback.format_exc())

    # 写入json文件
    def write_tmall_Nike_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./tmall_url/{}_tmall_img_url-1.json'.format('new'), 'ab') as f:
            f.write(item.encode("utf-8"))

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
            brand = data[2]
            key_word = data[0]
            # print(brand)
            yield {
                '关键词': key_word,
                '品牌': brand
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for key_word in key_word_list[0::]:
            # 商品数据，总共100页，从0开始(每页拼接商品数为44)
            # time.sleep(10)
            print('*********正在抓取关键词: %s 的商品数据*********' % key_word['关键词'])
            url1 = 'https://s.taobao.com/search?q={}&imgfile=&js=1&stats_click=search_radio_tmall%3A1&initiative_id=staobaoz_{}&tab=mall&ie=utf8&sort=default&bcoffset=0&p4ppushleft=%2C44&s=4356'.format(key_word['关键词'], time.strftime('%Y%m%d'))
            # print(url1, key_word['关键词'], key_word['品牌'])
            self.parse_pages(key_word['关键词'], url1, key_word['品牌'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
