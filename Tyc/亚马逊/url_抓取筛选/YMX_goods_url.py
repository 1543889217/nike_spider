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


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        # 时间判断部分
        date = datetime.datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]

        now = datetime.datetime.now() - timedelta(days=1)  # 昨天时间
        now_date = str(now).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, now_date))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, now_date))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = now_date
        # 标记爬虫工作
        self.is_work = False

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

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, key_word):
        try:
            # 根据关键词,例如：洗发水,抓取商品信息
            headers = {
                # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                # 'Accept-Encoding': 'gzip, deflate, br',
                # 'Accept-Language': 'zh-CN,zh;q=0.9',
                # 'Cache-Control': 'no-cache',
                # 'Connection': 'keep-alive',
                # 'Cookie': 'session-id=457-6049818-5407509; i18n-prefs=CNY; ubid-acbcn=461-1543774-5730813; x-wl-uid=1D2HfAfNoe4eUdJ6ZzyM2fnvna5QixxATqyW5m655FgD9MFQ0BQOrYAub+2t2juEPWKvSIO9wETU=; lc-acbcn=zh_CN; session-token=OVka/sxMdIyr5yI94QB3yACFmb9UMb+YFgMVOYVlXYNwSXCTnt061bZmS3DgVDd8HYAFX4zi42lZwa7wJ1Npy14+cBpL9vW72zMIIKit9hgaMvC3IdpKCmPIsdhwoO46GfpOzs/ye5A1V+2MY7DWdbBLV1G7xZz5ykr3KQH1pJquUXwV9pWNKg8tdjE/vQEw; session-id-time=2082729601l; csm-hit=tb:Q0KNXH65T2X9SHESP2YH+s-Y51GN3KZ4WJZWEV26T9V|1574233539297&t:1574233539297&adb:adblk_yes',
                'Host': 'www.amazon.cn',
                'Pragma': 'no-cache',
                'Referer': 'https://www.amazon.cn/s?k=%E8%80%90%E5%85%8B&i=shoes&page=2&__mk_zh_CN=%E4%BA%9A%E9%A9%AC%E9%80%8A%E7%BD%91%E7%AB%99&qid=1574233526&ref=sr_pg_2',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            goods_url = 'https://www.amazon.cn/s?k={}&i=shoes&__mk_zh_CN=亚马逊网站&ref=sr_pg_1'.format(key_word)
            #             https://www.amazon.cn/s?k={}&i=shoes&__mk_zh_CN=亚马逊网站&qid=1565244828&ref=sr_pg_1
            # print(goods_url)
            try:
                time.sleep(0.3)
                response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.3)
                    response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.3)
                    response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            # 将响应转换成一个element对象
            html1 = etree.HTML(response1.text)
            # 获取商品总页数
            pages_data1 = html1.xpath('//div[@class="a-text-center"]/ul/li[6]/text()')
            pages_data2 = html1.xpath('//div[@class="a-text-center"]/ul/li[5]/text()')
            pages_data3 = html1.xpath('//div[@class="a-text-center"]/ul/li[4]/text()')
            pages_data4 = html1.xpath('//div[@class="a-text-center"]/ul/li[3]/text()')
            pages_data5 = html1.xpath('//div[@class="a-text-center"]/ul/li[2]/text()')
            print(pages_data5, pages_data4, pages_data3, pages_data2, pages_data1)
            if pages_data1 == [] and pages_data2 == [] and pages_data3 == [] and pages_data4 == [] and pages_data5 == []:
                pages = 1
            elif pages_data1 != []:
                pages = pages_data1[0]
            elif pages_data1 == [] and pages_data2[0] != '...':
                pages = pages_data2[0]
            elif pages_data1 == [] and pages_data2 == [] and pages_data3 != []:
                pages = pages_data3[0]
            elif pages_data1 == [] and pages_data2 == [] and pages_data3 == [] and pages_data4 != []:
                pages = pages_data4[0]
            elif pages_data1 == [] and pages_data2 == [] and pages_data3 == [] and pages_data4 == [] and pages_data5 != []:
                pages = pages_data5[0]
            print(pages)
            # print(re_url)
            for i in range(1, int(pages)+1):
                logger.log(31, '---------------正在抓取第%s页商品信息--------------' % i)
                # 拼接的每一个的商品列表页页
                requests_url = 'https://www.amazon.cn/s?k={}&i=shoes&page={}&__mk_zh_CN=亚马逊网站&ref=sr_pg_{}'.format(key_word, i, i)
                try:
                    time.sleep(0.3)
                    response2 = requests.get(url=requests_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.3)
                        response2 = requests.get(url=requests_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.3)
                        response2 = requests.get(url=requests_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                # 将响应转换成一个element对象
                html2 = etree.HTML(response2.text)
                # 获取商品列表信息节点
                goods_list = html2.xpath('//div[@data-asin and @data-index]')
                # 遍历商品信息节点列表
                for node in goods_list:
                    goods_dict = dict()
                    goods_dict['platform'] = '亚马逊'
                    goods_dict['keyword'] = key_word
                    goods_dict['商品图片'] = node.xpath('.//div[@class="a-section aok-relative s-image-tall-aspect"]/img/@src')[0]
                    name_data = node.xpath('.//span[@class="a-size-base-plus a-color-base a-text-normal"]/text()')[0]
                    goods_dict['name'] = name_data.replace('\xa0', '')
                    goods_dict['url'] = 'https://www.amazon.cn' + node.xpath('.//a[@class="a-link-normal a-text-normal"]/@href')[0]
                    price_data = node.xpath('.//span[@class="a-price"]/span[1]/text()')
                    # print(price_data)
                    try:
                        goods_dict['price'] = price_data[0].replace('￥', '').replace(',', '')
                    except:
                        try:
                            goods_dict['price'] = price_data[0].replace(',', '')
                        except:
                            goods_dict['price'] = ''
                    goods_dict['goods_id'] = goods_dict['url'].split('/')[4]
                    print(goods_dict)
                    self.write_NIKE_jsonfile(goods_dict)
        except:
            print(111111111111111111111111, traceback.format_exc())

    # 写入json文件
    def write_NIKE_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_amazon_img_url_{}.json'.format('new', time.strftime('%Y%m%d')), 'ab') as f:
        # with open('./{}_amazon_goods_url.json'.format('pk'), 'ab') as f:
            f.write(item.encode("utf-8"))

    def parse_xlsx(self):
        # 设置路径
        path = './../快消采集关键词_v3_20200330.xlsx'
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
            # brand = data[2]
            # print(brand)
            yield {
                '关键词': keyword,
                # '品牌': brand
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for item_data in key_word_list[105::]:
            print(item_data)
            self.parse_goods(item_data['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
