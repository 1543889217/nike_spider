import os
import requests
from lxml import etree
import json
import re
import xlrd
import math
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
# from with_hdfs import HdfsClient

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


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        self.headers = {
            # 'Cookie': 'vip_address=%257B%2522pname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522cname%2522%253A%2522%255Cu4e0a%255Cu6d77%255Cu5e02%2522%252C%2522pid%2522%253A%2522103101%2522%252C%2522cid%2522%253A%2522103101101%2522%257D; vip_province=103101; vip_province_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_name=%E4%B8%8A%E6%B5%B7%E5%B8%82; vip_city_code=103101101; vip_wh=VIP_SH; mars_pid=0; _smt_uid=5d760d69.543b963e; vip_cps_cuid=CU1571123480767d7ed602cf3ff50b6f; vip_cps_cid=1571123480772_947a5a4b5bb751303154531b43d74159; PAPVisitorId=16774316e2027e80e776e25bd6a81eac; vip_new_old_user=1; mars_sid=ac6838e6cce7ae841d98221e03d61a05; vipte_viewed_=6918473682797934231%2C6917925712309461911%2C6917921953095107287%2C6917921954088940759; user_class=a; VipUINFO=luc%3Aa%7Csuc%3Aa%7Cbct%3Ac_new%7Chct%3Ac_new%7Cbdts%3A0%7Cbcts%3A0%7Ckfts%3A0%7Cc10%3A0%7Crcabt%3A0%7Cp2%3A0%7Cp3%3A1%7Cp4%3A0%7Cp5%3A0; visit_id=5A74BC5E5ACA5A78DAAB903F4BD347B7; pg_session_no=51; vip_tracker_source_from=; _jzqco=%7C%7C%7C%7C%7C1.856071625.1568017769350.1572231846634.1572231854510.1572231846634.1572231854510.0.0.0.121.121; mars_cid=1568017768340_42e19b11023c204dcce18db1aedeb9a4',
            'Host': 'category.vip.com',
            'Pragma': 'no-cache',
            'Referer': 'https://www.vip.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
        }
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
        # 链接hdfs
        # self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        # self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        # self.time_data = str(time.time()).split('.')[0]

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

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, key_word):
        # 根据关键词,例如：洗发水,抓取商品信息
        try:
            url = 'https://category.vip.com/suggest.php?keyword={}&ff=235|12|1|1'.format(key_word)
            try:
                time.sleep(0.2)
                response = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
            # print(response.text)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 获取商品总页数
            pages = html.xpath('//div[@id="J_pagingCt"]/span[@class="total-item-nums"]/text()')
            if pages == []:
                logger.log(31, '***************关键词：%s搜索不到数据***************' % key_word)
            else:
                pages_num = self.re_not_number(pages[0])
                if '{"code":200,"pageCount"' not in response.text:
                    print('-----当前页面为空-----')
                else:
                    # 获取商品信息
                    goods_data = re.search(r"Var.set\('suggestMerchandiseList'.*code.*200.*?}\);", response.text)
                    # print(goods_data.group())
                    data_json = goods_data.group().replace("Var.set('suggestMerchandiseList', ", '').replace(");", '')
                    # print(data_json)
                    try:
                        checkedWord = json.loads(data_json)['spellcheckV2']['checkedWord']
                        print(checkedWord)
                        print('搜索不到对应关键词的商品')
                    except:
                        for i in range(1, int(pages_num)+1):
                            logger.log(31, '***************关键词： %s , 商品总页数是:  %s  页,正在抓取第：%s 页***************' % (key_word, pages_num, i))
                            data_url = 'https://category.vip.com/suggest.php?keyword={}&page={}&count=50&suggestType=brand'.format(key_word, i)
                            try:
                                # self.parse_goods_url(data_url, key_word)
                                try:
                                    time.sleep(0.2)
                                    response1 = requests.get(url=data_url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                                except:
                                    try:
                                        time.sleep(0.2)
                                        response1 = requests.get(url=data_url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                                    except:
                                        time.sleep(0.2)
                                        response1 = requests.get(url=data_url, headers=self.headers, proxies=proxies, allow_redirects=False, timeout=30)
                                # print(response1.text)
                                if '{"code":200,"pageCount"' not in response1.text:
                                    print('-----当前页面为空-----')
                                else:
                                    # 获取商品信息
                                    goods_data = re.search(r"Var.set\('suggestMerchandiseList'.*code.*200.*?}\);", response1.text)
                                    # print(goods_data.group())
                                    data_json = goods_data.group().replace("Var.set('suggestMerchandiseList', ", '').replace(");", '')
                                    # print(data_json)
                                    # 商品列表
                                    goods_list = json.loads(data_json)['products']
                                    # print(len(goods_list))
                                    if int(len(goods_list)) == 0:
                                        pass
                                    else:
                                        for item in goods_list:
                                            goods_dict = dict()
                                            goods_dict['platform'] = '唯品会'
                                            goods_dict['keyword'] = key_word
                                            goods_dict['name'] = item['brand_show_name'] + ' ' + item['title_no_brand']
                                            goods_dict['商品图片'] = item['small_image']
                                            goods_dict['url'] = 'https://detail.vip.com/detail-{}-{}.html'.format(item['brand_id'], item['product_id'])
                                            goods_dict['brand'] = item['brand_store_name']
                                            # 判断价格
                                            price_test = item['priceIconMsg']
                                            if price_test == '快抢价':
                                                goods_dict['price'] = item['promotionPrice']
                                            else:
                                                goods_dict['price'] = item['price_info']['vipshop_price']
                                            goods_dict['sales'] = ''
                                            goods_dict['spuId'] = item['v_spu_id']
                                            goods_dict['brandId'] = item['brand_id']
                                            print(goods_dict)
                                            self.write_NIKE_jsonfile(goods_dict)
                            except:
                                print(999999999999999999999999, traceback.format_exc())
        except:
            print(22222222222222222, traceback.format_exc())

    # 写入json文件
    def write_NIKE_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_weipinhui_img_url_{}.json'.format('new', time.strftime('%Y%m%d')), 'ab') as f:
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
        for item_data in key_word_list[1::]:
            print(item_data['关键词'])
            self.parse_goods(item_data['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
