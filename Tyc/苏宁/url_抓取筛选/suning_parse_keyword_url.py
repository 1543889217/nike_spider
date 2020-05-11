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
import xlrd

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
file_name = r"./{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
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

pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''
        # 标记爬虫工作
        self.is_break = False

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

    # 过滤商品价格
    def re_price(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'pcData\(', '', message)
            ret2 = re.sub(r'\)', '', ret1)
            return ret2
        except:
            pass

    # 过滤商品品牌信息
    def re_brand(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'"brandName":', '', message)
            ret2 = re.sub(r'&amp;', '', ret1)
            ret3 = re.sub(r'"', '', ret2)
            return ret3
        except:
            pass

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, key_word):
        try:
            is_break = self.is_break
            for i_goods in range(0, 50):
                print('*********----------正在抓取关键词: %s ,第:%s页商品数据----------*********' % (key_word, i_goods))
                # time.sleep(0.5)
                # 根据关键词,例如：洗发水,抓取商品信息
                for i in range(0, 4):
                    url = 'https://search.suning.com/emall/searchV1Product.do?keyword={}&ci=0&pg=01&cp={}&il=0&st=0&iy=0&isDoufu=1&isNoResult=0&n=1&sc=0&sesab=ACAABAABCAAA&id=IDENTIFYING&cc=021&paging={}&sub=0&jzq=262'.format(key_word, i_goods, i)
                    # print(url)

                    headers = {
                        'Connection': 'keep-alive',
                        'Content-Type': 'text/html;charset=utf-8',
                        # 'cookie': 'tradeLdc=NJYH; _snvd=1555383181562rH9y3n/THLV; cityCode=021; districtId=12113; cityId=9264; hm_guid=ac41a4ae-4373-4445-ab29-65e90c29b272; _df_ud=60a62287-237d-4cf0-ada4-d39a276f2c2d; sesab=ACAABAAB; sesabv=15%2C12%2C7%2C1%2C27%2C4%2C3%2C8; _device_session_id=p_2fb27762-ef79-4f07-9f25-e0acad62907a; _cp_dt=bf4a6a96-909f-450a-b7ca-2d8d0b363cee-86574; _snsr=direct%7Cdirect%7C%7C%7C; _snzwt=THiw3Z16a429d6f24nzVa227f; smhst=10010138536|0070176294a601763915|0000000000a102374199|0000000000a101822787|0000000000a11012720481|0070752460a11024165323|0070745700a193148008|0000000000a861276981|0000000000a11028136288|0070705161a11002911104|0070756234a101822780|0000000000; route=1e7d5c18e29ff91e4df466bbda5bbac1; _snmc=1; city=1000267; province=20; district=10002671; provinceCode=20; districtCode=01; streetCode=0210199; SN_CITY=20_021_1000267_9264_01_12113_1_1; authId=si4A49953AAB4B589F0AE3B9683DB8434E; secureToken=3C31E076792EB3F165F582F2E489DC50; _snma=1%7C15553832315961909%7C1555383231596%7C1555916775278%7C1555918339251%7C116%7C9; _snmp=155591833895298270; _snmb=155591411681863515%7C1555918339271%7C1555918339254%7C13; _snms=155591833927154049',
                        'Host': 'search.suning.com',
                        'Referer': 'https://search.suning.com/%E6%B4%97%E5%8F%91%E6%B0%B4/&iy=0&isNoResult=0&cp=0',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
                    # print(response.text)
                    # 将响应转换成一个element对象
                    html = etree.HTML(response.text)
                    # 获取商品列表信息节点
                    goods_list = html.xpath('//li/div/div')
                    # print(len(goods_list))
                    if int(len(goods_list)) != 0:
                        # 遍历商品信息节点列表
                        for node in goods_list:
                            goods_dict = dict()
                            goods_dict['平台'] = '苏宁'
                            goods_dict['关键词'] = key_word
                            goods_dict['URL'] = 'https:' + node.xpath('./div[1]/div/a/@href')[0]
                            goods_dict['商品名'] = self.re_html(node.xpath('./div[1]/div/a/img/@alt')[0])
                            goods_dict['商品图片'] = 'https:' + node.xpath('./div[1]/div/a/img/@src')[0]
                            try:
                                goods_dict['shop_name'] = node.xpath('.//a[@class="store-name"]/text()|.//a[@class="store-class zy"]/text()|//.//a[@class="store-class hwgzy"]/text()')[0]
                            except:
                                goods_dict['shop_name'] = ''
                            goods_dict['评论数'] = ''
                            print(goods_dict)
                            self.write_Nike_index_jsonfile(goods_dict)
                            # redis_example.lpush('su_ning_url', json.dumps(goods_dict))
                    else:
                        is_break = True
                if is_break:
                    break
        except:
            print(111111111111111111111, traceback.format_exc())

    # 写入json文件
    def write_Nike_index_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_suning_keywords_url_{}.json'.format('new', time.strftime('%Y%m%d')), 'ab') as f:
            f.write(item.encode("utf-8"))

    # 读取关键词
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
            # print(brand)
            yield {
                '关键词': keyword,
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for item_data in key_word_list:
            print(item_data['关键词'])
            self.parse_goods(item_data['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
