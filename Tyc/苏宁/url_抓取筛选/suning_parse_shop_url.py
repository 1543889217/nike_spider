import os
import requests
from lxml import etree
import json
import re
import random
import math
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
import redis

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

pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class MutilProcess(object):
    """
    进程池
    """

    def __init__(self):
        pass

    def multi_process(self, processes_num, cols_list, Spider):
        pool = multiprocessing.Pool(processes=processes_num)
        for keyword in cols_list:
            spider = Spider(keyword)
            pool.apply_async(spider.run)
        pool.close()
        pool.join()


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
    def parse_goods(self, shop_id):
        try:
            # 根据关键词,例如：洗发水,抓取商品信息
            url = 'https://csearch.suning.com/emall/brandquery/brandstoreQuery.jsonp?btc=30001665&keyword=&cp=0&ps=48&st=&cityid=021&filters=&pcode=&callback=jsonpQueryByKeyword'.format(shop_id)
            headers = {
                'Content-Type': 'application/x-javascript;charset=UTF-8',
                # 'Cookie': 'SN_SESSION_ID=de223691-c3d2-4702-9c31-30e0d59819ab; tradeMA=55; _snvd=1565067528273QvL8ia7lwZC; SN_CITY=20_021_1000267_9264_01_12113_2_0; cityCode=021; districtId=12113; cityId=9264; hm_guid=ca34f536-186e-4619-aa8f-6c8808ee39a6; _df_ud=e64b917e-c77c-46e0-9d10-d84c86c93f3a; _device_session_id=p_806c72c6-6fa6-462d-bf88-f660c7094c1a; _cp_dt=21f7906b-c341-404f-996b-4d4f2e32e4af-70039; smhst=10966209249|0000000000a10757523126|0000000000a10620476914|0000000000a11180422688|0000000000a10966225829|0000000000a769909849|0070230352a10580507394|0070222946a826193435|0000000000a10163182478|0000000000a10964625880|0000000000a10571100966|0070074453; authId=siF2C3CB23149004DA63A974E6C877378F; secureToken=058F00D73A466B6C20C55A1C0D8C2784; _snmc=1; _snsr=direct%7Cdirect%7C%7C%7C; _snma=1%7C156506752678869586%7C1565067526788%7C1565356261349%7C1565748749523%7C96%7C4; _snmp=156574874908598353; _snmb=156574874953123759%7C1565748749548%7C1565748749531%7C1; _snzwt=THiVDC16c8de5e6c6Szi426a3',
                'Host': 'csearch.suning.com',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except (requests.exceptions.ConnectionError, ConnectionResetError):
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except (requests.exceptions.ConnectionError, ConnectionResetError):
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            # print(response.text)
            # 获取总商品信息
            goods_data = re.search(r'{"goodList":.*"valueDescSpellHeadTure":""}]}]}|{"totalGoodsCount":.*"valueDescSpellHeadTure":""}]}]}', response.text).group()
            # 商品总数
            goods_num = json.loads(goods_data)['totalGoodsCount']
            page_num = int(math.ceil(float(int(self.re_not_number(goods_num)) / 48)))
            print('----------------商品总数是: %s ,商品总页数是: %s------------------------' % (goods_num, page_num))
            for i in range(0, int(page_num)):
                url1 = 'https://csearch.suning.com/emall/brandquery/brandstoreQuery.jsonp?btc={}&keyword=&cp={}&ps=48&st=&cityid=021&filters=&pcode=&callback=jsonpQueryByKeyword'.format(shop_id, i)
                print('************************************正在抓取第: %s 页数据***********************' % i)
                try:
                    time.sleep(0.2)
                    response1 = requests.get(url=url1, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except (requests.exceptions.ConnectionError, ConnectionResetError):
                    try:
                        time.sleep(0.2)
                        response1 = requests.get(url=url1, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except (requests.exceptions.ConnectionError, ConnectionResetError):
                        time.sleep(0.2)
                        response1 = requests.get(url=url1, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                # print(response1.text)
                goods_data1 = re.search(r'{"goodList":.*"valueDescSpellHeadTure":""}]}]}|{"totalGoodsCount":.*"valueDescSpellHeadTure":""}]}]}', response1.text).group()
                # 获取商品列表信息节点
                goods_list = json.loads(goods_data1)['goodList']
                # print(len(goods_list))
                # 遍历商品信息节点列表
                for node in goods_list:
                    goods_dict = dict()
                    goods_dict['平台'] = '苏宁'
                    goods_dict['关键词'] = '耐克'
                    goods_dict['URL'] = 'https:' + node['commidityUrl']
                    goods_id = goods_dict['URL'].split('/')[4].replace('.html', '')
                    # print(goods_id)
                    goods_dict['商品图片'] = self.parse_img(goods_id)
                    goods_dict['商品名'] = node['title']
                    goods_dict['shop_name'] = ' 耐克苏宁自营店'
                    goods_dict['品牌'] = node['brandName'].replace('耐克(', '').replace(')', '')
                    goods_dict['月销量'] = ''
                    goods_dict['价格'] = node['price']
                    goods_dict['评论人数'] = node['countOfarticle']
                    print(goods_dict)
                    self.write_Nike_shop_jsonfile(goods_dict)
        except:
            print(111111111111111111111, traceback.format_exc())
    
    # 抓取商品图片链接
    def parse_img(self, goods_id):
        try:
            headers = {
                'Content-Type': 'application/json;charset=UTF-8',
                'Host': 'shop.suning.com',
                'Pragma': 'no-cache',
                'Referer': 'https://shop.suning.com/30001665/search.html?safp=d488778a.10004.0.f8b310eae6&safpn=10008.00038',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }

            url = 'https://shop.suning.com/mainpicture/mpBatchCallback/batchGetByLocation/0000000000-{}-0-1.jsonp?callback=jQuery1720831887911615117_1585619779208'.format(goods_id)
            response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            # print(response.text)
            json_data = json.loads(response.text.replace('mpBatchCallback(', '').replace(')', ''))[0]
            img_url = 'https://imgservice5.suning.cn' + json_data['pictureUrl']
            # print(img_url)
            return img_url
        except (requests.exceptions.ConnectionError, ConnectionResetError):
            try:
                self.parse_img(goods_id)
            except (requests.exceptions.ConnectionError, ConnectionResetError):
                try:
                    self.parse_img(goods_id)
                except:
                    print(2222222222222222, traceback.format_exc())

    # 写入json文件
    def write_Nike_shop_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_suning_shop_url_{}.json'.format('new', time.strftime('%Y%m%d')), 'ab') as f:
            f.write(item.encode("utf-8"))

    def run(self):
        shop_id = '30001665'
        self.parse_goods(shop_id)


if __name__ == "__main__":
    spider = Spider()
    spider.run()
