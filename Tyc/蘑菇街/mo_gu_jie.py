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
from with_hdfs import HdfsClient

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
proxyUser = "HL89Q19E86E2987D"
proxyPass = "71F33D94CE5F7BF2"

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
        # 时间部分
        # 爬虫开始抓取的日期
        date = datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]

        # 爬虫结束的抓取日期
        current_time = datetime.now()  # 当前日期
        current_day = str(current_time).split(' ')[0]

        print('爬取时间段：{}到{}'.format(news_start_time, current_day))
        logging.info('爬取时间段：{}到{}'.format(news_start_time,current_day))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = current_day
        # 标记爬虫工作
        self.is_work = False
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

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

    # 根据关键词搜索请求得到商品信息
    def parse_goods(self, key_word):
        try:
            # 根据关键词,例如：洗发水,抓取商品信息
            url = 'https://list.mogujie.com/search?q={}&cKey=43&page=1&sort=pop'.format(key_word)
            headers = {
                # 'authority': 'list.mogujie.com',
                # 'method': 'GET',
                # 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                # 'accept-encoding': 'gzip, deflate, br',
                # 'accept-language': 'zh-CN,zh;q=0.9',
                # 'cache-control': 'no-cache',
                'cookie': '__mgjuuid=ebddbce7-601f-4f3d-a860-d5ba8f411688; _TDeParam=1-1RjCYYeGOiwg6JI5UDopvg',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.1)
                response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=20)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                except:
                    time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
            # print(response.text)
            # 判断比对获取的是否是关键词关联搜索的商品
            rewriteKeyword = json.loads(response.text)['result']
            if 'searchRewrite' in rewriteKeyword:
                if rewriteKeyword['searchRewrite']['rewriteKeyword'] == key_word.replace(' ', ''):
                    # 获取商品总数
                    goods_num = json.loads(response.text)['result']['wall']['total']
                    # 商品总页数
                    page_num = int(math.ceil(float(int(goods_num) / 75)))
                    for i in range(1, page_num + 1):
                        logger.log(31, '------正在抓取关键词: %s 的第: %s 页商品数据， 商品总页数是: %s ------' % (key_word, i, page_num))
                        goods_url = 'https://list.mogujie.com/search?q={}&cKey=43&page={}&sort=pop'.format(key_word, i)
                        try:
                            time.sleep(0.2)
                            response1 = requests.get(url=goods_url, headers=headers, allow_redirects=False, timeout=20)
                        except:
                            try:
                                time.sleep(0.2)
                                response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                            except:
                                time.sleep(0.2)
                                response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                        # 获取商品列表信息节点
                        goods_list = json.loads(response1.text)['result']['wall']['docs']
                        # print(len(goods_list))
                        # 遍历商品信息节点列表
                        for node in goods_list:
                            goods_dict = dict()
                            goods_dict['platform'] = '蘑菇街'
                            goods_dict['keyword'] = key_word
                            goods_dict['url'] = node['link']
                            goods_dict['imageurl'] = node['img']
                            goods_dict['audiourl'] = ''
                            goods_dict['name'] = node['title']
                            goods_dict['sales'] = ''
                            goods_dict['price'] = node['price']
                            goods_dict['itemID'] = node['tradeItemId']
                            goods_dict['brand'] = ''
                            goods_dict['focus_count'] = node['cfav']
                            # print(goods_dict)
                            self.parse_goods_details(goods_dict)
                else:
                    logger.log(31, '------关键词: %s 搜索不到对应的商品数据--------' % key_word)
            else:
                # 获取商品总数
                goods_num = json.loads(response.text)['result']['wall']['total']
                # 商品总页数
                page_num = int(math.ceil(float(int(goods_num) / 75)))
                for i in range(1, page_num+1):
                    # logger.log(31, '------正在抓取关键词: %s 的第: %s 页商品数据， 商品总页数是: %s ------' % (key_word, i, page_num))
                    goods_url = 'https://list.mogujie.com/search?q={}&cKey=43&page={}&sort=pop'.format(key_word, i)
                    try:
                        time.sleep(0.2)
                        response1 = requests.get(url=goods_url, headers=headers, allow_redirects=False, timeout=20)
                    except:
                        try:
                            time.sleep(0.2)
                            response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                        except:
                            time.sleep(0.2)
                            response1 = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                    # 获取商品列表信息节点
                    goods_list = json.loads(response1.text)['result']['wall']['docs']
                    # print(len(goods_list))
                    # 遍历商品信息节点列表
                    for node in goods_list:
                        goods_dict = dict()
                        goods_dict['platform'] = '蘑菇街'
                        goods_dict['keyword'] = key_word
                        goods_dict['url'] = node['link']
                        goods_dict['imageurl'] = node['img']
                        goods_dict['audiourl'] = ''
                        goods_dict['name'] = node['title']
                        goods_dict['sales'] = ''
                        goods_dict['price'] = node['price']
                        goods_dict['itemID'] = node['tradeItemId']
                        goods_dict['brand'] = ''
                        goods_dict['focus_count'] = node['cfav']
                        # print(goods_dict)
                        self.parse_goods_details(goods_dict)
        except:
            print(111111111111111111111, traceback.format_exc())

    # 解析商品评论人数
    def parse_goods_details(self, goods_dict):
        try:
            headers = {
                'cookie': '__mgjuuid=7e841984-d679-49eb-9994-89abaec55322; _mwp_h5_token_enc=36d248108519bf86cf2fa681dbc521f8; _mwp_h5_token=3c71c26a371458b615f433396b39eccf_1564968570925; _ga=GA1.2.2057442167.1565061045; _gid=GA1.2.2144070558.1565061045; __mgjref=https%3A%2F%2Fshop.mogu.com%2Fdetail%2F1m6os9s%3Facm%3D3.ms.1_4_1m6os9s.43.1185-68998.4aiUQrym0Gs9T.sd_117-swt_43-imt_6-t_4aiUQrym0Gs9T-lc_4-pit_1-qid_21841-dit_170-idx_0-dm1_5001%26ptp%3D31.nXjSr.0.0.wLDh8N89',
                'pragma': 'no-cache',
                'Referer': goods_dict['url'],
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            # print(goods_dict)
            url = 'https://rate.mogu.com/jsonp/pc.rate.ratelist/v2?pageSize=20&sort=1&isNewDetail=1&itemId={}&type=1&marketType=market_mogujie&page=1'.format(goods_dict['itemID'])
            try:
                time.sleep(0.2)
                response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=20)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
            # print(response.url)
            # print(response.text)
            commnent_num_data = re.search(r'{".*"success":true}', response.text)
            num_data = commnent_num_data.group()
            # print(num_data)
            if 'total'not in num_data:
                pass
                # logger.log(31, '-----------没有商品评论数据-----------')
            else:
                goods_dict['achieve_num'] = json.loads(num_data)['data']['total']
                # 获取评论页数
                page_num = int(math.ceil(float(int(goods_dict['achieve_num']) / 20)))
                # print(goods_dict['achieve_num'], page_num)
                self.goods_comments(goods_dict, page_num)
        except:
            print(2222222222222222222, traceback.format_exc())

    # 解析商品评论
    def goods_comments(self, goods_dict, page_num):
        try:
            is_break = self.is_work
            headers = {
                'cookie': '__mgjuuid=7e841984-d679-49eb-9994-89abaec55322; _mwp_h5_token_enc=36d248108519bf86cf2fa681dbc521f8; _mwp_h5_token=3c71c26a371458b615f433396b39eccf_1564968570925; _ga=GA1.2.2057442167.1565061045; _gid=GA1.2.2144070558.1565061045; __mgjref=https%3A%2F%2Fshop.mogu.com%2Fdetail%2F1m6os9s%3Facm%3D3.ms.1_4_1m6os9s.43.1185-68998.4aiUQrym0Gs9T.sd_117-swt_43-imt_6-t_4aiUQrym0Gs9T-lc_4-pit_1-qid_21841-dit_170-idx_0-dm1_5001%26ptp%3D31.nXjSr.0.0.wLDh8N89',
                'pragma': 'no-cache',
                'Referer': goods_dict['url'],
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            # 抓取商品评论链接(总共50页,第一页从1开始)
            for i in range(1, int(page_num)+1):
                comment_url = 'https://rate.mogu.com/jsonp/pc.rate.ratelist/v2?pageSize=20&sort=1&isNewDetail=1&itemId={}&type=1&marketType=market_mogujie&page={}'.format(goods_dict['itemID'], i)
                # print(comment_url)
                # response = requests.get(url=comment_url, headers=headers, proxies=random.choice(proxies), timeout=10)
                try:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, allow_redirects=False, timeout=20)
                except:
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                    except:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=20)
                # print(comment_data)
                comment = re.search(r'{".*"success":true}', response.text)
                # print(comment.group())
                items = json.loads(comment.group())['data']['list']
                # print(len(items))

                goods_comment = dict()
                for item in items:
                    # print(item)
                    date_data = item['time'].replace('年', '-').replace('月', '-').replace('日', '')
                    if len(date_data.split('-')) == 2:
                        date_data_test = time.strftime('%Y') + '-' + date_data
                    else:
                        date_data_test = date_data
                    # print(date_data_test)
                    # 判断评论时间是否在规定的抓取时间内
                    if self.start_time <= date_data_test.strip():
                        goods_comment['platform'] = goods_dict['platform']
                        goods_comment['date'] = date_data_test.strip()
                        goods_comment['time'] = ''
                        goods_comment['keyword'] = goods_dict['keyword']
                        goods_comment['name'] = goods_dict['name']
                        goods_comment['imageurl'] = goods_dict['imageurl']
                        goods_comment['audiourl'] = goods_dict['audiourl']
                        goods_comment['url'] = goods_dict['url']
                        goods_comment['shop_name'] = ''
                        goods_comment['user_name'] = item['user']['uname']
                        goods_comment['content'] = item['content']
                        goods_comment['content_id'] = item['rateId']
                        goods_comment['brand'] = goods_dict['brand']
                        goods_comment['price'] = goods_dict['price']
                        goods_comment['sales'] = goods_dict['sales']
                        goods_comment['focus_count'] = goods_dict['focus_count']
                        goods_comment['comment_num'] = goods_dict['achieve_num']
                        goods_comment['views'] = ''
                        goods_comment['likes'] = ''
                        goods_comment['comments_count'] = ''
                        goods_comment['reposts_count'] = ''
                        goods_comment['author_id'] = item['user']['uid']
                        goods_comment['topic_id'] = goods_dict['itemID']
                        try:
                            goods_comment['type'] = item['style'].split(':')[1].replace(' 尺码', '')
                        except:
                            goods_comment['type'] = ''
                        try:
                            goods_comment['size'] = item['style'].split(':')[2]
                        except:
                            goods_comment['size'] = ''
                        goods_comment['file_code'] = '177'
                        # logger.log(31, '--------------正在写入符合时间的商品评论-----------------------')
                        # print(goods_comment)
                        item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/177_{}_{}_MoGujie_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                    if date_data.strip() < self.start_time:
                        is_break = True
                if is_break:
                    break
        except:
            print(3333333333333333333, traceback.format_exc())

    # 读取excel获取关键词
    def parse_xlsx(self):
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
        for item_dat in key_word_list:
            # print(item_dat['关键词'])
            self.parse_goods(item_dat['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()


