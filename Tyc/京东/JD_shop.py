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
        logging.info('爬取时间段：{}到{}'.format(news_start_time, current_day))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = current_day
        # 标记爬虫工作
        self.is_break = False
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
            ret2 = re.sub('<a.*></a>', '', ret1)
            return ret2
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

    def parse_url(self, data):
        # 创建一个字典接收数据
        goods_dict = dict()
        goods_dict['平台'] = data['平台']
        goods_dict['关键词'] = data['关键词']
        goods_dict['商品名'] = data['商品名']
        goods_dict['商品图片'] = data['商品图片']
        goods_dict['URL'] = data['URL']
        goods_dict['shop_name'] = data['shop_name']
        goods_dict['goods_id'] = data['goods_id']
        goods_dict['品牌'] = data['品牌']
        goods_dict['月销量'] = data['月销量']
        # logger.log(31, '--------********正在抓取的商品是：%s********--------' % goods_dict)
        self.parse_goods_price(goods_dict)

    # 解析商品价格信息
    def parse_goods_price(self, goods_dict):
        try:
            goods_url = 'https://p.3.cn/prices/mgets?callback=jQuery6465631&source=jshop&skuids=J_{}'.format(goods_dict['goods_id'])
            headers = {
                'content-type': 'application/json;charset=utf-8',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            price_data = re.search(r'\[{".*"}]', response.text)
            goods_dict['价格'] = json.loads(price_data.group())[0]['p']
            # print(goods_dict)
            self.parse_comment_num(goods_dict)
        except:
            print(22222222222222222222222, traceback.format_exc())

    # 抓取商品评论数
    def parse_comment_num(self, goods_dict):
        try:
            productId = goods_dict['goods_id']
            referer_url = goods_dict['URL']
            comment_url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv46&productId={}&score=0&sortType=6&page=0&pageSize=10&isShadowSku=0&rid=0&fold=1'.format(productId)
            headers = {
                'content-type': 'text/html;charset=GBK',
                'authority': 'sclub.jd.com',
                'method': 'GET',
                # 'cookie': 'shshshfpa=32a16413-dbf0-50ea-e5b3-fc0700600c82-1555380265; shshshfpb=xpfj85AdZf7nEIXa%2FfPnKQA%3D%3D; user-key=76e73b75-478f-450a-843d-e6bc97ab6f57; TrackID=1JkU9AvzDgHTRRBHhgHdYahMQFpg9HwywXxp4mumaDTg3wgCwgl-Om3llO2sZlBTQ7ojPYO3q3E7f1jiEFu3roH67lDo9yP-tEUKh5hPh0R0; pinId=0ng4x50EOTPaVd8k7Hb6MA; pin=t15239619067; unick=t152*****067; _tp=WXVubGec3KjciXDtJzPQhA%3D%3D; _pst=t15239619067; mt_xid=V2_52007VwMWVllaW1scThxaBGIDEFFYXlRbGEwdbARlBkJVVVBVRhwZHV4ZYgRGVEEIVgpMVRxbAWYEQlNfUFQPF3kaXQVvHxNXQVhaSx9JEl8NbAAbYl9oUmoWQRhYBGULEFRVWltTGkkcWgZiMxdb; unpl=V2_ZzNtbRBSRkd2CBFULxxcBmIBFV0SUxYRfFsTAHweWAdiChReclRCFX0UR1FnGVQUZwYZXktcQRRFCEdkeB5fA2AFEFlBZxVLK14bADlNDEY1WnwHBAJfFn0PTlJ7GFQFYwIabXJUQyV1CXZUfx1YB24CEVpHUUIQdQpFUX0fXQJiByJtRWdzJXEMQFF6GGwEVwIiHxYLSxV2CkdTNhlYAWMBG1xBUEYTdA1GVngcWgNmBBdZclZzFg%3d%3d; __jdv=122270672|google-search|t_262767352_googlesearch|cpc|kwd-296971091509_0_c44c21f1e4124361a5d58bde66534872|1555655309636; cn=1; _gcl_au=1.1.1967935789.1555659711; __jdc=122270672; areaId=2; __jdu=15553802647041324770645; __jda=122270672.15553802647041324770645.1555380265.1556415731.1556518168.15; ipLoc-djd=2-2830-51800-0; wlfstk_smdl=zn0664dqolt95jf7g1wjtft1hao7l0yl; 3AB9D23F7A4B3C9B=HPX726VSHMRMSR3STZRR7N5NRDNPYWVN43VETWWM5H7ZKTJNQRUDNAN3OFAJHRA4GMFUVMZ4HQPSNV63PBO6R5QDQI; shshshfp=4a332a1f062877da491a157dabe360b2; shshshsID=60254c5e3d13551f63eed3d934c61d6d_8_1556519503209; __jdb=122270672.11.15553802647041324770645|15.1556518168; JSESSIONID=831DC446C63444F227CAFCFFA4085E88.s1',
                'referer': referer_url,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            comment_data = re.search(r'{"productAttr":null.*]}', response.text)
            comment_number = json.loads(comment_data.group())['productCommentSummary']['commentCount']
            goods_dict['comment_num'] = json.loads(comment_data.group())['productCommentSummary']['commentCountStr']
            # print(comment_number)
            if int(comment_number) == 0:
                logger.log(31, '************该商品没有评论数据*********')
            else:
                pages = int(math.ceil(float(int(comment_number) / 10)))
                self.goods_comments(goods_dict, pages)
        except:
            print(33333333333333333333333, traceback.format_exc())

    # 解析商品评论
    def goods_comments(self, goods_dict, pages):
        try:
            is_break = self.is_break
            # print(goods_dict)
            productId = goods_dict['goods_id']
            referer_url = goods_dict['URL']
            headers = {
                'content-type': 'text/html;charset=GBK',
                'authority': 'sclub.jd.com',
                'method': 'GET',
                # 'cookie': 'shshshfpa=32a16413-dbf0-50ea-e5b3-fc0700600c82-1555380265; shshshfpb=xpfj85AdZf7nEIXa%2FfPnKQA%3D%3D; user-key=76e73b75-478f-450a-843d-e6bc97ab6f57; TrackID=1JkU9AvzDgHTRRBHhgHdYahMQFpg9HwywXxp4mumaDTg3wgCwgl-Om3llO2sZlBTQ7ojPYO3q3E7f1jiEFu3roH67lDo9yP-tEUKh5hPh0R0; pinId=0ng4x50EOTPaVd8k7Hb6MA; pin=t15239619067; unick=t152*****067; _tp=WXVubGec3KjciXDtJzPQhA%3D%3D; _pst=t15239619067; mt_xid=V2_52007VwMWVllaW1scThxaBGIDEFFYXlRbGEwdbARlBkJVVVBVRhwZHV4ZYgRGVEEIVgpMVRxbAWYEQlNfUFQPF3kaXQVvHxNXQVhaSx9JEl8NbAAbYl9oUmoWQRhYBGULEFRVWltTGkkcWgZiMxdb; unpl=V2_ZzNtbRBSRkd2CBFULxxcBmIBFV0SUxYRfFsTAHweWAdiChReclRCFX0UR1FnGVQUZwYZXktcQRRFCEdkeB5fA2AFEFlBZxVLK14bADlNDEY1WnwHBAJfFn0PTlJ7GFQFYwIabXJUQyV1CXZUfx1YB24CEVpHUUIQdQpFUX0fXQJiByJtRWdzJXEMQFF6GGwEVwIiHxYLSxV2CkdTNhlYAWMBG1xBUEYTdA1GVngcWgNmBBdZclZzFg%3d%3d; __jdv=122270672|google-search|t_262767352_googlesearch|cpc|kwd-296971091509_0_c44c21f1e4124361a5d58bde66534872|1555655309636; cn=1; _gcl_au=1.1.1967935789.1555659711; __jdc=122270672; areaId=2; __jdu=15553802647041324770645; __jda=122270672.15553802647041324770645.1555380265.1556415731.1556518168.15; ipLoc-djd=2-2830-51800-0; wlfstk_smdl=zn0664dqolt95jf7g1wjtft1hao7l0yl; 3AB9D23F7A4B3C9B=HPX726VSHMRMSR3STZRR7N5NRDNPYWVN43VETWWM5H7ZKTJNQRUDNAN3OFAJHRA4GMFUVMZ4HQPSNV63PBO6R5QDQI; shshshfp=4a332a1f062877da491a157dabe360b2; shshshsID=60254c5e3d13551f63eed3d934c61d6d_8_1556519503209; __jdb=122270672.11.15553802647041324770645|15.1556518168; JSESSIONID=831DC446C63444F227CAFCFFA4085E88.s1',
                'referer': referer_url,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            if int(pages) >= 50:
                pages_num = 50
            else:
                pages_num = pages
            # 抓取商品评论链接(总共50页,第一页从0开始)
            for i in range(0, int(pages_num)):
                comment_url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv46&productId={}&score=0&sortType=6&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1'.format(productId, i)
                # print(comment_url)
                try:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                # print("正在抓取的页面是: %s" % comment_url)
                comments = response.text
                # print(comments)  # {"productAttr":null,.*]}
                comment = re.search(r'{"productAttr":null.*]}', comments)
                # print(comment.group())
                items = json.loads(comment.group())['comments']
                if int(len(items)) == 0:
                    break
                else:
                    goods_comment_dict = dict()
                    for item in items:
                        date_data = item['creationTime'].split(' ', 1)[0]
                        time_data = item['creationTime'].split(' ', 1)[1]
                        # print(date, time)
                        try:
                            content = self.re_html(item['content'])
                        except:
                            content = ''
                        # 追加评论
                        try:
                            comments_2 = item['afterUserComment']['content']
                        except:
                            comments_2 = ''
                        # 判断评论时间是否在规定的抓取时间内
                        if self.start_time <= date_data.strip():
                            goods_comment_dict['platform'] = goods_dict['平台']
                            goods_comment_dict['date'] = date_data.strip()
                            goods_comment_dict['time'] = time_data.strip()
                            goods_comment_dict['keyword'] = goods_dict['关键词']
                            goods_comment_dict['name'] = goods_dict['商品名']
                            goods_comment_dict['imageurl'] = goods_dict['商品图片']
                            goods_comment_dict['audiourl'] = ''
                            goods_comment_dict['url'] = goods_dict['URL']
                            goods_comment_dict['shop_name'] = goods_dict['shop_name']
                            goods_comment_dict['user_name'] = item['nickname']
                            goods_comment_dict['author_id'] = ''
                            goods_comment_dict['content'] = content + ';' + comments_2
                            goods_comment_dict['content_id'] = str(item['id'])
                            goods_comment_dict['brand'] = goods_dict['品牌']
                            goods_comment_dict['price'] = goods_dict['价格']
                            goods_comment_dict['sales'] = goods_dict['月销量']
                            goods_comment_dict['focus_count'] = ''
                            goods_comment_dict['comment_num'] = goods_dict['comment_num']
                            goods_comment_dict['views'] = ''
                            goods_comment_dict['likes'] = item['usefulVoteCount']
                            try:
                                goods_comment_dict['comments_count'] = item['replyCount']
                            except:
                                goods_comment_dict['comments_count'] = ''
                            goods_comment_dict['reposts_count'] = ''
                            goods_comment_dict['topic_id'] = str(goods_dict['goods_id'])
                            try:
                                goods_comment_dict['type'] = item['productColor']
                            except:
                                goods_comment_dict['type'] = ''
                            try:
                                goods_comment_dict['size'] = item['productSize']
                            except:
                                goods_comment_dict['size'] = ''
                            goods_comment_dict['file_code'] = '51'
                            # print('********--------开始写入商品数据--------********')
                            # print(goods_comment_dict)
                            item = json.dumps(dict(goods_comment_dict), ensure_ascii=False) + '\n'
                            self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/51_{}_{}_jingdong_nike1.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                        if date_data.strip() < self.start_time:
                            is_break = True
                    if is_break:
                        break
        except:
            print(444444444444444444444444444, traceback.format_exc())

    def run(self):
        f = open('./{}_jd_shop_img_url.json'.format('20200407'), 'r', encoding='utf-8')
        goods_data_list = []
        for line in f.readlines():
            dic_data = json.loads(line)
            goods_data_list.append(dic_data)
        for data in goods_data_list:
            self.parse_url(data)


if __name__ == "__main__":
    spider = Spider()
    spider.run()