import os
import requests
from lxml import etree
import json
import re
import random
import math
import redis
import threading
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
# file_name = r"./{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
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
    def parse_url(self, data):
        # 创建一个字典接收数据
        goods_dict = dict()
        goods_dict['平台'] = data['平台']
        goods_dict['关键词'] = data['关键词']
        goods_dict['URL'] = data['URL']
        goods_dict['商品图片'] = data['商品图片']
        goods_dict['商品名'] = data['商品名']
        goods_dict['shop_name'] = data['shop_name']
        goods_dict['品牌'] = data['品牌']
        goods_dict['月销量'] = data['月销量']
        goods_dict['价格'] = data['价格']
        goods_dict['评论人数'] = data['评论人数']
        # logger.log(31, '--------********正在抓取的商品是：%s********--------' % goods_dict)
        self.parse_goods_details(goods_dict)

    # 解析商品品牌信息
    def parse_goods_details(self, goods_dict):
        try:
            headers = {
                'Content-Type': 'text/html;charset=utf-8',
                # 'Connection': 'keep-alive',
                # 'Cookie': 'SN_SESSION_ID=c55ac35a-f7d1-4b0c-b48a-f88e8bb896f4; useMp4=1.701108; _snvd=1555383181562rH9y3n/THLV; cityCode=021; districtId=12113; cityId=9264; hm_guid=ac41a4ae-4373-4445-ab29-65e90c29b272; _df_ud=60a62287-237d-4cf0-ada4-d39a276f2c2d; _device_session_id=p_2fb27762-ef79-4f07-9f25-e0acad62907a; _cp_dt=bf4a6a96-909f-450a-b7ca-2d8d0b363cee-86574; _snsr=direct%7Cdirect%7C%7C%7C; _snzwt=THiw3Z16a429d6f24nzVa227f; _snmc=1; city=1000267; province=20; district=10002671; provinceCode=20; districtCode=01; streetCode=0210199; SN_CITY=20_021_1000267_9264_01_12113_1_1; authId=si0BE64747CDCB0EC1B819BB87E6D52FC1; secureToken=E180078268FCC770B6CFC47BFC919E55; _snms=155592217017833779; smhst=651484555|0000000000a10607567457|0000000000a10010138536|0070176294a601763915|0000000000a102374199|0000000000a101822787|0000000000a11012720481|0070752460a11024165323|0070745700a193148008|0000000000a861276981|0000000000a11028136288|0070705161a11002911104|0070756234a101822780|0000000000; _snma=1%7C15553832315961909%7C1555383231596%7C1555923318059%7C1555923324804%7C140%7C9; _snmp=155592332389716467; _snmb=155591411681863515%7C1555923324825%7C1555923324807%7C37',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            # print(goods_dict)
            goods_url = goods_dict['URL']
            # response = requests.get(url=goods_url, headers=headers, proxies=random.choice(proxies), timeout=10)
            try:
                time.sleep(0.2)
                response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except (requests.exceptions.ConnectionError, ConnectionResetError):
                try:
                    time.sleep(0.2)
                    response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
                except (requests.exceptions.ConnectionError, ConnectionResetError):
                    time.sleep(0.2)
                    response = requests.get(url=goods_url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
            # print(response.text)
            # print('正在抓取的页面是: %s' % goods_url)
            data = response.text
            # print(html)
            # 用正则截取价格和评论链接里需要的两串ID
            partNumber = re.search(r'"partNumber".*?,', data)
            vendorCode = re.search(r'"vendorCode".*?,', data)
            # print(partNumber.group(), vendorCode.group())
            goods_dict['partNumber'] = self.re_not_number(partNumber.group())
            goods_dict['vendorCode'] = self.re_not_number(vendorCode.group())
            # print(goods_dict)
            if int(self.re_not_number(goods_dict['评论人数'])) == 0:
                logger.log(31, '-------------没有商品评论信息------------')
            else:
                # 获取评论页数
                page_num = int(math.ceil(float(int(self.re_not_number(goods_dict['评论人数'])) / 10)))
                self.goods_comments(goods_dict, page_num)
        except:
            print(2222222222222222222, traceback.format_exc())

    # 解析商品评论
    def goods_comments(self, goods_dict, page_num):
        try:
            is_break = self.is_break

            partNumber = goods_dict['partNumber']
            vendorCode = goods_dict['vendorCode']
            headers = {
                'Content-Type': 'application/javascript;charset=UTF-8',
                # 'Connection': 'keep-alive',
                # 'Cookie': '_snvd=1555383181562rH9y3n/THLV; cityCode=021; districtId=12113; cityId=9264; hm_guid=ac41a4ae-4373-4445-ab29-65e90c29b272; _df_ud=60a62287-237d-4cf0-ada4-d39a276f2c2d; _device_session_id=p_2fb27762-ef79-4f07-9f25-e0acad62907a; _cp_dt=bf4a6a96-909f-450a-b7ca-2d8d0b363cee-86574; city=1000267; province=20; district=10002671; provinceCode=20; districtCode=01; streetCode=0210199; SN_CITY=20_021_1000267_9264_01_12113_1_1; tradeMA=127; route=3798b42173574ff4536b1645bfa56286; _snzwt=THusFg16a66e65b60nBjXc7ab; _snsr=direct%7Cdirect%7C%7C%7C; _snmc=1; _snms=155652264991095847; authId=si07DE872B7B580CBB2CB11C7105B450A8; secureToken=5C8868551C3103287B59ADEDD6B90567; smhst=192279908|0000000000a600733096|0000000000a600479244|0000000000a10700388709|0070547159a651484540|0000000000a826233089|0000000000a10243606506|0000000000a101822738|0000000000a101822744|0000000000a160764310|0000000000a122819279|0000000000a651484555|0000000000a10607567457|0000000000a10010138536|0070176294a601763915|0000000000a102374199|0000000000a101822787|0000000000a11012720481|0070752460a11024165323|0070745700a193148008|0000000000; _snma=1%7C15553832315961909%7C1555383231596%7C1556524706411%7C1556524786984%7C224%7C15; _snmp=155652478697968344; _snmb=155652102706620667%7C1556524786995%7C1556524786988%7C28',
                'Host': 'review.suning.com',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
            }
            if int(page_num) >= 50:
                pages = 50
            else:
                pages = page_num
            # 抓取商品评论链接(总共50页,第一页从1开始)
            for i in range(1, int(pages)+1):
                comment_url = 'https://review.suning.com/ajax/cluster_review_lists/style--{}-{}-newest-{}-default-10-----reviewList.htm?callback=reviewList'.format(partNumber, vendorCode, i)
                # print(comment_url)
                try:
                    time.sleep(0.2)
                    response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
                except (requests.exceptions.ConnectionError, ConnectionResetError):
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
                    except (requests.exceptions.ConnectionError, ConnectionResetError):
                        time.sleep(0.2)
                        response = requests.get(url=comment_url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
                comment_data = response.text
                # print(comment_data)
                comment = re.search(r'{"commodityReviews":.*"reCloudDrill":0}', comment_data)
                # print(comment.group())
                items = json.loads(comment.group())['commodityReviews']
                if len(items) == 0:
                    break
                else:
                    goods_comment = dict()
                    for item in items:
                        # print(item)
                        date_data = item['publishTime'].split(' ')[0]
                        time_data = item['publishTime'].split(' ')[1]
                        # print(date_data, time_data)
                        try:
                            content = self.re_html(item['content'])
                        except:
                            content = ''
                        # 追加评论
                        try:
                            content_add = item['againReview']['againContent']
                        except:
                            content_add = ''

                        # 判断评论时间是否在规定的抓取时间内
                        if self.start_time <= date_data.strip():
                            goods_comment['platform'] = goods_dict['平台']
                            goods_comment['date'] = date_data.strip()
                            goods_comment['time'] = time_data.strip()
                            goods_comment['keyword'] = goods_dict['关键词']
                            goods_comment['name'] = goods_dict['商品名']
                            goods_comment['imageurl'] = goods_dict['商品图片']
                            goods_comment['audiourl'] = ''
                            goods_comment['url'] = goods_dict['URL']
                            goods_comment['shop_name'] = goods_dict['shop_name']
                            goods_comment['user_name'] = item['userInfo']['nickName']
                            goods_comment['content'] = content + ';' + content_add
                            goods_comment['content_id'] = str(item['commodityReviewId'])
                            goods_comment['brand'] = goods_dict['品牌']
                            goods_comment['price'] = goods_dict['价格']
                            goods_comment['sales'] = goods_dict['月销量']
                            goods_comment['focus_count'] = ''
                            goods_comment['comment_num'] = goods_dict['评论人数']
                            goods_comment['views'] = ''
                            goods_comment['author_id'] = ''
                            goods_comment['reposts_count'] = ''
                            goods_comment['topic_id'] = str(goods_dict['URL'].split('/')[4].replace('.html', ''))
                            test_data = item['commodityInfo']['charaterId1']
                            if test_data == '尺码':
                                goods_comment['type'] = item['commodityInfo']['charaterDesc2']
                                goods_comment['size'] = item['commodityInfo']['charaterDesc1']
                            else:
                                goods_comment['type'] = item['commodityInfo']['charaterDesc1']
                                goods_comment['size'] = item['commodityInfo']['charaterDesc2']
                            self.likes_comments(goods_comment)
                        if date_data.strip() < self.start_time:
                            is_break = True
                    if is_break:
                        break
        except:
            print(3333333333333333333, traceback.format_exc())

    # 解析商品评论的点赞数和回复数
    def likes_comments(self, goods_comment):
        try:
            comment_id = goods_comment['content_id']
            url = 'https://review.suning.com/ajax/useful_count/635960739-usefulCnt.htm'.format(comment_id)
            headers = {
                'Content-Type': 'application/javascript;charset=UTF-8',
                # 'Cookie': 'tradeMA=55; _snvd=1565067528273QvL8ia7lwZC; SN_CITY=20_021_1000267_9264_01_12113_2_0; cityCode=021; districtId=12113; cityId=9264; hm_guid=ca34f536-186e-4619-aa8f-6c8808ee39a6; _df_ud=e64b917e-c77c-46e0-9d10-d84c86c93f3a; _device_session_id=p_806c72c6-6fa6-462d-bf88-f660c7094c1a; _cp_dt=21f7906b-c341-404f-996b-4d4f2e32e4af-70039; route=e46977517568f7cad53fbfe19eaf4774; _snmc=1; _snsr=direct%7Cdirect%7C%7C%7C; authId=siC700F4CB8ABB1C2E87F1FA1E9650CF7A; secureToken=F9331FD98F503CE8898949382003910A; _snzwt=THs64g16ce02abb69OAUS9a89; _snms=156712934067680848; smhst=690105206|0000000000a10118749983|0000000000a10689501376|0070222946a10949954840|0000000000a10966209249|0000000000a10757523126|0000000000a10620476914|0000000000a11180422688|0000000000a10966225829|0000000000a769909849|0070230352a10580507394|0070222946a826193435|0000000000a10163182478|0000000000a10964625880|0000000000a10571100966|0070074453; _snma=1%7C156506752678869586%7C1565067526788%7C1567129356201%7C1567129676548%7C137%7C12; _snmp=156712967506243164; _snmb=156712899210934272%7C1567129676573%7C1567129676552%7C8',
                'Host': 'review.suning.com',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False,timeout=30)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            # print(response.text)
            likes_comments_data = json.loads(response.text.replace('usefulCnt(', '').replace(')', ''))
            goods_comment['likes'] = likes_comments_data['reviewUsefuAndReplylList'][0]['usefulCount']
            goods_comment['comments_count'] = likes_comments_data['reviewUsefuAndReplylList'][0]['replyCount']
            goods_comment['file_code'] = '53'
            # logger.log(31, '****-------正在写入符合时间的商品评论信息-------****')
            item = json.dumps(dict(goods_comment), ensure_ascii=False) + '\n'
            self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/53_{}_{}_Suning_nike_1.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

        except:
            print(4444444444444444444444, traceback.format_exc())

    def run(self):
        f = open('./{}_suning_shop_img_url.json'.format('20200407'), 'r', encoding='utf-8')
        goods_data_list = []
        for line in f.readlines():
            dic_data = json.loads(line)
            goods_data_list.append(dic_data)
        for data in goods_data_list:
            self.parse_url(data)


if __name__ == "__main__":
    spider = Spider()
    spider.run()