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
from with_hdfs import HdfsClient
import urllib3
import redis
import threading
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

    def __init__(self, redis_example):
        # 时间部分,按小时抓取
        date_time = str(datetime.now() - timedelta(days=1)).split('.')[0]
        start_time_test = time.strftime('%Y-%m-%d 00:00:00')

        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        a = end_time.split(' ')[1].split(':')[0]

        if a == '00':
            start_time_data = date_time
            hours_name = '22_24'
            wen_jian_jia_date = str(datetime.now() - timedelta(days=1)).split('.')[0].split(' ')[0].replace('-', '')
        else:
            two_hours_ago = int(a) - 2
            if len(str(two_hours_ago)) == 1:
                two_hour_ago = '0' + str(two_hours_ago)
            else:
                two_hour_ago = str(two_hours_ago)
            hours_name = str(two_hour_ago) + '_' + str(a)
            start_time_data = start_time_test
            wen_jian_jia_date = time.strftime('%Y%m%d')
        print('爬取时间段：{}到{}'.format(start_time_data, end_time))
        logging.info('爬取时间段：{}到{}'.format(start_time_data, end_time))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = start_time_data
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = end_time
        # 标记爬虫工作
        self.is_work = True
        self.redis_example = redis_example
        self.pid = os.getpid()

        self.h2_name = hours_name
        self.date_time = wen_jian_jia_date
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_2h/ecommerce/{}/{}'.format(wen_jian_jia_date, hours_name))  # 创建每日文件夹
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

    # 获取品牌信息
    def parse_brand(self, goods_dict):
        try:
            headers = {
                'content-type': 'text/html;charset=UTF-8',
                'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; _ga=GA1.3.290138937.1568617522; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; KAOLA_USER_ID=109999078912652422; KAOLA_MAIN_ACCOUNT=158651283112426553@pvkaola.163.com; JSESSIONID-WKL-8IO=0zc3WMz%2Bz0rQe5Jcv1xai4OAOScJJgZviUPXMI3RUo2IYlneCBZYhem2pXj85vvoJ8Z%2B2yMxkJZ%2BDbqGhohayCkj0RWfrbvXgwt00Wju%2BMWVg7WjBsfPPuM6Bq0yJI1vkeq%5C17ndJLsLrHGeY1Sf0k231zopBvGmtXomvGZ5J9TWLbPq%3A1586842936344; davisit=2; __da_ntes_utmb=2525167.1.10.1586756536; _samesite_flag_=true; cookie2=1f50b0bd27965ea6d4731440eb0ab6b2; _tb_token_=57e48eee49e7; csg=7c23ee4b; NTES_OSESS=REPpP5MMDS0ti.Kjs4kXCagwqwIe5DsWd2J6spGZnnoVWWhz6L9pI2HlXPVp_85PuZGCsnYofZ0FK56aZ.uX88iBgdi0zJZsRBB8fdi_YIZfYxQlVYg4kvmcVqVCqK9kxhu.Yzv4Avj3rW.UPrCYFGfnrd5TZovCzX0lNqe3j5rAEWHpYRLXj1PsCx_75evCuvl01iv5jej2sgH2yqYAm2a0p; kaola_csg=93dad892; kaola-user-beta-traffic=12217883524; firstLogin=0; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; NTES_KAOLA_RV=1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0|2546067_1569485553114_0|1758828_1569485116618_0|1616628_1569482665961_0|5111078_1569482641632_0|2482224_1569482624326_0; isg=BHV1IQtJR6edB6MO8FzlgBdJhPHvWigPBiZuAfeb4ewRzpfAv0AP1GEMGNLdjkG8',
                'pragma': 'no-cache',
                'referer': 'https://search.kaola.com/search.html?key=AlphaBounce&oldQuery=AIR%2520MAX&searchRefer=searchbutton&zn=top',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            url = goods_dict['url']
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
            # 品牌
            try:
                goods_dict['brand'] = html1.xpath('//dt[@class="orig-country"]/a/text()')[0].spilt(' ')[0]
            except:
                goods_dict['brand'] = ''
            # print(goods_dict)
            self.goods_comments(goods_dict)
        except:
            print(9999999999999999999999, traceback.format_exc())

    # 抓取第一页商品评论
    def goods_comments(self, goods_dict):
        try:
            if int(goods_dict['achieve_num']) == 0:
                pass
                # logger.log(31, '**********---------没有商品评论------************')
            else:
                goods_id = goods_dict['goods_id']
                comment_url = 'https://goods.kaola.com/commentAjax/comment_list_new.json'
                # print(comment_url, goods_id)
                headers = {
                    'authority': 'goods.kaola.com',
                    'method': 'POST',
                    'path': '/commentAjax/comment_list_new.json',
                    'scheme': 'https',
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'zh-CN,zh;q=0.9',
                    'cache-control': 'no-cache',
                    'content-length': '220',
                    'content-type': 'application/x-www-form-urlencoded',
                    'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; _ga=GA1.3.290138937.1568617522; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; KAOLA_USER_ID=109999078912652422; KAOLA_MAIN_ACCOUNT=158651283112426553@pvkaola.163.com; JSESSIONID-WKL-8IO=0zc3WMz%2Bz0rQe5Jcv1xai4OAOScJJgZviUPXMI3RUo2IYlneCBZYhem2pXj85vvoJ8Z%2B2yMxkJZ%2BDbqGhohayCkj0RWfrbvXgwt00Wju%2BMWVg7WjBsfPPuM6Bq0yJI1vkeq%5C17ndJLsLrHGeY1Sf0k231zopBvGmtXomvGZ5J9TWLbPq%3A1586842936344; davisit=2; __da_ntes_utmb=2525167.1.10.1586756536; _samesite_flag_=true; cookie2=1f50b0bd27965ea6d4731440eb0ab6b2; _tb_token_=57e48eee49e7; csg=7c23ee4b; NTES_OSESS=REPpP5MMDS0ti.Kjs4kXCagwqwIe5DsWd2J6spGZnnoVWWhz6L9pI2HlXPVp_85PuZGCsnYofZ0FK56aZ.uX88iBgdi0zJZsRBB8fdi_YIZfYxQlVYg4kvmcVqVCqK9kxhu.Yzv4Avj3rW.UPrCYFGfnrd5TZovCzX0lNqe3j5rAEWHpYRLXj1PsCx_75evCuvl01iv5jej2sgH2yqYAm2a0p; kaola_csg=93dad892; kaola-user-beta-traffic=12217883524; firstLogin=0; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; NTES_KAOLA_RV=1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0|2546067_1569485553114_0|1758828_1569485116618_0|1616628_1569482665961_0|5111078_1569482641632_0|2482224_1569482624326_0; isg=BHV1IQtJR6edB6MO8FzlgBdJhPHvWigPBiZuAfeb4ewRzpfAv0AP1GEMGNLdjkG8',
                    'origin': 'https://goods.kaola.com',
                    'pragma': 'no-cache',
                    'referer': 'https://goods.kaola.com/review/{}.html'.format(str(goods_id)),
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                }
                form_data = {
                    'goodsId': '{}'.format(str(goods_id)),
                    'grade': '0',
                    'tagType': '0',
                    'hasContent': '0',
                    'paginationContext': 'null',
                    'pageNo': '1',
                    'pageSize': '20',
                }
                try:
                    # time.sleep(0.2)
                    response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        # time.sleep(0.2)
                        response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        # time.sleep(0.2)
                        response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
                # print(response.text)
                data = json.loads(response.text)
                # 获取评论列表
                comments_list = data['data']['commentPage']['result']
                if int(len(comments_list)) == 0:
                    return
                else:
                    # 获取当前页数
                    page_data = data['data']['commentPage']['pageNo']
                    # 评价总页数
                    pages_num = data['data']['commentPage']['totalPage']
                    # logger.log(31, '*******************第1页评论****************')
                    for item in comments_list:
                        kao_la_dict = dict()
                        time_data = self.time_change(item['createTime'])
                        # print(data_time_data)
                        try:
                            content = item['commentContent'].replace('\n', ' ')
                        except:
                            content = ''
                        # 追加评论
                        try:
                            comments_2 = item['replyList'][0]['replyContent']
                        except:
                            comments_2 = ''
                        if self.start_time <= time_data:
                            kao_la_dict['platform'] = goods_dict['platform']
                            kao_la_dict['date'] = time_data.split(' ')[0]
                            kao_la_dict['time'] = time_data.split(' ')[1]
                            kao_la_dict['keyword'] = goods_dict['keyword']
                            kao_la_dict['name'] = goods_dict['name']
                            kao_la_dict['imageurl'] = goods_dict['商品图片']
                            kao_la_dict['audiourl'] = ''
                            kao_la_dict['url'] = goods_dict['url']
                            kao_la_dict['shop_name'] = goods_dict['shop_name']
                            kao_la_dict['user_name'] = ''
                            kao_la_dict['content'] = content + ';' + comments_2
                            kao_la_dict['content_id'] = str(item['goodsCommentId'])
                            kao_la_dict['brand'] = goods_dict['brand']
                            kao_la_dict['price'] = goods_dict['price']
                            kao_la_dict['sales'] = goods_dict['sales']
                            kao_la_dict['focus_count'] = ''
                            kao_la_dict['comment_num'] = goods_dict['achieve_num']
                            kao_la_dict['views'] = ''
                            kao_la_dict['likes'] = item['zanCount']
                            kao_la_dict['comments_count'] = ''
                            kao_la_dict['author_id'] = ''
                            kao_la_dict['reposts_count'] = ''
                            kao_la_dict['topic_id'] = str(item['goodsId'])
                            try:
                                kao_la_dict['type'] = item['skuPropertyList'][1]['propertyValue']
                            except:
                                kao_la_dict['type'] = ''
                            try:
                                kao_la_dict['size'] = item['skuPropertyList'][0]['propertyValue']
                            except:
                                kao_la_dict['size'] = ''
                            kao_la_dict['file_code'] = '176'
                            # print(kao_la_dict)
                            item = json.dumps(dict(kao_la_dict), ensure_ascii=False) + '\n'
                            self.hdfsclient.new_write('/user/cspider_daily/nike_2h/ecommerce/{}/{}/176_{}_KaoLa_nike{}.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.pid), item, encoding='utf-8')
                        else:
                            pass
                    if int(page_data) < int(pages_num):
                        # 获取第一页评论最后一个的id以及下一页从哪页跳转参数
                        lastId = data['data']['paginationContext']['lastId']
                        lastPage = data['data']['paginationContext']['lastPage']
                        # print(lastId, lastPage)
                        self.goods_comments_2(lastId, lastPage, goods_id, goods_dict, int(page_data)+1)
                    else:
                        pass
        except:
            print(22222222222222222, traceback.format_exc())

    # 获取第一页之后的所有页面评论
    def goods_comments_2(self, lastId, lastPage, goods_id, goods_dict, i):
        try:
            comment_url = 'https://goods.kaola.com/commentAjax/comment_list_new.json'
            # print(comment_url, goods_id, lastId, lastPage)
            headers = {
                'authority': 'goods.kaola.com',
                'method': 'POST',
                'path': '/commentAjax/comment_list_new.json',
                'scheme': 'https',
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9',
                'cache-control': 'no-cache',
                'content-length': '247',
                'content-type': 'application/x-www-form-urlencoded',
                'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; _ga=GA1.3.290138937.1568617522; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; KAOLA_USER_ID=109999078912652422; KAOLA_MAIN_ACCOUNT=158651283112426553@pvkaola.163.com; JSESSIONID-WKL-8IO=0zc3WMz%2Bz0rQe5Jcv1xai4OAOScJJgZviUPXMI3RUo2IYlneCBZYhem2pXj85vvoJ8Z%2B2yMxkJZ%2BDbqGhohayCkj0RWfrbvXgwt00Wju%2BMWVg7WjBsfPPuM6Bq0yJI1vkeq%5C17ndJLsLrHGeY1Sf0k231zopBvGmtXomvGZ5J9TWLbPq%3A1586842936344; davisit=2; __da_ntes_utmb=2525167.1.10.1586756536; _samesite_flag_=true; cookie2=1f50b0bd27965ea6d4731440eb0ab6b2; _tb_token_=57e48eee49e7; csg=7c23ee4b; NTES_OSESS=REPpP5MMDS0ti.Kjs4kXCagwqwIe5DsWd2J6spGZnnoVWWhz6L9pI2HlXPVp_85PuZGCsnYofZ0FK56aZ.uX88iBgdi0zJZsRBB8fdi_YIZfYxQlVYg4kvmcVqVCqK9kxhu.Yzv4Avj3rW.UPrCYFGfnrd5TZovCzX0lNqe3j5rAEWHpYRLXj1PsCx_75evCuvl01iv5jej2sgH2yqYAm2a0p; kaola_csg=93dad892; kaola-user-beta-traffic=12217883524; firstLogin=0; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; NTES_KAOLA_RV=1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0|2546067_1569485553114_0|1758828_1569485116618_0|1616628_1569482665961_0|5111078_1569482641632_0|2482224_1569482624326_0; isg=BHV1IQtJR6edB6MO8FzlgBdJhPHvWigPBiZuAfeb4ewRzpfAv0AP1GEMGNLdjkG8',
                'origin': 'https://goods.kaola.com',
                'pragma': 'no-cache',
                'referer': 'https://goods.kaola.com/review/{}.html'.format(str(goods_id)),
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
            }
            form_data = {
                'goodsId': '{}'.format(str(goods_id)),
                'grade': '0',
                'tagType': '0',
                'hasContent': '0',
                'showSelfGoodsComment': 'false',
                'paginationContext': {"lastId": '{}'.format(lastId), "lastPage": '{}'.format(lastPage)},
                'pageNo': '{}'.format(i),
                'pageSize': '20',
                'hasInitCommentTab': 'true'
            }
            try:
                # time.sleep(0.2)
                response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.2)
                    response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.2)
                    response = requests.post(url=comment_url, headers=headers, data=form_data, proxies=proxies, allow_redirects=False, timeout=30)
            data = json.loads(response.text)
            # print(data)
            # 获取评论列表
            comments_list = data['data']['commentPage']['result']
            # logger.log(31, '**********************第{}页评论**********************'.format(i))
            if int(len(comments_list)) == 0:
                return
            else:
                # 获取当前页数
                page_data = data['data']['commentPage']['pageNo']
                # 评价总页数
                pages_num = data['data']['commentPage']['totalPage']
                for item in comments_list:
                    kao_la_goods = dict()
                    time_data = self.time_change(item['createTime'])
                    try:
                        content = item['commentContent'].replace('\n', ' ')
                    except:
                        content = ''
                    # 追加评论
                    try:
                        comments_2 = item['replyList'][0]['replyContent']
                    except:
                        comments_2 = ''
                    if self.start_time <= time_data:
                        kao_la_goods['platform'] = goods_dict['platform']
                        kao_la_goods['date'] = time_data.split(' ')[0]
                        kao_la_goods['time'] = time_data.split(' ')[1]
                        kao_la_goods['keyword'] = goods_dict['keyword']
                        kao_la_goods['name'] = goods_dict['name']
                        kao_la_goods['imageurl'] = goods_dict['商品图片']
                        kao_la_goods['audiourl'] = ''
                        kao_la_goods['url'] = goods_dict['url']
                        kao_la_goods['shop_name'] = goods_dict['shop_name']
                        kao_la_goods['user_name'] = ''
                        kao_la_goods['content'] = content + ';' + comments_2
                        kao_la_goods['content_id'] = str(item['goodsCommentId'])
                        kao_la_goods['brand'] = goods_dict['brand']
                        kao_la_goods['price'] = goods_dict['price']
                        kao_la_goods['sales'] = goods_dict['sales']
                        kao_la_goods['focus_count'] = ''
                        kao_la_goods['comment_num'] = goods_dict['achieve_num']
                        kao_la_goods['views'] = ''
                        kao_la_goods['likes'] = item['zanCount']
                        kao_la_goods['comments_count'] = ''
                        kao_la_goods['author_id'] = ''
                        kao_la_goods['reposts_count'] = ''
                        kao_la_goods['topic_id'] = str(item['goodsId'])
                        try:
                            kao_la_goods['type'] = item['skuPropertyList'][1]['propertyValue']
                        except:
                            kao_la_goods['type'] = ''
                        try:
                            kao_la_goods['size'] = item['skuPropertyList'][0]['propertyValue']
                        except:
                            kao_la_goods['size'] = ''
                        kao_la_goods['file_code'] = '176'
                        # print(kao_la_goods)
                        item = json.dumps(dict(kao_la_goods), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_2h/ecommerce/{}/{}/176_{}_KaoLa_nike{}.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.pid), item, encoding='utf-8')
                    else:
                        pass

                if int(page_data) < int(pages_num):
                    # 获取第2页评论最后一个的id以及下一页从哪页跳转参数
                    lastId = data['data']['paginationContext']['lastId']
                    lastPage = data['data']['paginationContext']['lastPage']
                    i += 1
                    self.goods_comments_2(lastId, lastPage, goods_id, goods_dict, i)
                else:
                    pass
        except:
            print(3333333333333333333, traceback.format_exc())

    # # 读取excel获取关键词
    # def parse_xlsx(self):
    #     # 设置路径
    #     path = './快消采集关键词_0916_v3-1.xlsx'
    #     # 打开execl
    #     workbook = xlrd.open_workbook(path)
    #
    #     # 根据sheet索引或者名称获取sheet内容
    #     Data_sheet = workbook.sheets()[0]  # 通过索引获取
    #
    #     rowNum = Data_sheet.nrows  # sheet行数
    #     colNum = Data_sheet.ncols  # sheet列数
    #
    #     # 获取所有单元格的内容
    #     list = []
    #     for i in range(rowNum):
    #         rowlist = []
    #         for j in range(colNum):
    #             rowlist.append(Data_sheet.cell_value(i, j))
    #         list.append(rowlist)
    #
    #     for data in list[1::]:
    #         brand = data[0]
    #         # print(brand)
    #         yield {
    #             '关键词': brand,
    #         }

    def run(self, lock):
        for num in range(1000000):
            lock.acquire()
            redis_url_num = self.redis_example.llen('kaola_2h_url')
            if str(redis_url_num) == '0':
                print('**********Redis消息队列中url为空.....进程 {} 抓取结束......************'.format(str(os.getpid())))
                return
            item = self.redis_example.brpop('kaola_2h_url', timeout=3600)[1]
            lock.release()
            item1 = json.loads(item.decode())
            # print(item)
            self.parse_brand(item1)


pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


def Kaola_run():
    lock = threading.Lock()
    spider = Spider(redis_example)
    try:
        spider.run(lock)
    except:
        logger.error('pid={}错误为{}'.format(str(os.getpid()), str(traceback.format_exc())))
        print(traceback.format_exc())


if __name__ == "__main__":
    pid = os.getpid()
    hour = str(datetime.now()).split(' ')[1].split(':')[0]
    if 8 <= int(hour) <= 23:
        num = 3
    else:
        num = 1
    pool = multiprocessing.Pool(processes=int(num))
    for i in range(int(num)):
        pool.apply_async(Kaola_run, args=())
    pool.close()
    # pool.join()

    # 程序计时，两小时后结束任务
    py_start_time = time.time()
    while True:
        if (float(time.time()) - float(py_start_time)) > 7193:
            logger.log(31, u'爬取时间已经达到两小时，结束进程任务：' + str(pid))
            pool.terminate()
            break
        time.sleep(1)