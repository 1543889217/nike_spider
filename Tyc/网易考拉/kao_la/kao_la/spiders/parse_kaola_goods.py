# -*- coding: utf-8 -*-
import os
import json
import re
import xlrd
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import math
from kao_la.with_hdfs import HdfsClient
import urllib3
import scrapy
import redis
import random
from collections import OrderedDict
urllib3.disable_warnings()


pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)


class ParseKaolaGoodsSpider(scrapy.Spider):
    name = 'parse_kaola_goods'
    allowed_domains = ['kaola.com']
    start_urls = ''

    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
    ]

    # 时间部分
    # 爬虫开始抓取的日期
    date = datetime.now() - timedelta(days=1)
    py_start_time = str(date).split(' ')[0]
    # 爬虫结束的抓取日期
    current_time = datetime.now()  # 当前日期
    current_day = str(current_time).split(' ')[0]
    print('爬取时间段：{}到{}'.format(py_start_time, current_day))

    # 定义开始时间 y-m-d  离现在时间远  news_start_time
    start_time = py_start_time
    # 定义结束时间 y-m-d  离现在时间近  yesterday
    end_time = current_day
    # 标记爬虫工作
    is_work = True

    # 链接hdfs
    hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
    hdfsclient.makedirs('/user/cspider_daily/nike_daily/ecommerce/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
    time_data = str(time.time()).split('.')[0]

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

    def start_requests(self):
        # # 读取考拉耐克url链接
        # f = open('./{}_kaola_goods_url.json'.format('2019_09_29'), 'r', encoding='utf-8')
        # goods_data_list = []
        # for line in f.readlines():
        #     dic_data = json.loads(line)
        #     goods_data_list.append(dic_data)
        # print('去重前商品链接数: ', len(goods_data_list))
        # a = goods_data_list
        # goods_url = OrderedDict()
        # # 对url链接去重
        # for item in a:
        #     goods_url.setdefault(item['url'], {**item, })
        # kaola_nike_url = list(goods_url.values())
        # print('按规则去重之后商品链接数: ', len(kaola_nike_url))
        for i in range(25000):
            time.sleep(0.5)
            redis_url_num = redis_example.llen('kaola_day_url')
            if str(redis_url_num) == '0':
                print('******************************Redis消息队列中url为空，程序等待中.....***********************')
                return
            item_data = redis_example.brpop('kaola_day_url', timeout=60)[1]
            goods_dict = json.loads(item_data.decode())

            headers = {
                # 'content-type': 'text/html;charset=UTF-8',
                # 'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; _ga=GA1.3.290138937.1568617522; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; KAOLA_USER_ID=109999078912652422; KAOLA_MAIN_ACCOUNT=158651283112426553@pvkaola.163.com; JSESSIONID-WKL-8IO=0zc3WMz%2Bz0rQe5Jcv1xai4OAOScJJgZviUPXMI3RUo2IYlneCBZYhem2pXj85vvoJ8Z%2B2yMxkJZ%2BDbqGhohayCkj0RWfrbvXgwt00Wju%2BMWVg7WjBsfPPuM6Bq0yJI1vkeq%5C17ndJLsLrHGeY1Sf0k231zopBvGmtXomvGZ5J9TWLbPq%3A1586842936344; davisit=2; __da_ntes_utmb=2525167.1.10.1586756536; _samesite_flag_=true; cookie2=1f50b0bd27965ea6d4731440eb0ab6b2; _tb_token_=57e48eee49e7; csg=7c23ee4b; NTES_OSESS=REPpP5MMDS0ti.Kjs4kXCagwqwIe5DsWd2J6spGZnnoVWWhz6L9pI2HlXPVp_85PuZGCsnYofZ0FK56aZ.uX88iBgdi0zJZsRBB8fdi_YIZfYxQlVYg4kvmcVqVCqK9kxhu.Yzv4Avj3rW.UPrCYFGfnrd5TZovCzX0lNqe3j5rAEWHpYRLXj1PsCx_75evCuvl01iv5jej2sgH2yqYAm2a0p; kaola_csg=93dad892; kaola-user-beta-traffic=12217883524; firstLogin=0; hb_MA-AE38-1FCC6CD7201B_source=account.kaola.com; isg=BBwcqzPurrwOd1p1MRucayYG7ToOPcG0t7m32vYd0Ye8QbzLHqStT1Pjp-kcUvgX',
                'pragma': 'no-cache',
                # 'referer': 'https://search.kaola.com/search.html?key=AlphaBounce&oldQuery=AIR%2520MAX&searchRefer=searchbutton&zn=top',
                'upgrade-insecure-requests': '1',
                'user-agent': '{}'.format(random.choice(self.user_agent_list))
            }
            # 将去重的链接存入Redis数据库待抓取
            # for goods_dict in kaola_nike_url:
            # print(goods_dict)
            url_index = goods_dict['url']
            yield scrapy.Request(
                dont_filter=True,
                url=url_index,
                headers=headers,
                callback=self.parse_brand,
                meta={'meta_1': goods_dict}
            )

    # 抓取商品品牌信息
    def parse_brand(self, response):
        try:
            goods_dict = response.meta['meta_1']
            # print(goods_dict)
            # 品牌
            try:
                goods_dict['brand'] = response.xpath('//dt[@class="orig-country"]/a/text()').extract_first().split(' ')[0]
            except:
                goods_dict['brand'] = ''
            # print(goods_dict)
            goods_id = goods_dict['goods_id']
            headers = {
                # 'content-type': 'application/x-www-form-urlencoded',
                'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; _ga=GA1.3.290138937.1568617522; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; _samesite_flag_=true; _tb_token_=57e48eee49e7; csg=7c23ee4b; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; JSESSIONID-WKL-8IO=WrbxWqBstbryTpUoUTTO3lD2kpgUaN%5CQXfBrV7eKj3XYscWBXT3D2m9%5CKMYb%5CbCiR1Ed49%5CxGbTX%2BDQ9%2FYzaxdxG9EvNb%5C%2BPCG79%5CzVz%2B1n4bOLWx6KmfLtZjbNl1aCJ8JDDk9mosiJDeP%2Bco069H0U8bv3E38HZUe80r777GgVcfQCk%3A1587008540939; __da_ntes_utmb=2525167.1.10.1586926851; x5sec=7b227761676272696467652d616c69626162612d67726f75703b32223a226131343164373731633435383465343137653738366530363066326136333038434a4f7132765146454f623871382b676d614c7a7567453d227d; NTES_KAOLA_ADDRESS_CONTROL=310000|310100|310101|1; NTES_KAOLA_RV=1380027_1586926968862_0|6137022_1586917525489_0|6095594_1586915723435_0|1637695_1586832857734_0|6192366_1586830698135_0|1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0; isg=BJeXvumehXigEgHkTuLnigF_JgshdGo1sDCM6-nA1mf2GLNa8a5Mjid-evij8EO2',
                'origin': 'https://goods.kaola.com',
                'pragma': 'no-cache',
                # 'referer': 'https://goods.kaola.com/review/{}.html'.format(str(goods_id)),
                'user-agent': '{}'.format(random.choice(self.user_agent_list))
                # 'x-requested-with': 'XMLHttpRequest'
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
            yield scrapy.FormRequest(
                dont_filter=True,
                url='https://goods.kaola.com/commentAjax/comment_list_new.json',
                headers=headers,
                formdata=form_data,
                callback=self.goods_comment1,
                meta={'meta_1': goods_dict, 'meta_2': goods_id}
            )
        except:
            print(1111111111111111111, traceback.format_exc())

    # 抓取第一页评论数据
    def goods_comment1(self, response):
        try:
            goods_dict = response.meta['meta_1']
            goods_id = response.meta['meta_2']
            # print(goods_dict)
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
                # print('--------------第1页评论---------------------')
                # print('---当前页是: %s ,总页数是: %s ---' % (page_data, pages_num))

                for item in comments_list:
                    kao_la_dict = dict()
                    time_data = self.time_change(item['createTime'])
                    data_time_data = time_data.split(' ')[0]
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
                    if self.start_time <= data_time_data.strip():
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
                        # items = json.dumps(dict(kao_la_dict), ensure_ascii=False) + '\n'
                        # with open('./{}_kao_la_goods_NIKE.json'.format(time.strftime('%Y_%m_%d')), 'ab') as f:
                        #     f.write(items.encode("utf-8"))
                        item = json.dumps(dict(kao_la_dict), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/176_{}_{}_KaoLa_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                #      当前页数         总页数
                if int(page_data) < int(pages_num):
                    # 获取第2页评论最后一个的id以及下一页从哪页跳转参数
                    lastId = data['data']['paginationContext']['lastId']
                    lastPage = data['data']['paginationContext']['lastPage']
                    # print(lastId, lastPage)
                    headers = {
                        # 'authority': 'goods.kaola.com',
                        # 'method': 'POST',
                        # 'path': '/commentAjax/comment_list_new.json',
                        # 'scheme': 'https',
                        # 'accept': '*/*',
                        # 'accept-encoding': 'gzip, deflate, br',
                        # 'accept-language': 'zh-CN,zh;q=0.9',
                        # 'cache-control': 'no-cache',
                        # 'content-length': '247',
                        # 'content-type': 'application/x-www-form-urlencoded',
                        'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; _ga=GA1.3.290138937.1568617522; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; _samesite_flag_=true; _tb_token_=57e48eee49e7; csg=7c23ee4b; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; JSESSIONID-WKL-8IO=WrbxWqBstbryTpUoUTTO3lD2kpgUaN%5CQXfBrV7eKj3XYscWBXT3D2m9%5CKMYb%5CbCiR1Ed49%5CxGbTX%2BDQ9%2FYzaxdxG9EvNb%5C%2BPCG79%5CzVz%2B1n4bOLWx6KmfLtZjbNl1aCJ8JDDk9mosiJDeP%2Bco069H0U8bv3E38HZUe80r777GgVcfQCk%3A1587008540939; __da_ntes_utmb=2525167.1.10.1586926851; x5sec=7b227761676272696467652d616c69626162612d67726f75703b32223a226131343164373731633435383465343137653738366530363066326136333038434a4f7132765146454f623871382b676d614c7a7567453d227d; NTES_KAOLA_ADDRESS_CONTROL=310000|310100|310101|1; NTES_KAOLA_RV=1380027_1586926968862_0|6137022_1586917525489_0|6095594_1586915723435_0|1637695_1586832857734_0|6192366_1586830698135_0|1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0; isg=BJeXvumehXigEgHkTuLnigF_JgshdGo1sDCM6-nA1mf2GLNa8a5Mjid-evij8EO2',
                        'origin': 'https://goods.kaola.com',
                        'pragma': 'no-cache',
                        # 'referer': 'https://goods.kaola.com/review/{}.html'.format(str(goods_id)),
                        'user-agent': '{}'.format(random.choice(self.user_agent_list))
                        # 'x-requested-with': 'XMLHttpRequest'
                    }
                    form_data = {
                        'goodsId': '{}'.format(str(goods_id)),
                        'grade': '0',
                        'tagType': '0',
                        'hasContent': '0',
                        'paginationContext': {"lastId": '{}'.format(lastId), "lastPage": '{}'.format(lastPage)},
                        'pageNo': '{}'.format(int(page_data) + 1),
                        'pageSize': '20',
                        'hasInitCommentTab': 'true'
                    }
                    yield scrapy.FormRequest(
                        dont_filter=True,
                        url='https://goods.kaola.com/commentAjax/comment_list_new.json',
                        headers=headers,
                        formdata=form_data,
                        callback=self.goods_comment2,
                        meta={'meta_1': goods_dict, 'meta_2': goods_id, 'meta_3': int(page_data) + 1}
                    )
        except:
            print(22222222222222222222222222, traceback.format_exc())

    # 抓取第一页之后的评论
    def goods_comment2(self, response):
        try:
            goods_dict = response.meta['meta_1']
            goods_id = response.meta['meta_2']
            i = response.meta['meta_3']

            data = json.loads(response.text)
            # print(data)
            # 获取评论列表
            comments_list = data['data']['commentPage']['result']
            if int(len(comments_list)) == 0:
                return
            else:
                # 获取当前页数
                page_data = data['data']['commentPage']['pageNo']
                # 评价总页数
                pages_num = data['data']['commentPage']['totalPage']
                # print('**********************第{}页评论**********************'.format(i))
                # print('---当前页是: %s ,总页数是: %s ---' % (page_data, pages_num))
                kao_la_goods = dict()
                for item in comments_list:
                    time_data = self.time_change(item['createTime'])
                    data_time_data = time_data.split(' ')[0]
                    try:
                        content = item['commentContent'].replace('\n', ' ')
                    except:
                        content = ''
                    # 追加评论
                    try:
                        comments_2 = item['replyList'][0]['replyContent']
                    except:
                        comments_2 = ''
                    if self.start_time <= data_time_data.strip():
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
                        # items = json.dumps(dict(kao_la_goods), ensure_ascii=False) + '\n'
                        # with open('./{}_kao_la_goods_NIKE.json'.format(time.strftime('%Y_%m_%d')), 'ab') as f:
                        #     f.write(items.encode("utf-8"))
                        item = json.dumps(dict(kao_la_goods), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/ecommerce/{}/176_{}_{}_KaoLa_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

                if int(page_data) < int(pages_num):
                    # 获取第2页评论最后一个的id以及下一页从哪页跳转参数
                    lastId = data['data']['paginationContext']['lastId']
                    lastPage = data['data']['paginationContext']['lastPage']
                    i += 1
                    # self.goods_comments_2(lastId, lastPage, goods_id, goods_dict, i)
                    headers = {
                        # 'authority': 'goods.kaola.com',
                        # 'method': 'POST',
                        # 'path': '/commentAjax/comment_list_new.json',
                        # 'scheme': 'https',
                        # 'accept': '*/*',
                        # 'accept-encoding': 'gzip, deflate, br',
                        # 'accept-language': 'zh-CN,zh;q=0.9',
                        # 'cache-control': 'no-cache',
                        # 'content-length': '247',
                        # 'content-type': 'application/x-www-form-urlencoded',
                        'cookie': 'kaola_user_key=b87e28b9-e7fc-43ba-8ca7-42abae97a079; _ntes_nnid=116c0ca91001bfb53c23f45f9e55ac87,1568617522153; _ga=GA1.2.290138937.1568617522; _klhtxd_=31; __da_ntes_utma=2525167.417648162.1568617522.1568617522.1568617522.1; davisit=1; __da_ntes_utmz=2525167.1568617522.1.1.; _ga=GA1.3.290138937.1568617522; __da_ntes_utmfc=utmcsr%3D(direct)%7Cutmccn%3D(direct)%7Cutmcmd%3D(none); usertrack=CrGZAV2DFzgLhl54AwtSAg==; KAOLA_NEW_USER_COOKIE=yes; cna=MQj5FQMZD0sCAXxONRZeF0y0; WM_TID=beYPJ03r5ilFBUFUFEZo5jCUV1mKk4PC; t=cf5d799c2331f5cabed38ae64e05e79e; _samesite_flag_=true; _tb_token_=57e48eee49e7; csg=7c23ee4b; hb_MA-AE38-1FCC6CD7201B_source=search.kaola.com; JSESSIONID-WKL-8IO=WrbxWqBstbryTpUoUTTO3lD2kpgUaN%5CQXfBrV7eKj3XYscWBXT3D2m9%5CKMYb%5CbCiR1Ed49%5CxGbTX%2BDQ9%2FYzaxdxG9EvNb%5C%2BPCG79%5CzVz%2B1n4bOLWx6KmfLtZjbNl1aCJ8JDDk9mosiJDeP%2Bco069H0U8bv3E38HZUe80r777GgVcfQCk%3A1587008540939; __da_ntes_utmb=2525167.1.10.1586926851; x5sec=7b227761676272696467652d616c69626162612d67726f75703b32223a226131343164373731633435383465343137653738366530363066326136333038434a4f7132765146454f623871382b676d614c7a7567453d227d; NTES_KAOLA_ADDRESS_CONTROL=310000|310100|310101|1; NTES_KAOLA_RV=1380027_1586926968862_0|6137022_1586917525489_0|6095594_1586915723435_0|1637695_1586832857734_0|6192366_1586830698135_0|1537539_1586756945560_0|2884042_1586756792280_0|5522516_1586513810003_0|5705591_1585881322711_0|8317307_1585880658885_0|5553701_1585880652352_0|8517421_1585879009306_0|1467929_1571291229258_0|5218698_1569811431977_0|5536790_1569811422334_0|5457794_1569811411408_0|5115159_1569811404628_0|2843760_1569566707083_0|5481268_1569489750583_0|2723610_1569488978899_0; isg=BJeXvumehXigEgHkTuLnigF_JgshdGo1sDCM6-nA1mf2GLNa8a5Mjid-evij8EO2',
                        'origin': 'https://goods.kaola.com',
                        'pragma': 'no-cache',
                        # 'referer': 'https://goods.kaola.com/review/{}.html'.format(str(goods_id)),
                        'user-agent': '{}'.format(random.choice(self.user_agent_list))
                        # 'x-requested-with': 'XMLHttpRequest'
                    }
                    form_data = {
                        'goodsId': '{}'.format(str(goods_id)),
                        'grade': '0',
                        'tagType': '0',
                        'hasContent': '0',
                        'paginationContext': {"lastId": '{}'.format(lastId), "lastPage": '{}'.format(lastPage)},
                        'pageNo': '{}'.format(i),
                        'pageSize': '20',
                        'hasInitCommentTab': 'true'
                    }
                    yield scrapy.FormRequest(
                        dont_filter=True,
                        url='https://goods.kaola.com/commentAjax/comment_list_new.json',
                        headers=headers,
                        formdata=form_data,
                        callback=self.goods_comment2,
                        meta={'meta_1': goods_dict, 'meta_2': goods_id, 'meta_3': i}
                    )
        except:
            print(333333333333333333333, traceback.format_exc())


