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
import datetime
import traceback
import multiprocessing
import math
import scrapy
# from mei_pai_video.with_hdfs import HdfsClient


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


class ParseVideoSpider(scrapy.Spider):
    name = 'parse_video'
    allowed_domains = ['meipai.com']
    start_urls = []

    headers = {
        # 'Content-Type': 'text/html; charset=utf-8',
        # 'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565584700; searchStr=AJ%7C361%E5%BA%A6%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E8%80%90%E5%85%8B%7C',
        'Host': 'www.meipai.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.meipai.com/search/mv?q=%E8%80%90%E5%85%8B',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
    }

    # 时间判断部分
    date = datetime.datetime.now() - timedelta(days=7)
    news_start_time = str(date).split(' ')[0]
    yesterday_time = datetime.datetime.now() - timedelta(days=1)  # 昨天时间
    yesterday_date = str(yesterday_time).split(' ')[0]
    now_time = datetime.datetime.now() - timedelta(days=0)  # 当前时间
    now_date = str(now_time).split(' ')[0]
    print('爬取时间段：{}到{}'.format('2019-10-01', now_date))
    logging.info('爬取时间段：{}到{}'.format('2019-10-01', now_date))
    # 定义开始时间 y-m-d  离现在时间远  news_start_time
    start_time = '2019-10-01'
    # 定义结束时间 y-m-d  离现在时间近  yesterday
    end_time = now_date
    # 昨日时间
    yesterday_date = yesterday_date
    # 标记爬虫工作
    is_break = False

    # 链接hdfs
    # hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
    # hdfsclient.makedirs('/user/cspider_daily/nike_daily/video/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
    # hdfsclient.makedirs('/user/cspider_daily/nike_daily/videocomments/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
    # time_data = str(time.time()).split('.')[0]

    # 替换所有的HTML标签
    def re_html(self, data):
        # 替换抓取数据中的html标签 .replace('[', '').replace(']', '').replace(',', '').replace("'", '')
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub(r'\[', '', ret1)
            ret3 = re.sub(r'\]', '', ret2)
            ret4 = re.sub(r',', '', ret3)
            ret5 = re.sub(r"'", '', ret4)
            ret6 = re.sub(r'                    ', '', ret5)
            return ret6
        except:
            pass

    # 过滤非数字
    def re_not_number(self, data):
        try:
            message = str(data)
            ret1 = re.sub(r'\D', '', message)
            return ret1
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
            x = datetime.datetime.strftime(now + datetime.timedelta(days=-int(str(x).replace('天前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('小时前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(hours=-int(str(x).replace('小时前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('分钟前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(minutes=-int(str(x).replace('分钟前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('今天') != -1:
            x = str(x).replace('今天', now.strftime('%Y-%m-%d') + ' ')
        elif str(x).find('刚刚') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('秒前') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('月前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-4 * int(str(x).replace('月前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('周前') != -1:
            x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-int(str(x).replace('周前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('[') != -1:
            x = x.replace('[', '').replace(']', '')
        elif str(x).find('月') != -1:
            x = x.replace('月', '-').replace('日', '')
        return x

    def start_requests(self):
        # 设置路径
        path = './快消采集关键词_v12_20200119.xlsx'
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
            key_word = data[0]
            # print(brand)
            for i in range(1, 22):
                yield scrapy.Request(
                    dont_filter=True,
                    url='https://www.meipai.com/search/mv?q={}&page={}&fromAll=1'.format(key_word, i),
                    headers=self.headers,
                    callback=self.parse_page,
                    meta={'meta_1': key_word}
                )

    # 获取视频列表
    def parse_page(self, response):
        try:
            key_word = response.meta['meta_1']

            # 判断是否有视频数据
            test_data = response.xpath('//div[@class="search-result-null break"]/p//text()').extract()
            if test_data != []:
                # print('没有视频数据')
                pass
            else:
                # 获取视频列表信息
                video_data_list = response.xpath('//ul[@id="mediasList"]/li')

                for node in video_data_list:
                    video_dict = dict()
                    video_dict['platform'] = '美拍'
                    video_dict['title'] = node.xpath('./div[1]/a/@title').extract_first()
                    video_dict['keyword'] = key_word
                    video_dict['url'] = 'https://www.meipai.com' + node.xpath('./div[1]/a/@href').extract_first()
                    video_dict['imageurl'] = 'https:' + node.xpath('./img/@src').extract_first()
                    video_dict['audiourl'] = video_dict['url']
                    video_dict['topic_id'] = self.re_not_number(node.xpath('./div[1]/a/@href').extract_first())
                    video_dict['source_author'] = node.xpath('./div[2]/p/a/@title').extract_first()
                    video_dict['上传者url'] = 'https://www.meipai.com' + node.xpath('./div[2]/p/a/@href').extract_first()
                    video_dict['author_id'] = self.re_not_number(node.xpath('./div[2]/p/a/@href').extract_first())
                    video_dict['categroy'] = ''
                    # print(video_dict)

                    # print(video_dict['url'])
                    headers = {
                        # 'Content-Type': 'text/html; charset=utf-8',
                        # 'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565584700; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C',
                        # 'Host': 'www.meipai.com',
                        # 'Pragma': 'no-cache',
                        # 'Referer': 'https://www.meipai.com/search/all?q={}'.format(key_word),
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                    }
                    yield scrapy.Request(
                        url=video_dict['url'],
                        headers=headers,
                        dont_filter=True,
                        callback=self.parse_video_data,
                        meta={'meta_1': video_dict}
                    )
        except:
            print(111111111111111111111, traceback.format_exc())

    # 进入视频页面，抓取数据信息
    def parse_video_data(self, response):
        try:
            video_dict = response.meta['meta_1']
            # print(video_dict)
            # 发布日期时间数据
            date_time_data = response.xpath('//div[@class="detail-time pa"]/strong/text()').extract_first()
            data = self.clean_date(date_time_data)
            # print(data)
            date_data = data.split(' ')[0]
            time_data = data.split(' ')[1]
            if len(date_data.split('-')) == 3:
                if len(date_data.split('-')[0]) == 2:
                    video_dict['date'] = '20' + date_data.strip()
                else:
                    video_dict['date'] = date_data.strip()
            else:
                video_dict['date'] = (time.strftime('%Y') + '-' + date_data).strip()
            if len(time_data.split(':')) == 3:
                video_dict['time'] = time_data.strip()
            else:
                video_dict['time'] = (time_data + ':00').strip()
            # print(video_dict['date'], video_dict['time'])
            if self.start_time <= video_dict['date']:
                # 视频描述
                content = response.xpath('//div[@class="detail-info pr"]/h1//text()').extract()
                # print('99999999999999999999999999999999999999999999999999999')
                # print(content)
                try:
                    video_dict['description'] = self.re_html(content)
                except:
                    video_dict['description'] = ''
                video_dict['content_id'] = video_dict['topic_id']
                video_dict['clicks'] = ''
                # 播放量
                try:
                    play_data = response.xpath('//div[@class="detail-location"]/text()').extract()[1].replace('播放', '').replace('\n', '').replace(' ', '')
                except:
                    play_data = ''
                video_dict['play'] = play_data
                # 评论数
                comment_num = response.xpath('//span[@id="commentCount"]/text()').extract_first()
                if comment_num == '评论':
                    video_dict['comments_count'] = 0
                else:
                    video_dict['comments_count'] = comment_num
                # 点赞数
                likes_data = response.xpath('//span[@itemprop="ratingCount"]/text()').extract_first()
                video_dict['likes'] = likes_data
                video_dict['reposts_count'] = ''
                # print(video_dict)

                url = video_dict['上传者url']
                # print(url)
                headers = {
                    # 'Content-Type': 'text/html; charset=utf-8',
                    # 'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565590100',
                    # 'Host': 'www.meipai.com',
                    # 'Pragma': 'no-cache',
                    # 'Referer': url,
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    dont_filter=True,
                    callback=self.parse_followers_count,
                    meta={'meta_1': video_dict}
                )
        except:
            print(222222222222222222, traceback.format_exc())

    # 抓取作者粉丝数
    def parse_followers_count(self, response):
        try:
            video_dict = response.meta['meta_1']

            # 粉丝数
            followers_count = response.xpath('//div[@class="user-num"]/a[4]/span[1]//text()').extract()
            video_dict['followers_count'] = self.re_html(followers_count).replace(' ', '')
            video_dict['file_code'] = '165'
            video_data = video_dict.pop('上传者url')
            print('--------------------------------开始录入视频主贴数据------------------------------------')
            print(video_dict)
            items = json.dumps(dict(video_dict), ensure_ascii=False) + '\n'
            with open('./165_{}_MeiPai.json'.format(time.strftime('%Y%m%d')), 'ab') as f:
                f.write(items.encode("utf-8"))
            # item = json.dumps(dict(video_dict), ensure_ascii=False) + '\n'
            # self.hdfsclient.new_write('/user/cspider_daily/nike_daily/video/{}/165_{}_{}_MeiPai_Nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

            if int(video_dict['comments_count']) == 0:
                # print('此主贴没有视频评论回复。。。。。。')
                pass
            else:
                pages = int(math.ceil(float(int(video_dict['comments_count']) / 10)))
                # print('~~~~~~~~~~~~~~~~视频回帖数：%s , 回帖总页数: %s ~~~~~~~~~~~~~' % (video_dict['comments_count'], pages))
                headers = {
                    # 'Content-Type': 'application/json; charset=utf-8',
                    # 'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565590100',
                    # 'Host': 'www.meipai.com',
                    'Pragma': 'no-cache',
                    # 'Referer': video_dict['url'],
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest'
                }
                for i in range(1, int(pages) + 1):
                    url = 'https://www.meipai.com/medias/comments_timeline?page={}&count=10&id={}'.format(i, video_dict['topic_id'])
                    yield scrapy.Request(
                        url=url,
                        headers=headers,
                        dont_filter=True,
                        callback=self.parse_comment_data,
                        meta={'meta_1': video_dict, 'meta_2': url}
                    )
        except:
            print(33333333333333333333, traceback.format_exc())

    # 抓取视频回复数据
    def parse_comment_data(self, response):
        try:
            video_dict = response.meta['meta_1']
            url = response.meta['meta_2']

            comments_data = json.loads(response.text)
            for item in comments_data:
                video_comment = dict()
                date_time_data = item['created_at']

                # print(date_time_data, self.clean_date(date_time_data))

                date_data = self.clean_date(date_time_data).split(' ')[0]
                time_data = self.clean_date(date_time_data).split(' ')[1]
                if len(date_data.split('-')) == 3:
                    date_data_test = date_data.strip()
                else:
                    date_data_test = (time.strftime('%Y') + '-' + date_data).strip()

                if len(time_data.split(':')) == 3:
                    time_data_test = time_data.strip()
                else:
                    time_data_test = (time_data + ':00').strip()

                # print(date_data_test, time_data_test)
                # if self.start_time <= date_data_test:
                video_comment['platform'] = video_dict['platform']
                video_comment['date'] = date_data_test
                video_comment['time'] = time_data_test
                video_comment['title'] = video_dict['title']
                video_comment['author'] = item['user']['screen_name']
                video_comment['author_id'] = item['user']['id']
                video_comment['content'] = item['content_origin']
                video_comment['content_id'] = item['id']
                video_comment['floor'] = ''
                video_comment['keyword'] = video_dict['keyword']
                video_comment['comment_url'] = url
                video_comment['source_url'] = video_dict['url']
                video_comment['comments_count'] = ''
                video_comment['likes'] = ''
                video_comment['views'] = ''
                video_comment['reposts_count'] = ''
                video_comment['topic_id'] = video_dict['topic_id']
                video_comment['imageurl'] = ''
                video_comment['audiourl'] = ''
                video_comment['file_code'] = '166'
                print('--------------------------------开始录入视频回贴内容------------------------------------')
                print(video_comment)
                items = json.dumps(dict(video_dict), ensure_ascii=False) + '\n'
                with open('./166_{}_MeiPai_comments.json'.format(time.strftime('%Y%m%d')), 'ab') as f:
                    f.write(items.encode("utf-8"))
                # item = json.dumps(dict(video_comment), ensure_ascii=False) + '\n'
                # self.hdfsclient.new_write('/user/cspider_daily/nike_daily/videocomments/{}/166_{}_{}_MeiPai_nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                # else:
                #     break
        except:
            print(444444444444444444444, traceback.format_exc())