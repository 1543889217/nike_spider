# -*- coding:UTF-8 -*-
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
import multiprocessing
import math
from with_hdfs import HdfsClient

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
        self.is_break = False
        self.h2_name = hours_name
        self.date_time = wen_jian_jia_date
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_2h/video/{}/{}'.format(wen_jian_jia_date, hours_name))  # 创建每日文件夹
        self.hdfsclient.makedirs('/user/cspider_daily/nike_2h/videocomments/{}/{}'.format(wen_jian_jia_date, hours_name))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

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
        now = datetime.now()
        if str(x).find('昨天') != -1:
            x = datetime.strftime(now + timedelta(days=-1), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('前天') != -1:
            x = datetime.strftime(now + timedelta(days=-2), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('天前') != -1:
            x = datetime.strftime(now + timedelta(days=-int(str(x).replace('天前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('小时前') != -1:
            x = datetime.strftime(now + timedelta(hours=-int(str(x).replace('小时前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('分钟前') != -1:
            x = datetime.strftime(now + timedelta(minutes=-int(str(x).replace('分钟前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('今天') != -1:
            x = str(x).replace('今天', now.strftime('%Y-%m-%d') + ' ')
        elif str(x).find('刚刚') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('秒前') != -1:
            x = now.strftime('%Y-%m-%d %H:%M:%S')
        elif str(x).find('月前') != -1:
            x = datetime.strftime(now + timedelta(weeks=-4 * int(str(x).replace('月前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('周前') != -1:
            x = datetime.strftime(now + timedelta(weeks=-int(str(x).replace('周前', ''))), '%Y-%m-%d %H:%M:%S')
        elif str(x).find('[') != -1:
            x = x.replace('[', '').replace(']', '')
        elif str(x).find('月') != -1:
            x = x.replace('月', '-').replace('日', '')
        return x

    # 根据关键词搜索请求得到商品第一页信息
    def parse_page1(self, key_word):
        try:
            # 根据关键词,例如：洗发水,抓取商品信息
            url = 'https://www.meipai.com/search/all?q={}'.format(key_word)
            # print(url)
            headers = {
                'Content-Type': 'text/html; charset=utf-8',
                'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565584700; searchStr=AJ%7C361%E5%BA%A6%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E8%80%90%E5%85%8B%7C',
                'Host': 'www.meipai.com',
                'Pragma': 'no-cache',
                # 'Referer': '{}'.format(url),
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                # time.sleep(0.1)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
            # print(response.text)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 获取视频列表信息
            video_data_list = html.xpath('//ul[@id="mediasList"]/li')
            video_dict = dict()
            # logger.log(31, '***********关键词%s的*****第1页数据***********' % (key_word))
            for node in video_data_list:
                video_dict['platform'] = '美拍'
                video_dict['title'] = node.xpath('./img/@alt')[0].replace('\n', ' ')
                video_dict['keyword'] = key_word
                video_dict['url'] = 'https://www.meipai.com' + node.xpath('./div[1]/a/@href')[0]
                video_dict['imageurl'] = 'https:' + node.xpath('./img/@src')[0]
                video_dict['audiourl'] = video_dict['url']
                video_dict['topic_id'] = str(self.re_not_number(node.xpath('./div[1]/a/@href')[0]))
                video_dict['source_author'] = node.xpath('./div[2]/p/a/@title')[0]
                video_dict['上传者url'] = 'https://www.meipai.com' + node.xpath('./div[2]/p/a/@href')[0]
                video_dict['author_id'] = str(self.re_not_number(node.xpath('./div[2]/p/a/@href')[0]))
                video_dict['categroy'] = ''
                # print(video_dict)
                response.close()
                self.parse_video_data(video_dict, key_word)
        except:
            print(111111111111111111111111, traceback.format_exc())

    # 根据关键词搜索请求得到商品后1页信息
    def parse_page2(self, key_word):
        try:
            for i in range(2, 3):
                # 根据关键词,例如：洗发水,抓取商品信息
                url = 'https://www.meipai.com/search/mv?q={}&page={}&fromAll=1'.format(key_word, i)
                # logger.log(31, '***********关键词%s的******第%s页数据*********** +  %s' % (key_word, i, url))
                # print(url)
                headers = {
                    'Content-Type': 'text/html; charset=utf-8',
                    'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565584700; searchStr=AJ%7C361%E5%BA%A6%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E8%80%90%E5%85%8B%7C',
                    'Host': 'www.meipai.com',
                    'Pragma': 'no-cache',
                    # 'Referer': url,
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        # time.sleep(0.1)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        # time.sleep(0.1)
                        response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)
                # 判断是否有视频数据
                test_data = html.xpath('//div[@class="search-result-null break"]/p//text()')
                if test_data != []:
                    break
                else:
                    # 获取视频列表信息
                    video_data_list = html.xpath('//ul[@id="mediasList"]/li')
                    video_dict = dict()

                    for node in video_data_list:
                        video_dict['platform'] = '美拍'
                        video_dict['title'] = node.xpath('./img/@alt')[0].replace('\n', ' ').replace('\r', ' ').replace('\u200b', '').replace('\u200e', '').replace('\u200c', '')
                        video_dict['keyword'] = key_word
                        video_dict['url'] = 'https://www.meipai.com' + node.xpath('./div[1]/a/@href')[0]
                        video_dict['imageurl'] = 'https:' + node.xpath('./img/@src')[0]
                        video_dict['audiourl'] = video_dict['url']
                        video_dict['topic_id'] = str(self.re_not_number(node.xpath('./div[1]/a/@href')[0]))
                        video_dict['source_author'] = node.xpath('./div[2]/p/a/@title')[0]
                        video_dict['上传者url'] = 'https://www.meipai.com' + node.xpath('./div[2]/p/a/@href')[0]
                        video_dict['author_id'] = str(self.re_not_number(node.xpath('./div[2]/p/a/@href')[0]))
                        video_dict['categroy'] = ''
                        # print(video_dict)
                        response.close()
                        self.parse_video_data(video_dict, key_word)
        except:
            print(222222222222222222222222, traceback.format_exc())

    # 进入视频页面，抓取数据信息
    def parse_video_data(self, video_dict, key_word):
        try:
            url = video_dict['url']
            headers = {
                'Content-Type': 'text/html; charset=utf-8',
                'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565584700; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C',
                'Host': 'www.meipai.com',
                'Pragma': 'no-cache',
                # 'Referer': 'https://www.meipai.com/search/all?q={}'.format(key_word),
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            # logger.log(31,  '视频链接是: %s' % url)
            try:
                # time.sleep(0.1)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 发布日期时间数据
            date_time_data = html.xpath('//div[@class="detail-time pa"]/strong/text()')[0]
            data = self.clean_date(date_time_data)
            # print(data)
            date_data = data.split(' ')[0]
            time_data = data.split(' ')[1]
            if len(date_data.split('-')) == 3:
                video_dict['date'] = date_data.strip()
            else:
                video_dict['date'] = (time.strftime('%Y') + '-' + date_data).strip()

            if len(time_data.split(':')) == 3:
                video_dict['time'] = time_data.strip()
            else:
                video_dict['time'] = (time_data + ':00').strip()
            test_date_time = video_dict['date'] + ' ' + video_dict['time']
            # print(test_date_time)
            if self.start_time <= test_date_time <= self.end_time:
                # 视频描述
                content = html.xpath('//div[@class="detail-info pr"]/h1//text()')
                # print('99999999999999999999999999999999999999999999999999999')
                # print(content)
                try:
                    video_dict['description'] = self.re_html(content).replace('\\u200d', '').replace('\\n', '')
                except:
                    video_dict['description'] = ''
                video_dict['content_id'] = video_dict['topic_id']
                # print(video_dict['description'])
                video_dict['clicks'] = ''
                # 播放量
                try:
                    play_data = html.xpath('//div[@class="detail-location"]/text()')[1].replace('播放', '').replace('\n', '').replace(' ', '')
                except:
                    play_data = ''
                video_dict['play'] = play_data
                # 评论数
                comment_num = html.xpath('//span[@id="commentCount"]/text()')[0]
                if comment_num == '评论':
                    video_dict['comments_count'] = 0
                else:
                    video_dict['comments_count'] = comment_num
                # 点赞数
                likes_data = html.xpath('//span[@itemprop="ratingCount"]/text()')[0]
                video_dict['likes'] = likes_data
                video_dict['reposts_count'] = ''
                # print(video_dict)
                response.close()
                self.parse_followers_count(video_dict)
            else:
                pass
        except:
            print(333333333333333333333333333, traceback.format_exc())

    # 抓取作者粉丝数
    def parse_followers_count(self, video_dict):
        try:
            url = video_dict['上传者url']
            # print(url)
            headers = {
                'Content-Type': 'text/html; charset=utf-8',
                'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565590100',
                'Host': 'www.meipai.com',
                'Pragma': 'no-cache',
                # 'Referer': url,
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                # time.sleep(0.1)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 粉丝数
            followers_count = html.xpath('//div[@class="user-num"]/a[4]/span[1]//text()')
            video_dict['followers_count'] = self.re_html(followers_count).replace(' ', '')
            video_dict['file_code'] = '165'
            video_data = video_dict.pop('上传者url')
            # logger.log(31, '--------------------------------开始录入视频主贴数据------------------------------------')
            # print(video_dict)
            response.close()
            # self.write_topic_jsonfile(video_dict)
            item = json.dumps(dict(video_dict), ensure_ascii=False) + '\n'
            self.hdfsclient.new_write('/user/cspider_daily/nike_2h/video/{}/{}/165_{}_{}_MeiPai_Nike.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

            if int(video_dict['comments_count']) == 0:
                logger.log(31, '此主贴没有视频评论回复。。。。。。')
            else:
                pages = int(math.ceil(float(int(video_dict['comments_count']) / 10)))
                # logger.log(31, '~~~~~~~~~~~~~~~~视频回帖数：%s , 回帖总页数: %s ~~~~~~~~~~~~~' % (video_dict['comments_count'], pages))
                self.parse_comment_data(video_dict, pages)
        except:
            print(4444444444444444444, traceback.format_exc())

    # 抓取视频回复数据
    def parse_comment_data(self, video_dict, pages):
        try:
            is_break = self.is_break
            headers = {
                'Content-Type': 'application/json; charset=utf-8',
                'Cookie': 'MUSID=ta4877m5ongth47s2n7kt0km13; virtual_device_id=d8afe1d9634ad1f6591e3486d4312976; pvid=imZ0YWzzY7TAFPWAQnp71Vl1bDpOXY91; sid=ta4877m5ongth47s2n7kt0km13; UM_distinctid=16c84237ad71af-096aa6932eb12f-37c143e-1fa400-16c84237ad870f; searchStr=AJ%7C%E9%98%BF%E8%BF%AA%E8%BE%BE%E6%96%AF%7C%E6%9D%8E%E5%AE%81%7C%E8%80%90%E5%85%8B%7C361%E5%BA%A6%7C; CNZZDATA1256786412=2077818170-1565584700-https%253A%252F%252Fwww.baidu.com%252F%7C1565590100',
                'Host': 'www.meipai.com',
                'Pragma': 'no-cache',
                # 'Referer': video_dict['url'],
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }
            for i in range(1, int(pages)+1):
                url = 'https://www.meipai.com/medias/comments_timeline?page={}&count=10&id={}'.format(i, video_dict['topic_id'])
                # print(url)
                try:
                    # time.sleep(0.1)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        # time.sleep(0.1)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        # time.sleep(0.1)
                        response = requests.get(url=url, headers=headers, allow_redirects=False, timeout=30)
                comments_data = json.loads(response.text)
                video_comment = dict()

                for item in comments_data:
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

                    date_time_test = date_data_test + ' ' + time_data_test
                    if self.start_time <= date_time_test <= self.end_time:
                        video_comment['platform'] = video_dict['platform']
                        video_comment['source_date'] = video_dict['date']
                        video_comment['source_time'] = video_dict['time']
                        video_comment['date'] = date_data_test
                        video_comment['time'] = time_data_test
                        video_comment['title'] = video_dict['title']
                        video_comment['author'] = item['user']['screen_name']
                        video_comment['author_id'] = str(item['user']['id'])
                        video_comment['content'] = item['content_origin']
                        video_comment['content_id'] = str(item['id'])
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
                        # logger.log(31, '--------------------------------开始录入视频回贴内容------------------------------------')
                        # print(video_comment)
                        # self.write_comment_jsonfile(video_comment)
                        response.close()
                        item = json.dumps(dict(video_comment), ensure_ascii=False) + '\n'
                        self.hdfsclient.new_write('/user/cspider_daily/nike_2h/videocomments/{}/{}/166_{}_{}_MeiPai_nike.json'.format(self.date_time, self.h2_name, time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                    if date_data_test < self.start_time:
                        is_break = True
                if is_break:
                    break

        except:
            print(5555555555555555555555, traceback.format_exc())

    # 读取excel获取关键词
    def parse_xlsx(self):
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
        for item_data1 in key_word_list:
            print(item_data1['关键词'])
            self.parse_page1(item_data1['关键词'])
            self.parse_page2(item_data1['关键词'])


def Meipai_run():
    spider = Spider()
    try:
        spider.run()
    except:
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    pid = os.getpid()
    pool = multiprocessing.Pool(processes=1)
    for i in range(1):
        pool.apply_async(Meipai_run, args=())
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