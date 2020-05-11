import requests
from lxml import etree
import json
import re
import math
import time
import ast
import random
# from as_cp import get_as_cp
from xml.sax.saxutils import unescape
# from ippro.proxies import res_ip
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import sys
from with_hdfs import HdfsClient

# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
file_name = "./../toutiao/toutiao-{}_dayli.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')
import os


class TouTiaoSpider(object):
    """
    今日头条的爬虫，主要采集和汽车有关的新闻
    """
    def __init__(self):

        # 'cookie':'uuid="w:d0214807f672416fb7d3ee0431aa13a3"; UM_distinctid=1674ef3a9800-0bce565d4c8dc4-414f0120-15f900-1674ef3a981290; _ga=GA1.2.823209007.1543222670; _gid=GA1.2.547615301.1543222670; CNZZDATA1259612802=603836554-1543213069-%7C1543218469; __tasessionId=tpisw88851543281460530; csrftoken=d9a6dad7de6c1fbbf3ddd1a3de811481; tt_webid=6628070185327625741',
        # ':authority':'www.toutiao.com',
        # ':method':'GET',
        # ':path':'/api/pc/feed/?category=news_car&utm_source=toutiao&widen=1&max_behot_time=0&max_behot_time_tmp=0&tadrequire=true&as=A1E56B7F8CD9B35&cp=5BFC39BB43B5DE1&_signature=pMmtcAAA.0TvpJ9rFvhWIKTJrW',
        # ':scheme':'https',
        # 'cache-control': 'max-age=0',
        # 'cookie': 'tt_webid=6628733243796178436; tt_webid=6628733243796178436; csrftoken=3a6f2dc0f315bd1fe957319a75bba4ed; uuid="w:2203d39caf3249c0bcda19ee5839b850"; UM_distinctid=1675827673a27a-0dd556679b3f63-3a3a5d0c-15f900-1675827673b22c; __tasessionId=qb2c0x9mb1543386267822; CNZZDATA1259612802=992935523-1543369669-%7C1543385869',
        # 'referer': 'https://www.toutiao.com/ch/news_car/',
        self.headers_one = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'max-age=0',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
        }

        self.start_url = 'https://www.toutiao.com/api/pc/feed/'
        # 评论接口模板
        self.commnet_port_url = ''

        date = datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        # 定义开始时间 y-m-d  离现在时间远
        self.start_time = news_start_time
        # self.start_time = '2019-09-09'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = yesterday
        # self.end_time = '2019-09-16'
        print('爬取时间段：{}到{}'.format(self.start_time, self.end_time))

        # 标记爬虫工作
        self.is_work = True
        # 评论页数
        self.comment_page_num = 1
        # 去重列表
        self.set_list = []
        # 代理ip
        self.proxies = [
            '112.245.235.249:4243',
            # '59.53.47.4:4249'
        ]
        # 搜集问答类网页的列表
        self.questions_list = []

        # 读取url列表
        # with open('./../toutiao/new_url_file.json', 'r') as f:
        #     self.url_list = f.readlines()
        self.file_name_time = self.get_file_name_time()
        try:
            os.mkdir('./../toutiao/json_file/{}'.format(self.file_name_time.split(' ')[0]))
        except:
            pass
        self.file_path = '/user/cspider_daily/nike_daily/article'
        self.comment_apth = '/user/cspider_daily/nike_daily/articlecomments'
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        hour = str(datetime.now()).split(' ')[-1].split(':')[0]
        if str(hour) != '00':
            two_hour_ago = int(hour) - 2
            if len(str(two_hour_ago)) == 1:
                two_hour_ago = '0' + str(two_hour_ago)
            self.hour_name = str(two_hour_ago) + '_' + str(hour)
        else:
            self.hour_name = '22_24'
        self.hdfsclient.makedirs('{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]
        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        # proxyUser = "HQ60F7PAQBO68GWD"
        # proxyUser = "H7307T4706B25G4D"
        proxyUser = "HEW657EL99F83S9D"
        # proxyPass = "FBD9D819229DBB1B"
        # proxyPass = "05B4877CC39192C0"
        proxyPass = "8916B1F3F10B1979"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            # "http": proxyMeta,
            "https": proxyMeta
        }

    def get_news_page(self, url):
        user_agent = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        ]
        headers_one = {
            'accept': 'textml,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cookie': '__tasessionId=0k1ayrc511577344635809',
            'sec-fetch-user': '?1',
            'cache-control': 'max-age=0',
            'upgrade-insecure-requests': '1',
            'user-agent': '{}'.format(random.choice(user_agent))
        }

        item = {}
        response = requests.get(url, headers=headers_one, proxies=self.proxies, timeout=60)  #, proxies={'https': ip}
        stutus_code = response.status_code
        if str(stutus_code) == '200':
            data_all = response.content.decode()
            try:
                data = re.search(r"articleInfo: {([\s\S]*time: '\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", data_all).group(1)
                data = '{' + data + "'}}"
                data = re.sub('\n', '', data)
                data = unescape(data)
                data = data.replace('&quot;', '"').replace('&#x3D;', '=')
                content = re.search('content: ([\s\S]*)groupId', data).group(1).strip()[1:][:-2]
                content = etree.HTML(content)
                text = content.xpath('.//p//text()')
                text_con = ''.join(text)
                # print(text_con)
                text_con = re.sub(r'class=.*?class', '', text_con)
                # print(text_con)
                text_con = re.sub(r'\\u003C.*?\\u003E', '', text_con).replace('.slice(6, -6', '')
                # print(text_con)
                date, create_time = re.search('(\d{4}-\d{1,2}-\d{1,2}) (\d{1,2}:\d{1,2}:\d{1,2})', data).group(1, 2)
                id_num = re.search("groupId: '(\d{1,50}).*itemId", data).group(1)  # 新闻的标识id
                source = re.search("source: '(.*)time", data).group(1).strip()[:-2]  # 来源
                comment_count = re.search("commentCount: '(\d{0,10})[\s\S]*ban_comment", data_all).group(1)
                title = re.search("title: '([\s\S])*content", data).group(0).split("'")[1]
                item['platform'] = '今日头条'
                item['date'] = date
                item['time'] = create_time
                item['title'] = title.replace('"', '')
                item['article_source'] = ''  # 文章来源
                item['article_author'] = source  # 文章作者
                item['content'] = text_con
                item['comments_count'] = comment_count
                item['clicks'] = ''
                item['views'] = ''
                item['likes'] = ''
                item['keyword'] = ''
                item['article_url'] = url  # 文章详情URL
                item['dislikes'] = ''  # 踩人数
                item['series_url'] = ''  # 车系首页
                item['list_url'] = 'https://www.toutiao.com/ch/news_car/'  # 文章列表URL
                item['article_type_1st'] = ''  # 文章类型
                item['article_type_2nd'] = ''  # 文章类型
                item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                content_id = url.split('/')[-1]
                item['content_id'] = str(content_id)  # 文章id
                item['topic_id'] = str(content_id)  # 主贴id
                item['author_id'] = ''  # 作者id
                item['file_code'] = '24'
                item['reposts_count'] = ''
                print(item)
                self.write_news_jsonfile(item)
                self.get_comment_info(url, title, date, create_time)

            except AttributeError:
                print('问答类网页', url)
                self.questions_list.append(url)
                print(self.questions_list)

        else:
            print('网页404错误', url)

    # 获取评论
    # http://lf.snssdk.com/article/v1/tab_comments/?count=50&item_id=6629460454148145678&group_id=6629460454148145678&offset=0
    def get_comment_info(self, source_url, source_title, source_date, source_time, page_id="0"):
        user_agent = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        ]
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            # 'Connection': 'keep-alive',
            'Host': 'lf.snssdk.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': '{}'.format(random.choice(user_agent))
        }
        headers_two = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            # 'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Cookie': 'csrftoken=4be00616a67933bdef5696b162e70937; tt_webid=6762029080495375880',
            # 'Connection': 'keep-alive',
            # 'Host': 'lf.snssdk.com',
            # 'Upgrade-Insecure-Requests': '1',
            'Connection': 'keep-alive',
            'User-Agent': 'PostmanRuntime/7.20.1'
        }

        url_id = source_url.split('/')[-1][1:]
        news_comment_url = 'https://www.toutiao.com/article/v2/tab_comments/?aid=24&app_name=toutiao-web&group_id={}&item_id={}&offset={}&count=50'.format(url_id, url_id, page_id)
        comment_url = 'http://lf.snssdk.com/article/v1/tab_comments/?count=50&item_id={}&group_id={}&offset={}'.format(url_id, url_id, page_id)
        print('评论爬取中......')
        print(comment_url)
        # ip = random.choice(self.proxies)
        try:
            response = requests.get(news_comment_url, headers=headers_two, proxies=self.proxies)  # , proxies={'https': ip}
            datas = json.loads(response.content)
            print(datas)
            data_list = datas['data']
            if data_list:
                total_item = ''
                for comment in data_list:
                    print(1111111111111, comment)
                    item = dict()
                    item['platform'] = '今日头条'
                    item['source_date'] = source_date
                    item['source_time'] = source_time

                    content = comment['comment']['text']
                    date_all = comment['comment']['create_time']
                    # #转换成localtime
                    time_local = time.localtime(float(str(date_all)))
                    # 转换成新的时间格式(2016-05-05 20:28:54)
                    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                    date = dt.split(' ')[0]
                    comment_time = dt.split(' ')[1]
                    item['date'] = date
                    item['time'] = comment_time
                    item['title'] = source_title
                    author = comment['comment']['user_name']
                    item['author'] = author
                    item['content'] = content
                    item['source_url'] = source_url
                    item['comment_url'] = source_url
                    item['floor'] = ''
                    item['views'] = ''
                    item['comments_count'] = comment['comment']['reply_count']
                    item['keyword'] = ''
                    item['likes'] = comment['comment']['digg_count']
                    item['author_id'] = comment['comment']['user_id']  # 用户id
                    item['dislikes'] = ''  # 踩人数
                    item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                    item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                    item['content_id'] = comment['comment']['id']
                    content_id = source_url.split('/')[-1]
                    item['topic_id'] = str(content_id)  # 主贴id
                    item['file_code'] = '38'
                    item['reposts_count'] = ''

                    item = json.dumps(dict(item), ensure_ascii=False) + '\n'

                    total_item += item
                print('写入评论中......')
                self.write_comment_jsonfile(total_item)
                if len(data_list) == 50:
                    page_id = int(page_id) + 50
                    print('爬取评论翻页信息.....')
                    time.sleep(2)
                    self.get_comment_info(source_url, source_title, source_date, source_time, page_id=str(page_id))
        except requests.exceptions.ConnectionError:
            print('获取评论时发生链接错误,程序暂停100s后爬取')
            time.sleep(100)
            self.get_comment_info(source_url, source_title, source_date, source_time, page_id=str(page_id))
            logging.error('获取评论时发生链接错误,程序暂停100s后爬取，get_comment error:{}'.format(traceback.format_exc()))


    # 写入json文件
    def write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.hdfsclient.new_write('{}/{}/24_{}_{}_toutiao_news.json'.format(self.file_path, '20200408', str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def write_comment_jsonfile(self, item):
        # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.hdfsclient.new_write('{}/{}/38_{}_{}_toutiao_comment.json'.format(self.comment_apth, '20200408', str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')


    def get_file_name_time(self):
        a = str(datetime.now())
        hour = a.split(' ')[-1].split(':')[0]
        num = int(hour) / 3
        num = int(num) * 3
        if num == 0:
            num = 24
            a = str(datetime.now() - timedelta(days=1))  # 昨天时间
        num = a.split(' ')[0] + ' ' + str(num)
        return num

    def run(self):
        set_list = []
        logger.info('开始读取url文件,进行新闻爬取')
        # for url in open('./../toutiao/json_file/{}/{}_comment_url.json'.format(self.file_name_time.split(' ')[0], self.file_name_time.split(' ')[0])):
        for url in open('./../toutiao/json_file/{}/{}_comment_url.json'.format('2020-04-07', '2020-04-07')):
            if url in set_list:
                continue
            else:
                set_list.append(url)
            if url:
                logger.info('打开new_url_file.json文件，读取要爬取的url')
                url = url.strip()
                print('一个爬虫正在爬取网址{}'.format(url))
                logger.info('一个爬虫正在爬取网址{}'.format(url))
                try:
                    self.get_news_page(url)
                except:
                    print(traceback.format_exc())
                    try:
                        self.get_news_page(url)
                    except:
                        print('错误')
            print('一个网址爬虫结束.....')
        logger.info('爬取完毕......')


if __name__ == "__main__":
    toutiao = TouTiaoSpider()
    toutiao.run()
