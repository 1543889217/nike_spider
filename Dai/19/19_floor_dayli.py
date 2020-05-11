import os
import requests
from lxml import etree
import json
import re
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
import xlrd
import sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
from config_para import get_config_para



# # 获取文件名称
# name = os.path.basename(__file__)
# name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)


class Spider(object):
    """
    这是一个爬虫模板
    """
    def __init__(self, file_path):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        # 时间判断部分
        date = datetime.now() - timedelta(days=7)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # self.start_time = '2019-09-09'
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # self.end_time = '2019-09-16'
        logger.info('爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        # logging.info('爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        # 定义评论的抓取时间范围
        self.comment_start_time = yesterday  # 一天回复
        # self.comment_start_time = '2019-09-09'  # 一天回复
        # self.comment_start_time = ''  # 不限定时间回复
        self.comment_end_time = yesterday
        # self.comment_end_time = '2019-09-16'
        # 标记爬虫工作
        self.is_work = True
        self.set_list = []  #去重列表
        self.file_path = file_path
        self.hdfsclient = HdfsClient(url='http://jq-chance-05:9870', user='dpp-executor')
        self.hdfsclient.makedirs('{}/{}'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]
        self.source_date = ''
        self.source_time = ''

    def get_search_page(self, url, keyword):
        """
        搜索列表页
        :param url:
        :return:
        """
        heasers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'f9big=u62; _Z3nY0d4C_=37XgPK9h; JSESSIONID=4AB05FA49E2A1477353FD49E96A7DC94; sajssdk_2015_cross_new_user=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216cadbc7b78349-00406e52f50a5c-7373e61-2304000-16cadbc7b797b8%22%2C%22%24device_id%22%3A%2216cadbc7b78349-00406e52f50a5c-7373e61-2304000-16cadbc7b797b8%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _DM_SID_=f9eb430f8631d9542bb6023a0185fd5e; _DM_S_=2446eea1926edb3b390dd3a02a7bcfb2; f19big=ip48; _dm_userinfo=%7B%22uid%22%3A0%2C%22stage%22%3A%22%22%2C%22city%22%3A%22%E4%B8%8A%E6%B5%B7%3A%E4%B8%8A%E6%B5%B7%22%2C%22ip%22%3A%22124.78.53.22%22%2C%22sex%22%3A%22%22%2C%22frontdomain%22%3A%22www.19lou.com%22%2C%22category%22%3A%22%22%7D; pm_count=%7B%7D; dayCount=%5B%5D; Hm_lvt_5185a335802fb72073721d2bb161cd94=1566282908; screen=682; _dm_tagnames=%5B%7B%22k%22%3A%22%E8%80%90%E5%85%8B%22%2C%22c%22%3A1%7D%2C%7B%22k%22%3A%22baoma%22%2C%22c%22%3A3%7D%2C%7B%22k%22%3A%22baoma%22%2C%22c%22%3A2%7D%5D; Hm_lpvt_5185a335802fb72073721d2bb161cd94=1566283069',
            'Host': 'www.19lou.com',
            'Referer': 'https://www.19lou.com/search/thread?keyword=%E8%80%90%E5%85%8B',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        response = requests.get(url, headers=heasers)
        # print(response.text)
        data = etree.HTML(response.content.decode('gb2312', 'ignore'))
        li_list = data.xpath('.//ul[@class="detailtz clearall"]/li[@class="clearall"]')
        for li in li_list:
            print(11111)
            title = ''.join(li.xpath('.//p[@class="title"]//text()')).strip()
            news_date = li.xpath('.//span[@class="postdate"]/text()')[0].strip()
            news_url = 'https:' + li.xpath('.//p[@class="title"]/a/@href')[0].strip()
            views = li.xpath('.//span[@class="fr"]/span/text()')[0].split('(')[-1].split(')')[0]
            replay_no = li.xpath('.//span[@class="fr"]/a/text()')[0].split('(')[-1].split(')')[0]
            # print(title, news_date, news_url, views, replay_no)
            time.sleep(3)
            # # 做时间判断部分---------------
            # get_news_time = time.mktime(time.strptime(news_date, "%Y-%m-%d"))
            # end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            # if self.start_time != '':
            #     start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            # else:
            #     start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            # if float(get_news_time) < float(start_time):
            #     self.is_work = False
            #
            # if float(start_time) <= float(get_news_time) < float(end_time):  # 符合时间段的内容
            print(news_date)
            if '1天' in news_date or '刚刚' in news_date:
                print(222222, news_url)
                if news_url not in self.set_list:   # 去重判断
                    self.get_news_page(news_url, title, views, replay_no, keyword)
                    self.set_list.append(news_url)

        if data.xpath('.//a[@class="page-next"]/@href') and self.is_work:
            next_page_url = 'https:' + data.xpath('.//a[@class="page-next"]/@href')[0].strip()
            self.get_search_page(next_page_url, keyword)

    def get_news_page(self, url, title, views, replay_no, keyword, is_first=True):
        """
        帖子详情页
        :param url:
        :return:
        """
        heasers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'f9big=u62; _Z3nY0d4C_=37XgPK9h; JSESSIONID=4AB05FA49E2A1477353FD49E96A7DC94; sajssdk_2015_cross_new_user=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216cadbc7b78349-00406e52f50a5c-7373e61-2304000-16cadbc7b797b8%22%2C%22%24device_id%22%3A%2216cadbc7b78349-00406e52f50a5c-7373e61-2304000-16cadbc7b797b8%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _DM_SID_=f9eb430f8631d9542bb6023a0185fd5e; _DM_S_=2446eea1926edb3b390dd3a02a7bcfb2; f19big=ip48; _dm_userinfo=%7B%22uid%22%3A0%2C%22stage%22%3A%22%22%2C%22city%22%3A%22%E4%B8%8A%E6%B5%B7%3A%E4%B8%8A%E6%B5%B7%22%2C%22ip%22%3A%22124.78.53.22%22%2C%22sex%22%3A%22%22%2C%22frontdomain%22%3A%22www.19lou.com%22%2C%22category%22%3A%22%22%7D; pm_count=%7B%7D; dayCount=%5B%5D; Hm_lvt_5185a335802fb72073721d2bb161cd94=1566282908; screen=682; _dm_tagnames=%5B%7B%22k%22%3A%22%E8%80%90%E5%85%8B%22%2C%22c%22%3A1%7D%2C%7B%22k%22%3A%22baoma%22%2C%22c%22%3A3%7D%2C%7B%22k%22%3A%22baoma%22%2C%22c%22%3A2%7D%5D; Hm_lpvt_5185a335802fb72073721d2bb161cd94=1566283069',
            'Host': 'www.19lou.com',
            'Referer': 'https://www.19lou.com/search/thread?keyword=%E8%80%90%E5%85%8B',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        logger.info(url)
        response = requests.get(url, headers=heasers, timeout=120)
        # print(response.text)
        data = etree.HTML(response.content.decode('gb2312', 'ignore'))
        if data.xpath('.//div[@class="user-info thread-side"]'):  # 文章类帖子

            if is_first:  # 文章类帖子，首页要抓取文章内容
                content_list = data.xpath('.//div[@class="post-cont"]//text()')
                content = ''.join(content_list).strip()
                # print(floor)
                # print(content)
                item = {}
                item['platform'] = '19楼'
                date_all = data.xpath('.//span[@class="u-add-ft"]/@title')[0]
                item['date'] = date_all.split(' ')[0].replace('发表于', '')
                item['time'] = date_all.split(' ')[1]

                self.source_date = date_all.split(' ')[0].replace('发表于', '')
                self.source_time = date_all.split(' ')[1]
                item['source_date'] = self.source_date
                item['source_time'] = self.source_time

                item['author'] = data.xpath('.//div[@class="user-name"]/a/span/text()')[0]
                try:
                    item['author_id'] = data.xpath('.//div[@class="user-name"]/a/@href')[0].split('-')[1]
                except:
                    item['author_id'] = ''
                item['post_client'] = ''
                item['title'] = title
                item['content'] = content
                item['content_id'] = url.split('-')[3]
                item['brand'] = ''
                item['carseries'] = ''
                item['from'] = ''
                item['series_url'] = ''
                item['url'] = url
                item['is_topics'] = '是'
                item['floor'] = '楼主'
                item['identification'] = ''
                item['favorite'] = ''
                item['signin_time'] = ''
                item['reply_no'] = replay_no
                item['views'] = views
                item['likes'] = ''
                item['is_elite'] = ''
                item['topic_count'] = ''
                item['reply_count'] = ''
                item['pick_count'] = ''
                item['follows'] = ''
                item['topic_categroy'] = ''
                item['topic_type'] = ''
                item['insert_time'] = str(datetime.now()).split('.')[0]
                item['update_time'] = str(datetime.now()).split('.')[0]
                item['topic_id'] = url.split('-')[3]
                item['reply_floor'] = ''
                item['keyword'] = keyword
                item['file_code'] = '186'
                item['reposts_count'] = ''
                # print(item)
                self.__write_news_jsonfile(item)

            if data.xpath('.//div[@itemprop="replyPost"]'):  # 判断文章回复
                reply_list = data.xpath('.//div[@itemprop="replyPost"]')
                for replay in reply_list:
                    content_list = replay.xpath('.//div[@class="post-cont"]//text()')
                    content = ''.join(content_list).strip()
                    # print(floor)
                    # print(content)
                    item = {}
                    item['platform'] = '19楼'
                    date_all = replay.xpath('.//div[@class="u-add link0 clearall"]/span[@class="fl"]/text()')[0]
                    print(11111, date_all)
                    item['date'] = date_all.split(' ')[0].replace('发表于', '')
                    item['time'] = date_all.split(' ')[1]
                    item['source_date'] = self.source_date
                    item['source_time'] = self.source_time
                    item['author'] = replay.xpath('.//a[@class="name"]/span/text()')[0]
                    try:
                        item['author_id'] = replay.xpath('.//a[@class="name"]/@href')[0].split('-')[1]
                    except:
                        item['author_id'] = ''
                    item['post_client'] = ''
                    item['title'] = title
                    item['content'] = content
                    item['content_id'] = replay.xpath('.//parent::div/@id')[0]
                    item['brand'] = ''
                    item['carseries'] = ''
                    item['from'] = ''
                    item['series_url'] = ''
                    item['url'] = url
                    item['is_topics'] = '否'
                    item['floor'] = replay.xpath('.//a[@itemprop="postSequenceNumber"]/text()')[0]
                    item['identification'] = ''
                    item['favorite'] = ''
                    item['signin_time'] = ''
                    item['reply_no'] = ''
                    item['views'] = ''
                    item['likes'] = ''
                    item['is_elite'] = ''
                    item['topic_count'] = ''
                    item['reply_count'] = ''
                    item['pick_count'] = ''
                    item['follows'] = ''
                    item['topic_categroy'] = ''
                    item['topic_type'] = ''
                    item['insert_time'] = str(datetime.now()).split('.')[0]
                    item['update_time'] = str(datetime.now()).split('.')[0]
                    item['topic_id'] = url.split('-')[3]
                    item['reply_floor'] = ''
                    item['keyword'] = keyword
                    item['file_code'] = '186'
                    item['reposts_count'] = ''
                    # print(item)
                    self.__write_news_jsonfile(item)

        else:   # 论坛类帖子
            div_list = data.xpath('.//div[@id="view-bd"]/div[@id and @itemprop]')
            for div in div_list:
                content_list = div.xpath('.//div[@class="thread-cont"]//text()')
                content = ''.join(content_list).strip()
                floor = div.xpath('.//div[@class="cont-hd clearall"]/a[@data-pid]/text() | .//span[@itemprop="postSequenceNumber"]/text() | .//em[@itemprop="postSequenceNumber"]/text()')
                floor = ''.join(floor).strip()
                # print(floor)
                # print(content)
                item = {}
                item['platform'] = '19楼'
                if floor == '楼主':
                    date_all = div.xpath('.//li[@title]/@title')[0]
                else:
                    date_all = div.xpath('.//p[@class="fl link1"]/span/text()')[0]

                item['date'] = date_all.split(' ')[0].replace('发表于', '')
                item['time'] = date_all.split(' ')[1]
                item['author'] = div.xpath('.//div[@class="uname"]/a/@title')[0]
                try:
                    item['author_id'] = div.xpath('.//div[@class="uname"]/a/@href')[0].split('-')[1]
                except:
                    item['author_id'] = ''
                try:
                    item['post_client'] = div.xpath('.//p[@class="forum-source fl link0"]/a/text()')[0]
                except:
                    item['post_client'] = ''
                item['title'] = title
                item['content'] = content
                item['content_id'] = div.xpath('.//div[@id and @class="cont"]/@id')[0].replace('pid', '')
                item['brand'] = ''
                item['carseries'] = ''
                item['from'] = ''
                item['series_url'] = ''
                item['url'] = url
                if floor == '楼主':
                    is_topics ='是'
                else:
                    is_topics = '否'


                item['is_topics'] = is_topics
                item['floor'] = floor
                item['identification'] = ''
                item['favorite'] = ''
                item['signin_time'] = div.xpath('.//dl/dd[@class="color6" and @itemprop]/text()')[0]
                if is_topics == '是':
                    item['reply_no'] = replay_no
                    item['views'] = views
                    self.source_date = date_all.split(' ')[0].replace('发表于', '')
                    self.source_time = date_all.split(' ')[1]

                else:
                    item['reply_no'] = ''
                    item['views'] = ''
                item['source_date'] = self.source_date
                item['source_time'] = self.source_time
                item['likes'] = ''
                item['is_elite'] = ''
                item['topic_count'] = ''
                item['reply_count'] = ''
                item['pick_count'] = ''
                item['follows'] = ''
                item['topic_categroy'] = ''
                item['topic_type'] = ''
                item['insert_time'] = str(datetime.now()).split('.')[0]
                item['update_time'] = str(datetime.now()).split('.')[0]
                item['topic_id'] = url.split('-')[3]
                item['reply_floor'] = ''
                item['keyword'] = keyword
                item['file_code'] = '186'
                item['reposts_count'] = ''
                # print(item)
                self.__write_news_jsonfile(item)

        if data.xpath('.//a[@class="page-next"]/@href'):
            next_page_url = 'https:' + data.xpath('.//a[@class="page-next"]/@href')[0].strip()
            self.get_news_page(next_page_url, title, views, replay_no, keyword, is_first=False)

    # 写入json文件
    def __write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../19/{}_floor_news.json'.format(str(datetime.now()).split(' ')[0]), 'ab') as f:
        # with open('{}/{}_floor_news_adidas.json'.format(self.file_path, str(datetime.now()).split(' ')[0]), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/186_{}_{}_floor_news.json'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def run(self):
        url = 'https://www.19lou.com/search/thread?keyword={}&sorts=0&timeType=1&fids=undefined&usesearchtype=1'
        url_list = get_config_para('nike_daily_keywords')
        logger.log(31, url_list)
        for item in url_list:
            # print(1)
            keyword = item['keywords']
            logger.log(31, keyword)
            if keyword:
                search_url = url.format(keyword.strip())
                logger.info('搜索url:' + search_url)
                self.get_search_page(search_url, keyword)


if __name__ == "__main__":
    logger.info(sys.argv)
    # file_path = sys.argv[1]
    file_path = '/user/cspider_daily/nike_daily/forum'
    spider = Spider(file_path)
    try:
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.info('程序结束......')