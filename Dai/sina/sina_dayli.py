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
import uuid
import sys
from with_hdfs import HdfsClient
# from multiprocessing import Queue
from queue import Queue

sys.setrecursionlimit(3000)


# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
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
    def __init__(self, file_path, comment_path):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        # 时间判断部分
        date = datetime.now() - timedelta(days=7)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        logging.log(31, '爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        # 定义评论的抓取时间范围
        # self.comment_start_time = yesterday  # 一天回复
        # self.comment_start_time = '2019-08-01'  # 一天回复
        self.comment_start_time = ''  # 不限定时间回复
        self.comment_end_time = yesterday
        # self.comment_end_time = yesterday
        # 标记爬虫工作
        self.is_work = True
        self.commnet_port_url = 'http://comment.sina.com.cn/page/info?version=1&format=json&channel=ty&newsid=comos-{}&group=0&compress=0&ie=utf-8&oe=utf-8&page={}&page_size=10&t_size=3&h_size=3&thread=1&callback=jsonp_1542676393124&_=1542676393124'
        self.page_num = 1
        self.file_path = file_path
        self.comment_apth = comment_path
        self.hdfsclient = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
        self.hdfsclient.makedirs('{}/{}'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}'.format(self.comment_apth, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]

        self.article_queue = Queue()
        self.comment_queue = Queue()
        self.total_item = ''


    def get_list_page(self, url):
        logger.log(31, '列表页url:  ' + url)
        response = requests.get(url, headers=self.headers_one)
        data = json.loads(response.text[46:-14])
        list_data = data['result']['data']
        for li_data in list_data:
            news_url = li_data['url']
            ctime = li_data['ctime']
            time_local = time.localtime(float(ctime))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            #
            try:
                self.get_news_info(news_url, '', 'http://sports.sina.com.cn/roll/index.shtml#pageid=13&lid=2503&k=&num=50&page=1', dt)
            except:
                logger.error(traceback.format_exc())

    def get_news_info(self, url, news_type, page_list, date_all):
        logger.log(31, '新闻url:  ' + url)
        item = dict()
        response = requests.get(url, headers=self.headers_one, timeout=60)
        try:
            data = etree.HTML(response.content.decode())
            # 网站
            item['platform'] = '新浪新闻'
            # 标题
            try:
                title = data.xpath('.//h1[@class="main-title"]/text()')[0]
            except:
                title = data.xpath('.//h2/text()')[0]
            item['title'] = title
            # date_all = data.xpath('.//div[@class="date-source"]/span/text()')[0].replace('年', '-').replace('月', '-').replace('日', '')
            date = date_all.split(' ')[0]
            news_time = date_all.split(' ')[1]
            # print(date)
            item['date'] = date
            item['time'] = news_time
            # 文章来源
            try:
                article_source = data.xpath('.//div[@class="date-source"]/a/text()')[0]
            except:
                article_source = data.xpath('.//p[@class="from"]/span[2]//text()')
                article_source = ''.join(article_source)
            item['article_source'] = article_source
            # article_author
            try:
                article_author = data.xpath('.//div[@class="show_author"]/text()')
            except:
                article_author = ''
            if article_author:
                item['article_author'] = article_author[0]
            else:
                item['article_author'] = ''
            # 内容
            try:
                content = data.xpath('.//div[@id="article_content"]/div[1]/div/p/text()')
            except:
                content = data.xpath('.//em[@class="vdiCont"]//text()')
            content = ''.join(content)
            # 翻页数据
            next_page = data.xpath('.//div[@data-sudaclick="content_pagination_p"]/a/@href')
            if len(next_page) > 3:
                next_page = next_page[1:][:-2]
                for page_url in next_page:
                    print('获取翻页数据')
                    next_content = self.get_next_page(page_url)
                    content = content + next_content

            item['content'] = content

            # 从接口处获取评论数
            news_id = re.search('(\w{7}\d{7})', url).group(0)
            try:
                comment_count = self.get_commnet_count(news_id)
            except AttributeError:
                comment_count = '0'
            item['comments_count'] = comment_count
            item['clicks'] = ''
            item['views'] = ''
            item['likes'] = ''
            item['keyword'] = ''
            item['article_url'] = url  # 文章详情URL
            item['dislikes'] = ''  # 踩人数
            item['series_url'] = ''  # 车系首页
            item['list_url'] = page_list  # 文章列表URL
            # item['article_type'] = news_type  # 文章类型
            item['article_type_1st'] = news_type  # 文章类型
            item['article_type_2nd'] = ''  # 文章类型
            item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
            item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
            content_id = url.split('/')[-1].split('.')[0].split('_')[-1].split('-')[-1]
            # content_id = re.search('\d{5,8}', content_id).group(0)
            item['content_id'] = str(content_id)  # 文章id
            item['topic_id'] = str(content_id)  # 主贴id
            item['author_id'] = ''  # 作者id
            item['file_code'] = '17'
            item['reposts_count'] = ''
            # 做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                self.is_work = False
                return

            if float(start_time) <= float(get_news_time) <= float(end_time):
                self.write_news_jsonfile(item)

                # self.article_queue.put(item)
                if int(comment_count) > 0:
                    self.is_get_comment = True

                    while True:
                        if self.is_get_comment:
                            self.get_comments_info(news_id, title, date, news_time, url)
                        else:
                            self.page_num = 1
                            break
        except IndexError:
            time.sleep(5)
            logger.error('网页请求404 url: {},   {}'.format(url, traceback.format_exc()))

    # 获取翻页数据
    def get_next_page(self, url):
        response = requests.get(url, headers=self.headers_one, timeout=60)
        try:
            data = etree.HTML(response.content)
            # 内容
            content = data.xpath('.//div[@id="article_content"]/div[1]/div/p/text()')
            content = ''.join(content)
            return content
        except:
            content = ''
            return content

    # 获取评论数
    def get_commnet_count(self, news_id):
        response = requests.get(self.commnet_port_url.format(news_id, str(1)))
        data = response.content.decode()
        data = data[20:][:-1]
        # print(11111,data)
        data = json.loads(data)
        # print(222222,data)
        # data = re.search('"qreply": \d{0,9}', data).group(0)
        try:
            comment_count = data['result']['count']['show']
        except:
            comment_count = 0
        return comment_count

    # 获取评论信息
    def get_comments_info(self, news_id, title, source_date, source_time, source_url, page_id="1"):

        url = self.commnet_port_url.format(news_id, str(self.page_num))
        response = requests.get(url)
        data = response.content.decode()
        # data = re.search(r'{"result.*}\)', data).group(0)
        data = data[20:][:-1]
        data = json.loads(data)
        comments_list = data['result']['cmntlist']
        if comments_list:
            for comment in comments_list:
                item = {}
                item['platform'] = u'新浪新闻'
                item['source_date'] = source_date
                item['source_time'] = source_time
                date_all = comment['time']
                date = date_all.split(' ')[0]
                commnet_time = date_all.split(' ')[1]
                item['date'] = date
                item['time'] = commnet_time
                #  评论部分做时间判断部分---------------
                get_news_time = time.mktime(time.strptime(str(date), "%Y-%m-%d"))
                end_time = time.mktime(time.strptime(self.comment_end_time, "%Y-%m-%d"))
                if self.comment_start_time != '':
                    start_time = time.mktime(time.strptime(self.comment_start_time, "%Y-%m-%d"))
                else:
                    start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
                if float(get_news_time) < float(start_time):
                    self.is_get_comment = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                    break
                elif float(start_time) <= float(get_news_time) <= float(end_time):

                    item['title'] = title
                    author = comment['nick']
                    item['author'] = author
                    item['author_id'] = comment['uid']  # 用户id
                    content = comment['content']

                    item['content'] = content
                    item['floor'] = ''
                    item['keyword'] = ''
                    item['source_url'] = source_url
                    comment_url = 'http://comment5.news.sina.com.cn/comment/skin/default.html?channel=ty&newsid=comos-{}&group=0'.format(
                        news_id)
                    item['comment_url'] = comment_url
                    item['views'] = ''
                    item['comments_count'] = ''
                    likes = comment['agree']
                    item['likes'] = likes
                    item['dislikes'] = ''  # 踩人数
                    item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                    item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                    item['content_id'] = str(uuid.uuid4()).replace('-', '')
                    topic_id = source_url.split('/')[-1].split('.')[0].split('_')[-1].split('-')[-1]
                    # topic_id = re.search('\d{5,8}', topic_id).group(0)
                    item['topic_id'] = topic_id  # 主贴id
                    item['file_code'] = '31'
                    item['reposts_count'] = ''
                    # self.write_comment_jsonfile(item)
                    # print(11111111, item)
                    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                    self.total_item = self.total_item + item
                    # self.comment_queue.put(item)
            self.write_comment_jsonfile()
            if self.is_get_comment:
                self.page_num += 1
                # self.get_comments_info(news_id, title, source_date, source_time, source_url,page_id=str(self.page_num))
        else:
            self.page_num = 1
            logger.log(31, '评论抓取完毕   '+ url)
            self.is_get_comment = False
    # ------------------------------------------------新能源模块--------------------------------------------------------

    def write_news_jsonfile(self, item):

        # q_size = self.article_queue.qsize()
        # total_item = ''
        # if q_size > 0:
        #     for i in range(q_size):
        #         item = self.article_queue.get()
        #         # print('写入数据中......')
        print('写入新闻数据......')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                # total_item += item
        try:
            self.hdfsclient.new_write('{}/{}/17_{}_{}_sina_news.json'.format(self.file_path,str(datetime.now()).split(' ')[0].replace('-', ''),str(datetime.now()).split(' ')[0].replace('-', '_'),self.time_time), item,encoding='utf-8')
        except:

            logging.error(traceback.format_exc())
            self.write_news_jsonfile(item)
            return
        # else:
        #     pass


    def write_comment_jsonfile(self):
        # q_size = self.comment_queue.qsize()
        # total_item = ''
        # if q_size > 0:
        #     print(q_size)
        #     for i in range(q_size):
        #         item = self.comment_queue.get()
        #         print(2222222, item)
        #         # print('写入数据中......')
        #         item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        #         total_item = total_item + item
        #
        #     # try:
        #     #
        #     #     self.hdfsclient.new_write('{}/{}/31_{}_{}_sina_comment.json'.format(self.comment_apth, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), total_item,encoding='utf-8')
        #     # except:
        #     #     logging.error(traceback.format_exc())
        # else:
        #     pass
        # print(3333333, total_item)
        try:
            self.hdfsclient.new_write('{}/{}/31_{}_{}_sina_comment.json'.format(self.comment_apth, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), self.total_item,encoding='utf-8')
        except:
            print('写入重试中......')
            self.write_comment_jsonfile()
            return
        print('写入成功......')
        self.total_item = ''


    def run(self):
        for i in range(1, 100):
            if self.is_work:
                url = 'http://feed.mix.sina.com.cn/api/roll/get?pageid=13&lid=2503&k=&num=50&page={}&r=0.6019004029484454&callback=jQuery311016308312964736538_1566799251373&_=1566799251388'.format(str(i))
                self.get_list_page(url)
                # self.write_news_jsonfile()
            else:
                logger.log(31, '爬取到指定时间......')


if __name__ == "__main__":
    # print(sys.argv)
    # file_path = sys.argv[1]
    # comment_path = sys.argv[2]

    file_path = '/user/cspider_daily/nike_daily/article'
    comment_path = '/user/cspider_daily/nike_daily/articlecomments'
    spider = Spider(file_path, comment_path)
    try:
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.log(31, '程序结束......')
