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
import math
import sys
from with_hdfs import HdfsClient


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
    get 文章
    """
    def __init__(self, file_path, comment_path, need_time):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        # # 时间判断部分
        # date = datetime.now() - timedelta(days=300)
        # news_start_time = str(date).split(' ')[0]
        # yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
        # yesterday = str(yesterday).split(' ')[0]
        #
        # # 定义开始时间 y-m-d  离现在时间远  news_start_time
        # self.start_time = news_start_time
        # # 定义结束时间 y-m-d  离现在时间近  yesterday
        # self.end_time = yesterday
        # print('爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        #
        # logging.info('爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        #
        # # 定义评论的抓取时间范围
        # # self.comment_start_time = yesterday  # 一天回复
        # self.comment_start_time = '2019-08-01'  # 一天回复
        # # self.comment_start_time = ''  # 不限定时间回复
        #
        # self.comment_end_time = yesterday
        # # self.comment_end_time = yesterday

        # get_now_time = time.time() - 86400
        get_now_time = time.time() - int(need_time)
        time_local = time.localtime(float(get_now_time))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        end_t = time.time()
        time_local = time.localtime(float(end_t))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        end_dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # end_time = str(end_time).split(' ')[0]
        logger.log(31, '爬取时间段：{}到{}'.format(dt, end_dt))
        # 定义开始时间 y-m-d  离现在时间远
        self.start_time = dt
        # self.start_time = '2019-09-09 12:01'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = end_dt
        # self.end_time = '2019-09-16 12:01'
        # 标记爬虫工作
        self.is_work = True
        self.file_name_time = self.get_file_name_time()
        self.file_path = file_path
        self.comment_apth = comment_path
        self.hdfsclient = HdfsClient(url='http://jq-chance-05:9870', user='dpp-executor')
        hour = str(datetime.now()).split(' ')[-1].split(':')[0]
        if str(hour) != '00':
            two_hour_ago = int(hour) - 2
            if len(str(two_hour_ago)) == 1:
                two_hour_ago = '0' + str(two_hour_ago)
            self.hour_name = str(two_hour_ago) + '_' + str(hour)
        else:
            self.hour_name = '22_24'
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]

    def get_news_list_page(self, url):
        """
        新闻版块列表页
        :param url:
        :return:
        """
        logger.log(31, '新闻版块列表页:' + url)
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode())
        li_list = data.xpath('.//ul[@class="clearfix"]/li')
        self.is_work = True

        for li in li_list:
            title = li.xpath('.//div[@class="news-intro"]/text()')[0]
            # print(title)
            views = li.xpath('.//span[@class="tip-view"]/text()')[0]
            comments_count = li.xpath('.//span[@class="tip-comment"]/text()')[0]
            date_all = li.xpath('.//span[@class="tip-date"]/text()')[0]
            date_all = self.time_change(date_all)
            # 做时间判断部分---------------
            logger.log(31, '时间' + date_all)
            if len(date_all) == 10:
                date_all += ' 12:01:01'
            if len(date_all) == 7:
                date_all += '-01 12:01:01'
            get_news_time = time.mktime(time.strptime(date_all[:-3], "%Y-%m-%d %H:%M"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
            if float(get_news_time) < float(start_time):
                self.is_work = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
            elif float(start_time) <= float(get_news_time) <= float(end_time):
                # print(views, comments_count, date_all)
                news_url = 'http://www.dunkhome.com' + li.xpath('.//a[@title]/@href')[0].strip()
                self.get_news_info(news_url, views, comments_count, title)

        if self.is_work:  # 判断是否要继续进行翻页
            # 列表页翻页
            next_page = data.xpath('.//a[@rel="next"]/@href')[0]
            # print(next_page)
            next_page_url = 'http://www.dunkhome.com' + next_page
            self.get_news_page_list_two(next_page_url)
        else:
            logger.log(31, '版块抓取到指定时间......')

    def get_news_page_list_two(self, url):
        """
        社区版块 列表页
        :param url:
        :return:
        """
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode())
        li_list = data.xpath('.//div[@class="items"]/div[@class="evaluation-item s-object-item s-eva-item"]')
        self.is_work = True
        for li in li_list:
            title = li.xpath('.//h6/text()')[0]
            # print(title)
            views = ''
            comments_count = li.xpath('.//a[@class="item-comment"]/text()')[0]
            date_all = li.xpath('.//span[@class="item-time" ]/text()')[0]
            date_all = self.time_change(date_all)
            # 做时间判断部分---------------
            # print(date_all)
            if len(date_all) == 16:
                date_all += ':12'
            get_news_time = time.mktime(time.strptime(date_all[:-3], "%Y-%m-%d %H:%M"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
            if float(get_news_time) < float(start_time):
                self.is_work = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
            elif float(start_time) <= float(get_news_time) <= float(end_time):
                # print(views, comments_count, date_all)
                news_url = 'http://www.dunkhome.com' + li.xpath('.//div[@class="item-content"]/a/@href')[0].strip()
                try:
                    self.get_news_info(news_url, views, comments_count, title)
                except:
                    pass

        if self.is_work:  # 判断是否要继续进行翻页

            # 列表页翻页
            next_page = data.xpath('.//a[@rel="next"]/@href')[0]
            # print(next_page)
            next_page_url = 'http://www.dunkhome.com' + next_page
            self.get_news_list_page(next_page_url)
        else:
            logger.log(31, '版块抓取到指定时间......')

    def get_news_info(self, url, views, comments_count, title):
        logger.log(31, url)
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode())
        content_list = data.xpath('.//div[@class="content s-news-content"]//p/text() | .//div[@class="show-content"]//p/text()')
        # print(content_list)
        item = {}
        item['platform'] = 'get'
        try:
            date_all = data.xpath('.//div[@class="fl"]/span/text()')[0]
            item['date'] = date_all.split(' ')[0]
        except:
            date_all = data.xpath('.//span[@class="i-time"]/text()')[0]
            item['date'] = date_all.split(' ')[0]
        item['time'] = date_all.split(' ')[1]
        item['title'] = title
        item['content'] = ''.join(content_list)
        item['content_id'] = url.split('/')[-1]
        try:
            item['article_author'] = data.xpath('.//span[@class="s-name"]/text()')[0]
        except:
            try:
                item['article_author'] = data.xpath('.//span[@class="i-nickname"]/text()')[0]
            except:
                item['article_author'] = ''
        item['clicks'] = ''
        item['views'] = views
        item['comments_count'] = comments_count
        try:
            item['likes'] = data.xpath('.//span[@class="item-like"]/text()')[0]
        except:
            item['likes'] = ''
        item['dislikes'] = ''
        item['keyword'] = ''
        item['article_url'] = url
        item['series_url'] = ''
        item['list_url'] = ''
        item['article_type'] = ''
        item['article_source'] = ''
        item['insert_time'] = str(datetime.now()).split('.')[0]
        item['update_time'] = str(datetime.now()).split('.')[0]
        topic_id = url.split('/')[-1]
        item['topic_id'] = url.split('/')[-1]
        item['content_id'] = url.split('/')[-1]
        item['reposts_count'] = ''
        item['file_code'] = '154'
        try:
            item['author_id'] = data.xpath('.//div[@class="t-user-avator"]/a/@href')[0].split('/')[-1]
        except:
            try:
                item['author_id'] = data.xpath('.//div[@class="avator"]/img/@src')[0].split('/')[-1].split('.')[0].split('_')[-1]
            except:
                item['author_id'] = ''
        # print(item)
        self.write_news_jsonfile(item)
        if int(comments_count) > 0:
            all_page = math.ceil(float(int(comments_count))/10)

            for i in range(1, int(all_page)+1):
                comment_url = url + '?page=' + str(i)
                self.get_comment(comment_url, url, title, topic_id)

    def get_comment(self, url, news_url, title, topic_id):
        # print(111111111111111111111111)
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode())
        li_list = data.xpath('.//div[@class="comment-list"]/ul/li')
        for li in li_list:
            content_id =  li.xpath('.//parent::li/@data-id')[0]
            # print(etree.tostring(li))
            content = li.xpath('.//div[@class="c-message"]//p/text()')[0]
            item = {}
            item['platform'] = 'get'
            item['source_date'] = ''
            item['source_time'] = ''
            date_all = li.xpath('.//div[@class="c-nickname"]/text()')[0].strip()
            date_all = self.time_change(date_all)
            #  评论部分做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(date_all[:-3], "%Y-%m-%d %H:%M"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
            if float(get_news_time) < float(start_time):
                self.is_get_comment = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                break
            elif float(start_time) <= float(get_news_time) <= float(end_time):

                item['date'] = date_all.split(' ')[0]
                item['time'] = ''
                item['title'] = title
                item['author'] = li.xpath('.//div[@class="c-nickname"]/span/text()')[0].strip()
                item['author_id'] = li.xpath('.//div[@data-user-id]/@data-user-id')[0]
                item['content'] = content
                item['content_id'] = content_id
                item['floor'] = ''
                item['keyword'] = ''
                item['source_url'] = news_url
                item['comment_url'] = ''
                item['views'] = ''
                item['comments_count'] = ''
                try:
                    item['likes'] = li.xpath('.//a[@class="item-like"]/text()')[0]
                except:
                    item['likes'] = ''
                item['dislikes'] = ''
                item['insert_time'] = str(datetime.now()).split('.')[0]
                item['update_time'] = str(datetime.now()).split('.')[0]
                item['topic_id'] = topic_id
                item['reposts_count'] = ''
                item['file_code'] = '155'
                # print(item)
                self.write_comment_jsonfile(item)

    def time_change(self, str_time):
        """
        时间可是转换， 将‘分钟前’，‘小时前’，‘昨天’，‘前天’，转换成标准时间格式Y-m-d h:m:s
        :param str_time:
        :return:
        """
        # print(str_time, 55555555555)
        if '秒' in str_time or '刚刚' in str_time:
            get_time = str(datetime.now()).split('.')[0]
            return get_time

        elif '分钟' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '小时' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '今天' in str_time:
            part_time = str_time.split(' ')[1]
            yesterday = datetime.now() - timedelta(days=0)  # 今天时间
            dt = str(yesterday).split(' ')[0] + ' ' + part_time
            return dt

        elif '昨天' in str_time:
            part_time = str_time.split(' ')[1]
            yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0] + ' ' + part_time
            return yesterday

        elif '前天' in str_time:
            part_time = str_time.split(' ')[1]
            two_days_ago = datetime.now() - timedelta(days=2)  # 前天时间
            two_days_ago = str(two_days_ago).split(' ')[0] + ' ' + part_time.replace('点', ':').replace('分', '')
            return two_days_ago

        elif '天前' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60 * 24
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '201' not in str_time:
            str_time = '2019-' + str_time
            return str_time
        else:
            return str_time

    # 写入json文件
    def write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../get/json_file/{}/{}_get_news.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/154_{}_{}_get_news.json'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def write_comment_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../get/json_file/{}/{}_get_news_comments.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/155_{}_{}_get_news_comments.json'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

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
        url = 'http://www.dunkhome.com/news'
        self.get_news_list_page(url)
        url = 'http://www.dunkhome.com/evaluations'
        self.get_news_page_list_two(url)
        # self.get_news_info('http://www.dunkhome.com/news/57696', '', '', '')
        # self.get_comment('http://www.dunkhome.com/news/57451#s-shared-comment')


if __name__ == "__main__":
    print(sys.argv)
    file_path = sys.argv[1]
    comment_path = sys.argv[2]
    need_time = sys.argv[3]
    try:
        spider = Spider(file_path, comment_path, need_time)
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.log(31, '程序结束......')
