import requests
from lxml import etree
import json
import re
import math
import time
import ast
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import xlrd
import os
import sys
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
from config_para import get_config_para

# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./../aiqiyi/aiqiyi-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)

class AiQiYi(object):
    """
    这是一个爬虫模板
    """
    def __init__(self, file_path, comment_path):

        a = str(datetime.now())
        hour = a.split(' ')[-1].split(':')[0]
        num = int(hour) / 3
        num = int(num) * 3
        if num == 0:   # 对于凌晨 0 点的判断
            # 时间判断部分
            date = datetime.now() - timedelta(days=1)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        else:
            # 时间判断部分
            date = datetime.now() - timedelta(days=0)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        # print('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # self.start_time = '2019-09-09'
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # self.end_time = '2019-09-16'
        # 标记爬虫工作
        self.is_work = True
        self.is_stop = False
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

        logger.warning('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))
        logger.warning('{}/{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]

    def get_video_list(self, url, keyword):
        logger.info('搜索url:  ' + url  + '   ' + keyword)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=120)
        data = etree.HTML(response.content.decode())
        # print(response.text)
        video_list = data.xpath('.//div[@class="qy-search-result-item"]')
        for video in video_list:
            try:
                video_url = video.xpath('.//h3[@class="qy-search-result-tit title-line"]/a/@href')[0]
                print(2222222, video_url)
                try:
                    video_time = video.xpath('.//div[@class="qy-search-result-info half"]/span[@class="info-des"]/text()')[0]
                except:
                    video_time =str(datetime.now()).split(' ')[0]
                logger.info('视频时间：' + video_time)
                # 做时间判断部分---------------
                get_news_time = time.mktime(time.strptime(video_time, "%Y-%m-%d"))
                end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))

                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
                if float(get_news_time) < float(start_time):
                    self.is_stop = True  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                    break

                elif float(start_time) <= float(get_news_time) <= float(end_time):
                    try:
                        if 'http' not in video_url:
                            video_url = 'https:' + video_url
                        self.get_video_page(video_url, keyword)
                    except:
                        print(traceback.format_exc())
                        logger.error(traceback.format_exc())
            except:
                pass

        if data.xpath('.//a[@data-key="down"]') and not self.is_stop:
            next_page = data.xpath('.//a[@data-key="down"]/@href')[0]
            next_page = 'https://so.iqiyi.com' + next_page.strip()
            print(next_page)
            self.get_video_list(next_page, keyword)

    def get_video_page(self, url, keyword):
        logger.info('视频url: ' + url)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=120)
        # data = etree.HTML(response.content.decode())
        # page_info = data.xpath('.//div[@id="iqiyi-main"]/div/@page-info')
        # print(page_info)
        if response.status_code == 200:
            text = response.text

            get_page_info = re.search("page-info=[\S\s]*video-info", text)[0]
            try:
                page_info = get_page_info[11:][:-13]
                page_info = json.loads(page_info)
            except:
                try:
                    page_info = get_page_info[11:][:-14]
                    page_info = json.loads(page_info)
                except:
                    # print(get_page_info)
                    logger.error(traceback.format_exc())
            # print(page_info)
            video_info = re.search("video-info=[\S\s]*}'>", text)[0]
            video_info = video_info[12:][:-2]
            video_info = json.loads(video_info)
            item = {}
            item['platform'] = '爱奇艺'
            date_all = video_info['firstPublishTime']
            date_all = str(date_all)[:-3]
            # #转换成localtime
            time_local = time.localtime(float(date_all))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            item['date'] = dt.split(' ')[0]  # 发布日期
            item['time'] = dt.split(' ')[1]  # 发布时间
            item['title'] = video_info['name']  # 视频标题
            item['description'] = video_info['description']  # 视频描述
            try:
                item['source_author'] = video_info['user']['name']  # 来源/上传者
                item['followers_count'] = video_info['user']['followerCount']  # 粉丝数
            except:
                item['source_author'] = ''
                item['followers_count'] = ''
            item['clicks'] = ''  # 点击数
            item['play'] = ''  # 播放量
            item['keyword'] = keyword  # 关键词
            item['url'] = url  # URL
            try:
                item['categroy'] = video_info['categories'][0]['name']  # 视频分类
            except KeyError:
                item['categroy'] = ''
            video_id = video_info['tvId']
            likes = self.get_likes_count(video_id)  # 获取点赞数
            item['likes'] = likes  # 点赞数
            page = 1
            comment_count = self.get_comment_count(video_id, page)  # 获取评论数
            item['comments_count'] = comment_count  # 评论数
            item['topic_id'] = url.split('/')[-1].split('.')[0]  # 主贴id
            item['author_id'] = video_info['user']['id']  # 作者id
            item['content_id'] = url.split('/')[-1].split('.')[0]
            item['file_code'] = '111'

            # print(item)
            self.write_news_jsonfile(item)
            if int(comment_count) > 0:
                self.get_comment(video_id, page, url, video_info['name'], comment_count, keyword)  # 获取评论

    def get_likes_count(self, video_id):
        url = 'http://iface2.iqiyi.com/like/count?businessType=14&entityId={}&qyid=63204618cb07f6722139214f3b31f1b0&callback=jsonp_1550734824178_93496'.format(str(video_id))
        response = requests.get(url)
        text = response.text
        text = text[30:][:-2]
        text = json.loads(text)
        likes = text['data']
        return likes

    def get_comment_count(self, video_id, page):
        """
        获取评论数量
        :param video_id:
        :param page:
        :return:
        """
        # http://sns-comment.iqiyi.com/v3/comment/get_comments.action?agent_type=118&agent_version=9.11.5&authcookie=null&business_type=17&content_id=31067882509&hot_size=10&last_id=&page=1&page_size=10&types=hot,time&callback=jsonp_1550734826037_45721
        url = 'http://sns-comment.iqiyi.com/v3/comment/get_comments.action?agent_type=118&agent_version=9.11.5&authcookie=null&business_type=17&content_id={}&hot_size=10&last_id=&page={}&page_size=20&types=hot,time&callback=jsonp_1550734826037_45721'.format(str(video_id), str(page))
        response = requests.get(url)
        text = response.text
        text = text[31:][:-14]
        text = json.loads(text)
        # print(text)
        comment_count = text['data']['count']
        # print(comment_count)
        return comment_count

    def get_comment(self, video_id, page, source_url, title, comment_count, keyword):
        """
        获取评论内容, 和上面的分开写是为了方便调用和修改
        :param video_id:
        :param page:
        :return:
        """
        # http://sns-comment.iqiyi.com/v3/comment/get_comments.action?agent_type=118&agent_version=9.11.5&authcookie=null&business_type=17&content_id=31067882509&hot_size=10&last_id=&page=1&page_size=10&types=hot,time&callback=jsonp_1550734826037_45721
        url = 'http://sns-comment.iqiyi.com/v3/comment/get_comments.action?agent_type=118&agent_version=9.11.5&authcookie=null&business_type=17&content_id={}&hot_size=10&last_id=&page={}&page_size=20&types=hot,time&callback=jsonp_1550734826037_45721'.format(str(video_id), page)
        response = requests.get(url)
        text = response.text
        text = text[31:][:-14]
        text = json.loads(text)
        # print(22222, text)
        comments_list = text['data']['comments']
        for comment in comments_list:
            # print(comment)
            item = {}
            item['platform'] = '爱奇艺'

            time_all = comment['addTime']
            # #转换成localtime
            time_local = time.localtime(float(time_all))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            # print(dt)
            item['date'] = str(dt).split(' ')[0]
            item['time'] = str(dt).split(' ')[1]
            item['title'] = title
            item['author'] = comment['userInfo']['uname']
            item['content'] = comment['content']
            item['floor'] = comment['floor']
            item['keyword'] = keyword
            item['comment_url'] = source_url
            item['source_url'] = source_url
            item['comments_count'] = ''
            item['likes'] = comment['likes']
            item['views'] = ''
            item['topic_id'] = source_url.split('/')[-1].split('.')[0]  # 主贴id
            item['author_id'] = comment['userInfo']['uid']  # 作者id
            item['content_id'] = comment['id']  # 作者id
            item['file_code'] = '112'
            self.write_comment_jsonfile(item)
        if int(comment_count) > 20*page:  # 判断评论数量，进行翻页操作
            page += 1
            self.get_comment(video_id, page, source_url, title, comment_count, keyword)

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

    # 写入json文件
    def write_news_jsonfile(self, item):
        logger.log(31, '写入视频数据......')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../aiqiyi/json_file/{}/{}_aiqiyi_video_adidas.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/111_{}_{}_aiqiyi_video.json'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def write_comment_jsonfile(self, item):
        logger.log(31, '写入视频评论数据......')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../aiqiyi/json_file/{}/{}_aiqiyi_video_comment_adidas.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/112_{}_{}_aiqiyi_video_comment.json'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def run(self):

        url_list = get_config_para('nike_daily_keywords')
        logger.log(31, url_list)
        for item in url_list:
            # print(1)
            keyword = item['keywords']
            logger.log(31, keyword)
            # https://so.iqiyi.com/so/q_%E5%A5%A5%E8%BF%AAA3%E4%B8%A4%E5%8E%A2_ctg_%E6%B1%BD%E8%BD%A6_t_0_page_1_p_1_qc_0_rd__site_iqiyi_m_4_bitrate_?af=true
            # for keyword in cols:
            url = 'https://so.iqiyi.com/so/q_{}_ctg__t_0_page_1_p_1_qc_0_rd_2_site_iqiyi_m_4_bitrate_?af=true'.format(keyword)
            self.is_stop = False
            self.get_video_list(url, keyword)

        # self.get_video_page('http://www.iqiyi.com/w_19s4ej82k1.html')

if __name__ == "__main__":
    print(sys.argv)
    file_path = sys.argv[1]
    comment_path = sys.argv[2]
    try:
        spider = AiQiYi(file_path, comment_path)
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.info('程序结束......')
