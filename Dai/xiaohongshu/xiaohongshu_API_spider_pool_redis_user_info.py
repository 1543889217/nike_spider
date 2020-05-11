import os, sys
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
from xhsapi import XhsApi
import math
import random
import xlrd
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
from config_para import get_config_para
import redis


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

def retry_if_key_error(exception):
    logger.log(31, '重试一次中......')
    return isinstance(exception, KeyError)

class Spider(object):
    """
    这是一个爬虫模板
    """
    def __init__(self):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        date = datetime.now() - timedelta(days=700)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        # print('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        #
        logger.log(31, '爬取时间段：{}到{}'.format(news_start_time, yesterday))
        #
        # # 定义开始时间 y-m-d  离现在时间远  news_start_time
        # self.start_time = news_start_time
        self.start_time = '2019-09-01'
        # # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # # 标记爬虫工作
        self.xhsapi = XhsApi('8ac1d719cd0a2d16')
        self.is_work = True
        self.set_list = []
        self.file_name_time = self.get_file_name_time()
        with open('./all_session_id.json') as f:
            session_id = f.read()
        self.session_id_list = eval(session_id)
        self.time_time = str(time.time()).split('.')[0]
        self.pid = os.getpid()
        self.session_id_error = []
        self.queue = ''
        self.comment_queue = ''
        self.pid = os.getpid()
        pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
        self.redis_example = redis.Redis(connection_pool=pool)
        self.error_count = 0

        # 代理服务器
        proxyHost = "http-cla.abuyun.com"
        proxyPort = "9030"

        # 代理隧道验证信息
        proxyUser = "H3487178Q0I1HVPC"
        proxyPass = "ACE81171D81169CA"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            "http": proxyMeta,
            "https": proxyMeta
        }

    def get_sid(self):
        register_smid_ret = self.xhsapi.register_smid()
        print('register_smid_ret:' + register_smid_ret)
        smid = json.loads(register_smid_ret)['detail']['deviceId']
        print('smid:' + smid)
        self.xhsapi.set_smid(smid)
        # 激活用户
        active_user_ret = self.xhsapi.active_user()
        print('active_user_ret:' + active_user_ret)
        # 设置session id
        session_id = json.loads(active_user_ret)['data']['session']
        print('session_id:' + session_id)

        return smid, session_id

    def change_ip(self):
        logger.log(31, '开始切换ip')
        url = 'http://proxy.abuyun.com/switch-ip'
        time.sleep(random.randint(1, 15))
        response = requests.get(url, proxies=self.proxies)
        logger.log(31, '现使用ip：'+ response.text)

    def res_ip_three_hour(self):
        """
        25分钟-3小时
        :return:
        """
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'webapi.http.zhimacangku.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        }
        # 5-25分 500个ip
        url = 'http://http.tiqu.alicdns.com/getip3?num=1&type=1&pro=310000&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4'
        time.sleep(random.randint(3, 30))
        ip_pro = requests.get(url, headers=headers)
        ip = ip_pro.text.strip()
        if '请2秒后再试' in ip or '请重新提取' in ip:
            return self.res_ip_three_hour()
        return ip

    def get_serach_list(self, page, keyword):
        info = random.choice(self.session_id_list)
        # info = self.sid_info
        logger.log(31, 'session_id下标:  ' + str(self.session_id_list.index(info)))
        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        try:
            search_ret = self.xhsapi.search(keyword, page, 20)
        except:
            if '503 Service Temporarily' in traceback.format_exc():
                self.change_ip()
                try:
                    search_ret = self.xhsapi.search(keyword, page, 20)
                except:
                    return
            else:
                return
        # print(222222222222, search_ret)
        if '{"msg":"","result":0,"success":true}' in search_ret:
            self.session_id_error.append(info)
            if self.session_id_error.count(info) > 5:
                logger.log(31, '无效id：' + str(info))
                del self.session_id_list[self.session_id_list.index(info)]
            if self.error_count > 5:
                self.change_ip()
                self.error_count = 0
            self.error_count += 1
            self.get_serach_list(page, keyword)
            return

        if '参数错误' in search_ret:
            logger.log(31, '参数错误，重试.....')
            self.get_serach_list(page, keyword)
            return

        json_text = json.loads(search_ret)
        # print(json_text)
        note_list = json_text["data"]["notes"]
        for note in note_list:
            title = note["title"]
            if not title:
                title = note["desc"]
            id = note["id"]
            time.sleep(0.1)
            logger.log(31, '标题：  ' + title)
            if id not in self.set_list and not self.redis_example.sismember('xiaohongshu_keyword_have_user_info', id):
                # logger.log(31, '标题：  ' + title)
                try:
                    self.get_note(id, keyword)
                except:
                    if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                        self.change_ip()
                    print(traceback.format_exc())
                self.set_list.append(id)
            else:
                logger.log(31, '根据去重列表和从redis中判断时间不符合......' + str(id))

    def get_note(self, note_id, keyword):
        info = random.choice(self.session_id_list)
        # info = self.sid_info
        logger.log(31, 'session_id下标:  ' + str(self.session_id_list.index(info)))

        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        note_ret = self.xhsapi.get_note(note_id)
        # print(333333, note_ret)

        if '参数错误' in note_ret:
            logger.log(31, '参数错误，重试.....')
            self.get_note(note_id, keyword)
            return
        # print(response.text)
        # if '"result":0' in response.text and 'msg:' in response.text:
        #     logger.log(31, '无效id：', info)
        #     del self.session_id_list[self.session_id_list.index(info)]
        #     return
        if '{"msg":"","result":0,"success":true}' in note_ret:
            self.session_id_error.append(info)
            if self.session_id_error.count(info) > 5:
                logger.log(31, '无效id：' + str(info))
                # del self.session_id_list[self.session_id_list.index(info)]
            if self.error_count > 5:
                self.change_ip()
                self.error_count = 0
            self.error_count += 1
            self.get_note(note_id, keyword)
            return

        json_text = json.loads(note_ret)
        print(11111, json_text)
        data = json_text["data"][0]['note_list'][0]
        item = {}
        item['platform'] = '小红书'
        # print(222222, data)
        date_all = data['time']
        time_local = time.localtime(float(date_all))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        logger.log(31, "时间:    " + str(dt))
        # # 做时间判断部分---------------
        get_news_time = time.mktime(time.strptime(str(dt).split(' ')[0], "%Y-%m-%d"))
        end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
        if self.start_time != '':
            start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
        else:
            start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
        if float(get_news_time) < float(start_time):
            logger.log(31, '不符合时间')
            self.redis_example.sadd('xiaohongshu_keyword_have_user_info', note_id)

        elif float(start_time) <= float(get_news_time) <= float(end_time):

            # print('符合时间')
            news_date = dt.split(' ')[0]
            news_time = dt.split(' ')[1]
            item['date'] = news_date
            item['time'] = news_time
            title = data['share_info']["title"]
            item['title'] = title
            item['content'] = data["desc"]
            note_id = data["id"]
            item['content_id'] = note_id
            item['article_author'] = data["user"]["nickname"]
            item['clicks'] = ''
            item['views'] = data['view_count']
            comments = data["comments_count"]
            item['comments_count'] = comments
            item['likes'] = data["liked_count"]
            item['dislikes'] = ''
            item['keyword'] = keyword
            article_url = data['share_info']["link"]
            item['article_url'] = article_url
            item['series_url'] = ''
            item['list_url'] = ''
            item['article_type'] = ''
            item['article_source'] = ''
            item['insert_time'] = str(datetime.now()).split('.')[0]
            item['update_time'] = str(datetime.now()).split('.')[0]
            item['topic_id'] = note_id
            user_id = data["user"]["id"]
            item['author_id'] = user_id
            item['file_code'] = '28'
            item['reposts_count'] = data['shared_count']
            # print(item)
            # self.write_news_jsonfile(item)
            # self.queue.put(item)
            for i in range(5):
                try:
                    self.get_user_info(user_id, item)
                    break
                except:
                    # self.change_ip()
                    time.sleep(2)
                    logging.error(traceback.format_exc())
            if int(comments) > 0:
                try:
                    self.get_note_comment(note_id, keyword, article_url, news_date, news_time, title)
                except:
                    if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                        self.change_ip()
                    logging.error(traceback.format_exc())
                    try:
                        self.get_note_comment(note_id, keyword, article_url, news_date, news_time, title)
                    except:
                        if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                            self.change_ip()
                        logging.error(traceback.format_exc())

    # @retry(stop_max_attempt_number=2, retry_on_exception=retry_if_key_error)
    def get_note_comment(self, note_id, keyword, article_url, news_date, news_time, title, start='', now_page=1):
        if start:
            response = self.xhsapi.get_note_comments(note_id, 20, start)
        else:
            response = self.xhsapi.get_note_comments(note_id, 20)
        # if '"result":0' in response.text and 'msg:' in response.text:
        #     del self.session_id_list[self.session_id_list.index(s)]
        #     return

        data = json.loads(response)
        # print(data)
        try:
            comment_list = data['data']["comments"]
        except:
            logging.log(31, data)
            logging.error(traceback.format_exc())
            return
        comment_count = data['data']["comment_count_l1"]

        last_comment_id = ''
        # total_item = ''
        for comment in comment_list:
            item = {}
            item['platform'] = '小红书'
            item['source_date'] = news_date
            item['source_time'] = news_time
            date_all = comment['time']
            # #转换成localtime
            time_local = time.localtime(float(date_all))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            comment_date = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
            # # 做时间判断部分---------------
            # get_news_time = time.mktime(time.strptime(str(comment_date), "%Y-%m-%d %H:%M"))
            # # end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
            # if self.start_time != '':
            #     start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
            # else:
            #     start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
            # if float(get_news_time) < float(start_time):
            #     self.is_work = False
            #     return
            #
            # if float(start_time) <= float(get_news_time):

            get_news_time = time.mktime(time.strptime(str(comment_date).split(' ')[0], "%Y-%m-%d"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                self.is_get_comment = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                break
            elif float(start_time) <= float(get_news_time) <= float(end_time):
                item['date'] = comment_date.split(' ')[0]
                item['time'] = comment_date.split(' ')[1]
                item['title'] = title
                item['author'] = comment['user']["nickname"]
                item['author_id'] = comment['user']["userid"]
                item['content'] = comment["content"]
                comment_id = comment["id"]
                last_comment_id = comment_id
                item['content_id'] = comment_id
                item['floor'] = ''
                item['keyword'] = keyword
                item['source_url'] = article_url
                item['comment_url'] = article_url
                item['views'] = ''
                item['comments_count'] = ''
                item['likes'] = comment["like_count"]

                item['dislikes'] = ''
                item['insert_time'] = str(datetime.now()).split('.')[0]
                item['update_time'] = str(datetime.now()).split('.')[0]
                item['topic_id'] = note_id
                item['file_code'] = '42'
                item['reposts_count'] = ''
                # print(item)
                # print(11111111, item)
                # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                # total_item = total_item + item
                self.comment_queue.put(item)
        # self.write_comment_jsonfile(total_item)
        # self.comment_queue.put

        # print(last_comment_id)
        all_page_num = math.ceil(float(int(comment_count)/20))
        if int(all_page_num) > now_page and self.is_work:
            now_page += 1
            time.sleep(0.1)
            try:
                self.get_note_comment(note_id, keyword, article_url, news_date, news_time, title, last_comment_id, now_page)
            except:
                try:
                    self.get_note_comment(note_id, keyword, article_url, news_date, news_time, title, last_comment_id, now_page)
                except:
                    pass

    def get_user_info(self, user_id, item={}):
        info = random.choice(self.session_id_list)
        # info = self.sid_info
        print(self.session_id_list.index(info))
        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        # response = requests.get(url, params=parms, headers=headers)
        user_info = self.xhsapi.get_user_info(user_id)
        # print(1111, user_info)
        if '参数错误' in user_info:
            logger.log(31, '参数错误，重试.....')
            self.get_user_info(user_id, item)
            return
        # if '"result":0' in response.text and 'msg:' in response.text:
        #     logger.log(31, '无效id：', info)
        #     del self.session_id_list[self.session_id_list.index(info)]
        #     return
        if '{"msg":"","result":0,"success":true}' in user_info:
            self.change_ip()
            self.get_user_info(user_id, item)
            return
        data = json.loads(user_info)['data']
        user_info_item = dict()

        user_info_item['user_id'] = user_id

        user_info_item['user_name'] = data['nickname']  # 用户名
        user_info_item['red_id'] = data['red_id']  # 小红书号
        user_info_item['location'] = data['location']  # 位置
        try:
            user_info_item['desc'] = data['desc'] if data['desc'] else '' # 简介
        except:
            user_info_item['desc'] = ''
        user_info_item['follows'] = data['follows']  # 关注
        user_info_item['fans'] = data['fans']  # 粉丝
        user_info_item['collected'] = data['collected']  # 被收藏数
        user_info_item['liked'] = data['liked']  # 被点赞数
        user_info_item['gender'] = '女' if str(data['gender']) == '1' else '男'  # 性别
        user_info_item['collected_notes_num'] = data['collected_notes_num']  # 收藏笔记数
        user_info_item['collected_tags_num'] = data['collected_tags_num']  # 收藏标签数
        user_info_item['notes'] = data['ndiscovery']  # 笔记数
        user_info_item['profile_url'] = data['share_link']  # 个人主页
        user_info_item['notes'] = data['ndiscovery']  # 笔记数
        user_info_item['level_number'] = data['level']['number']  # 等级数
        try:
            user_info_item['level_name'] = data['level']['level_name']  # 等级名称
        except:
            user_info_item['level_name'] = ''
            # print(user_info_item)
        item.update(user_info_item)
        # print(item)
        self.queue.put(item)

    # # 写入json文件
    # def write_news_jsonfile(self, item):
    #     logger.log(31, '写入文章数据，')
    #     item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    #     # with open('./28_{}_xiaohongshu_article.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
    #     #     f.write(item.encode("utf-8"))
    #     try:
    #         self.hdfsclient.new_write('{}/{}/28_{}_{}_xiaohongshu_article_2.json'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')
    #     except urllib3.exceptions.NewConnectionError:
    #         self.write_news_jsonfile(item)
    #
    #
    # def write_comment_jsonfile(self, item):
    #     logger.log(31, '写入评论数据')
    #     # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    #     # with open('./42_{}_xiaohongshu_comment.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
    #     #     f.write(item.encode("utf-8"))
    #     try:
    #         self.hdfsclient.new_write('{}/{}/42_{}_{}_xiaohongshu_comment_2.json'.format(self.comment_apth, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')
    #     except urllib3.exceptions.NewConnectionError:
    #         self.write_comment_jsonfile(item)

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

    def run_pool(self, queue, comment_queue, keyword):
        try:

            self.queue = queue
            self.comment_queue = comment_queue
            self.is_work = True
            # try:
            self.tiezi_list = []
            logger.log(31, '关键词：' + keyword)
            for i in range(24, 50):
                logger.log(31, '获取搜索列表页: {}'.format(i))
                try:
                    self.get_serach_list(i, keyword)
                except:
                    logger.error(traceback.format_exc())

            # self.get_note('5ce2a1ea0000000006016cd9')
            #
            # self.get_comment('5ce2a1ea0000000006016cd9', 20)
        except:
            # print(traceback.format_exc())
            logger.critical(traceback.format_exc())

        # self.get_note('5e0721340000000001005602', '')



file_path = '/user/cspider_daily/nike_daily/article'
comment_path = '/user/cspider_daily/nike_daily/articlecomments'
# spider = Spider(file_path, comment_path)
# spider.run()
hdfsclient = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
time_time = str(time.time()).split('.')[0]

def run(queue, comment_queue, keyword):
    spider = Spider()
    try:
        spider.run_pool(queue, comment_queue, keyword)
    except:
        logger.log(31, traceback.format_exc())


def write_news(queue):
    # print(3)
    # for i in range(100000):
    #     item = queue.get(timeout=600)
    #     print('写入数据中：', item)
    #     item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    #     with open('./../zhihu/47_{}_zhihu.json'.format(str(now_time)), 'ab') as f:
    #         f.write(item.encode("utf-8"))

    zero_num = 0
    while True:
        q_size = queue.qsize()
        total_item = ''
        if q_size > 0:
            for i in range(q_size):
                item = queue.get(timeout=600)
                # print('写入数据中......')
                item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                total_item += item
            # print('写入数据中......')
            # item = queue.get(timeout=600)
            # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
            with open('./28_{}_xiaohongshu_article.json'.format(str(datetime.now()).split(' ')[0]), 'ab') as f:
                f.write(total_item.encode("utf-8"))
            # try:
            #     hdfsclient.new_write('{}/{}/28_{}_{}_xiaohongshu_article.json'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), time_time), total_item,encoding='utf-8')
            # except:
            #     try:
            #         hdfsclient.new_write('{}/{}/28_{}_{}_xiaohongshu_article.json'.format(file_path,str(datetime.now()).split(' ')[0].replace('-', ''),str(datetime.now()).split(' ')[0].replace('-', '_'),time_time), total_item,encoding='utf-8')
            #     except:
            #         logging.error(traceback.format_exc())
            logger.log(31, '文章数据.....')
            # print(total_item)
            zero_num = 0
        else:
            zero_num += 1
        time.sleep(5)
        if zero_num > 120:
            logger.log(31, '文章队列中数据写入完毕......')
            break

def write_comment(comment_queue):
    zero_num = 0
    while True:
        q_size = comment_queue.qsize()
        total_item = ''
        if q_size > 0:
            for i in range(q_size):
                item = comment_queue.get(timeout=600)
                # print('写入数据中......')
                item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                total_item += item
            # print('写入数据中......')
            # item = queue.get(timeout=600)
            # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
            with open('./42_{}_xiaohongshu_comment.json'.format(str(datetime.now()).split(' ')[0]), 'ab') as f:
                f.write(total_item.encode("utf-8"))

            # try:
            #     hdfsclient.new_write('{}/{}/42_{}_{}_xiaohongshu_comment.json'.format(comment_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), time_time), total_item,encoding='utf-8')
            # except:
            #     try:
            #         hdfsclient.new_write('{}/{}/42_{}_{}_xiaohongshu_comment.json'.format(comment_path,str(datetime.now()).split(' ')[0].replace('-', ''),str(datetime.now()).split(' ')[0].replace('-', '_'),time_time), total_item,encoding='utf-8')
            #     except:
            #         logging.error(traceback.format_exc())
            logger.log(31, '评论数据......')
            # print(total_item)
            zero_num = 0
        else:
            zero_num += 1
        time.sleep(5)
        if zero_num > 120:
            logger.log(31, '评论队列中数据写入完毕......')
            break


if __name__ == "__main__":
    print(sys.argv)
    # file_path = sys.argv[1]
    # comment_path = sys.argv[2]

    hdfsclient.makedirs('{}/{}'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
    hdfsclient.makedirs('{}/{}'.format(comment_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
    pool = multiprocessing.Pool(3)
    queue = multiprocessing.Manager().Queue()
    comment_queue = multiprocessing.Manager().Queue()
    pool.apply_async(write_news, args=(queue,))
    pool.apply_async(write_comment, args=(comment_queue,))
    # url_list = get_config_para('nike_daily_keywords')
    url_list = ['粥']
    print(url_list)
    for item in url_list:
        # print(1)
        # keyword = item['keywords']
        keyword = item
        logger.log(31, keyword)
        pool.apply_async(run, args=(queue, comment_queue, keyword))
    pool.close()
    # pool.join()
    # a = Process(target=run, args=(file_path, comment_path, ))
    # a.start()

    start_time = time.time()
    while True:
        # print(1111)
        if (float(time.time()) - float(start_time)) > 28800:
            logger.log(31, u'爬取时间已经达到8小时，结束进程任务')
            break
        time.sleep(1)
    logger.log(31, '程序结束......')


    # spder = Spider()
    # spder.get_user_info('5a16311de8ac2b349577ec8e')

