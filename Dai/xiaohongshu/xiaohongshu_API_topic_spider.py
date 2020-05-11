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
from xhsapi import XhsApi
import math
import random
import xlrd
import sys
import urllib3
from session_id_list_topic import seeeion_id_list
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
# from config_para import get_config_para
# import redis


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

class MutilProcess(object):
    """
    进程池
    """

    def __init__(self):
        pass

    def multi_process(self, processes_num, cols_list, Spider):
        pool = multiprocessing.Pool(processes=processes_num)
        for keyword in cols_list:
            spider = Spider(keyword)
            pool.apply_async(spider.run)
        pool.close()
        pool.join()

class Spider(object):
    """
    这是一个爬虫模板
    """
    def __init__(self, file_path, comment_path):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''
        # # 时间判断部分
        date = datetime.now() - timedelta(days=2)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        # print('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        #
        # logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        #
        # # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # self.start_time = '2010-03-20'
        # # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # self.end_time = '2019-12-09 14:08'
        # # 标记爬虫工作

        # get_now_time = time.time() - 86400  # 一天或者三小时 的秒数
        # # get_now_time = time.time() - 8640000  # 一百天
        # print(get_now_time)
        # time_local = time.localtime(float(get_now_time))
        # # 转换成新的时间格式(2016-05-05 20:28:54)
        # dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # print(dt)
        # end_t = time.time()
        # print(end_t)
        # time_local = time.localtime(float(end_t))
        # # 转换成新的时间格式(2016-05-05 20:28:54)
        # end_dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # print(end_dt)
        # # end_time = str(end_time).split(' ')[0]
        # print('爬取时间段：{}到{}'.format(dt, end_dt))

        # logging.info('爬取时间段：{}到{}'.format(dt, str(datetime.now())))
        # 定义开始时间 y-m-d  离现在时间远
        # self.start_time = dt
        # self.start_time = '2019-09-09 00:22'
        # 定义结束时间 y-m-d  离现在时间近
        # self.end_time = end_dt
        # self.end_time = '2019-09-16 10:22'

        self.is_work = True

        self.xhsapi = XhsApi('8ac1d719cd0a2d16')
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
        self.set_list = []
        self.info = seeeion_id_list
        # try:
        #     os.mkdir('./json_file/{}'.format(str(datetime.now()).split(' ')[0]))
        # except:
        #     pass

        with open('./session_id_list_topic.json') as f:
            session_id = f.read()

        self.session_id_list = eval(session_id)
        self.session_id_error = []
        self.error_count = 0
        self.file_path = file_path
        self.comment_path = comment_path
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('{}/{}'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}'.format(self.comment_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]


    # def get_session_id(self):
    #     register_smid_ret = self.xhsapi.register_smid_proxy(self.ip)
    #     print('register_smid_ret:' + register_smid_ret)
    #     smid = json.loads(register_smid_ret)['detail']['deviceId']
    #     print('smid:' + smid)
    #     self.xhsapi.set_smid(smid)
    #     # 激活用户
    #     active_user_ret = self.xhsapi.active_user_proxy(self.ip)
    #     print('active_user_ret:' + active_user_ret)
    #     # 设置session id
    #     session_id = json.loads(active_user_ret)['data']['session']
    #     print('session_id:' + session_id)
    #     item = {
    #         'deviceId': "abbd5bf5-3a82-3fcd-b8b8-4e4c48f68950",
    #         'device_fingerprint': "201908191457046c8b8bd154ae84d8f7c9f8e912c573870183341147f781ee",
    #         'device_fingerprint1': "201908191457046c8b8bd154ae84d8f7c9f8e912c573870183341147f781ee",
    #         'sid': "session.1566198308579055731492",
    #         'search_id': "A9F65F9019EF946464D38BF16C0E250A",
    #     }
    #     item['device_fingerprint'] = smid
    #     item['device_fingerprint1'] = smid
    #     item['sid'] = "session." + session_id
    #     print(item)

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


    def res_ip(self):
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
        import time
        time.sleep(3)
        url = 'http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=310000&city=0&yys=0&port=1&time=2&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
        ip_pro = requests.get(url, headers=headers)
        # print(ip_pro.text)
        # ip_data = json.loads(ip_pro.text)
        ip = ip_pro.text.strip()

        # ip = str(ip_data['data'][0]['ip']) + ':' + str(ip_data['data'][0]['port'])
        return ip

    def get_serach_list(self, page, keyword):
        info = random.choice(self.session_id_list)
        # info = self.sid_info
        print(self.session_id_list.index(info))
        parms = {
            'keyword': keyword,
            'platform': 'android',
            'filters': '',
            # 'sort': '',  # 综合 排序
            # 'sort': 'popularity_descending',  # 最热 排序
            'sort': 'time_descending',  # 最新 排序
            'page': page,
            'page_size': '20',
            'source': 'explore_feed',
            # 'search_id': info['search_id'],
            'api_extra': '',
            'deviceId': info['deviceId'],
            'device_fingerprint': info['device_fingerprint'],
            'device_fingerprint1': info['device_fingerprint1'],
            'versionName': '5.35.1',
            'channel': 'YingYongBao',
            'sid': info['sid'],
            'lang': 'zh',
            't': str(round(time.time())),
        }

        url = 'https://www.xiaohongshu.com/api/sns/v9/search/notes'
        for i in range(10):
            res = self.xhsapi.get_sign(url, parms)
            print(1111, res)
            if len(res['shield']) == 32:
                break
        res = self.xhsapi.get_sign(url, parms)
        print(res['sign'])
        parms['sign'] = res['sign']
        headers = {
            # 'authorization': info['sid'],
            # 'device_id': info['deviceId'],
            'user-agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0; DIG-AL00 Build/HUAWEIDIG-AL00) Resolution/720*1280 Version/6.8.0.3 Build/6080103 Device/(HUAWEI;DIG-AL00) NetType/WiFi',
            'shield': res['shield'],
            'Host': 'www.xiaohongshu.com',
            'accept-encoding': 'gzip',
            'Connection': 'Keep-Alive',
        }

        response = requests.get(url, params=parms, headers=headers)
        print(response.url)
        if '"result":0' in response.text and 'msg:' in response.text:
            del self.session_id_list[self.session_id_list.index(info)]
            return
        json_text = json.loads(response.text)
        print(json_text)
        note_list = json_text["data"]["notes"]
        for note in note_list:
            title = note["title"]
            if not title:
                title = note["desc"]
            id = note["id"]
            print(title)
            time.sleep(0.1)
            if id not in self.set_list:

                try:
                    self.get_note(id, keyword)
                except:
                    print(traceback.format_exc())
                    try:
                        self.get_note(id, keyword)
                    except:
                        print(traceback.format_exc())
                self.set_list.append(id)

    def get_note(self, note_id, keyword, index=0):
        info = random.choice(self.info)
        # info = self.sid_info
        logger.log(31, 'session_id下标:  ' + str(self.info.index(info)))

        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        note_ret = self.xhsapi.get_note(note_id)
        # print(333333, note_ret)

        if '参数错误' in note_ret:
            logger.log(31, '参数错误，重试.....')
            self.get_note(note_id, keyword, index)
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
                # del self.info[self.info.index(info)]
            if self.error_count > 5:
                self.change_ip()
                self.error_count = 0
            self.error_count += 1
            self.get_note(note_id, keyword, index)
            return

        json_text = json.loads(note_ret)
        # print(11111, json_text)
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
        if float(get_news_time) < float(start_time) and index > 1:
            logger.log(31, '不符合时间')
            # self.redis_example.sadd('xiaohongshu_out_day_url', note_id)
            self.is_work = False
            return
        elif float(start_time) <= float(get_news_time) <= float(end_time):

            logging.log(31, '符合时间')
            news_date = dt.split(' ')[0]
            news_time = dt.split(' ')[1]
            item['date'] = news_date
            item['time'] = news_time
            title = data['share_info']["title"]
            item['title'] = title
            item['content'] = data["desc"] + '#今日份AJ女生'
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
            item['author_id'] = data["user"]["id"]
            item['file_code'] = '28'
            item['reposts_count'] = data['shared_count']
            if data['topics']:
                item['topic'] = data['topics'][0]['name']
                item['get_topic_id'] = data['topics'][0]['id']
                item['get_topic_url'] = data['topics'][0]['link']
            else:
                item['topic'] = ''
                item['get_topic_id'] = ''
                item['get_topic_url'] = ''
            # if '韩束' not in item['title'] and '韩束' not in item['content']:
            #     print('检索文章没有包含关键词，判定不符合数据......')
            #     return
            # else:
            #     print('符合检索关键词的文章......')
            #     print(item)
            logging.log(31, item)
            self.write_news_jsonfile(item)
            # self.queue.put(item)
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
                # break
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
                # self.comment_queue.put(item)
                self.write_comment_jsonfile(item)
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

    def get_user(self, user_id, page, num):
        info = random.choice(self.info)
        # info = self.sid_info
        print(self.info.index(info))
        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        # response = requests.get(url, params=parms, headers=headers)
        note_ret = self.xhsapi.get_user_note(user_id, page, num)
        print(1111, note_ret)
        if '参数错误' in note_ret:
            logger.log(31, '参数错误，重试.....')
            self.get_user(user_id, page, num)
            return
        # if '"result":0' in response.text and 'msg:' in response.text:
        #     logger.log(31, '无效id：', info)
        #     del self.session_id_list[self.session_id_list.index(info)]
        #     return
        if '{"msg":"","result":0,"success":true}' in note_ret:
            self.change_ip()
        #     self.session_id_error.append(info)
        #     if self.session_id_error.count(info) > 5:
        #         logger.log(31, '无效id：' + str(info))
        #         del self.session_id_list[self.session_id_list.index(info)]
        #     if self.error_count > 5:
        #         self.change_ip()
        #         self.error_count = 0
        #     self.error_count += 1
        #     self.get_user(user_id, page, num)
        #     return
        data = json.loads(note_ret)
        notes = data['data']['notes']
        if not notes:
            with open('uses_id', 'a') as f:
                f.write(user_id + '\n')
        else:
            for index, note in enumerate(notes):
                # item = {}
                # print(note)
                id = note['id']
                if not self.is_work:
                    return
                try:
                    time.sleep(1)
                    self.get_note(id, '', index)
                except:
                    if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                        self.change_ip()
                    logging.error(traceback.format_exc())
                    try:
                        time.sleep(1)
                        self.get_note(id, '', index)
                    except:
                        if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                            self.change_ip()
                        logging.error(traceback.format_exc())
                        try:
                            time.sleep(1)
                            self.get_note(id, '', index)
                        except:
                            if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
                                self.change_ip()
                            logging.error(traceback.format_exc())
                time.sleep(1)


    def get_topic(self, topic_id, page, num):
        """
        get topice info
        :param user_id:
        :param page:
        :param num:
        :return:
        """

        info = random.choice(self.info)
        # info = self.sid_info
        logging.log(31, self.info.index(info))
        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])
        # response = requests.get(url, params=parms, headers=headers)
        note_ret = self.xhsapi.get_topic_notes(topic_id, page, num)
        # logging.log(31, note_ret)
        if '参数错误' in note_ret:
            logger.log(31, '参数错误，重试.....')
            self.get_topic(topic_id, page, num)
            return
        # if '"result":0' in response.text and 'msg:' in response.text:
        #     logger.log(31, '无效id：', info)
        #     del self.session_id_list[self.session_id_list.index(info)]
        #     return
        if '{"msg":"","result":0,"success":true}' in note_ret:
            self.change_ip()
        #     self.session_id_error.append(info)
        #     if self.session_id_error.count(info) > 5:
        #         logger.log(31, '无效id：' + str(info))
        #         del self.session_id_list[self.session_id_list.index(info)]
        #     if self.error_count > 5:
        #         self.change_ip()
        #         self.error_count = 0
        #     self.error_count += 1
            self.get_topic(topic_id, page, num)
            return
        #     return
        data = json.loads(note_ret)
        notes = data['data']['noteList']
        if not notes:
            # with open('uses_id', 'a') as f:
            #     f.write(topic_id + '\n')
            pass
        else:
            for index, note in enumerate(notes):
                # item = {}
                # print(note)
                id = note['id']
                # if not self.is_work:
                #     return
                for i in range(10):
                    try:
                        time.sleep(1)
                        self.get_note(id, '', index)
                        break
                    except:
                        time.sleep(3)
                        logging.error(traceback.format_exc())
                time.sleep(1)


    def get_topic_v2(self, topic_id, page):
        info = random.choice(self.info)
        # info = self.sid_info
        logging.log(31, self.info.index(info))
        self.xhsapi.set_smid(info['device_fingerprint'])
        self.xhsapi.set_session_id(info['sid'].split('.')[-1])

        parms = {

            # 'sort': 'hot',  # 最热 排序
            'sort': 'time',  # 最新 排序
            'page': page,  # 最新 排序
            'pageSize': '6',
            # 'sid': info['sid'],
            'sid': 'session.1570584984409448341951',

        }

        url = 'https://www.xiaohongshu.com/fe_api/burdock/v1/page/{}/notes'.format(topic_id)
        for i in range(10):
            res = self.xhsapi.get_xsign(url, parms)
            # if len(res['shield']) == 32:
            break

        res = self.xhsapi.get_sign(url, parms)
        print(res)
        parms['sign'] = res['sign']
        headers = {
            # 'authorization': info['sid'],
            # 'device_id': info['deviceId'],
            'user-agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0; DIG-AL00 Build/HUAWEIDIG-AL00) Resolution/720*1280 Version/6.8.0.3 Build/6080103 Device/(HUAWEI;DIG-AL00) NetType/WiFi',
            'shield': res['shield'],
            'Host': 'www.xiaohongshu.com',
            'accept-encoding': 'gzip',
            'Connection': 'Keep-Alive',
        }


    # 写入json文件
    def write_news_jsonfile(self, item):
        # print(item)
        logging.log(31, '写入数据')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./json_file/{}/28_{}_xiaohongshu_article_topic_time.json'.format(str(datetime.now()).split(' ')[0], str(datetime.now()).split(' ')[0]), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        try:
            self.hdfsclient.new_write('{}/{}/28_{}_{}_xiaohongshu_article_topic_time.json'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')
        except urllib3.exceptions.NewConnectionError:
            self.write_news_jsonfile(item)

    def write_comment_jsonfile(self, item):
        # print(item)
        logging.log(31, '写入评论')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./json_file/{}/42_{}_xiaohongshu_comment_topic_time.json'.format(str(datetime.now()).split(' ')[0], str(datetime.now()).split(' ')[0]), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        try:
            self.hdfsclient.new_write('{}/{}/42_{}_{}_xiaohongshu_comment_topic_time.json'.format(self.comment_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')
        except urllib3.exceptions.NewConnectionError:
            self.write_comment_jsonfile(item)

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
        # excel_file = xlrd.open_workbook(r'./韩束小红书投放.xlsx')
        # excel_form = excel_file.sheet_by_name('KOC')
        # for i in range(2, 102):
        #     rows = excel_form.row_values(i)
        #     print(rows)
        #     name = rows[2]
        #     user_url = rows[3]
        #     flows = rows[4]
        #     likes = rows[5]
        #     direction = rows[6]
        #     date_time = rows[7]
        #     print(user_url)
        #     print(date_time)
        #     user_id = user_url.split('/')[-1].split('?')[0]
        #     self.is_work = True
        #     self.tiezi_list = []
        #     print(user_id)
        #     if len(str(date_time)) > 5:
        #         date_time = str(date_time)[:4]
        #     get_date = '2020-' + str(date_time).replace('.', '-')
        #     print(get_date)
        #     # str_time = time.mktime(time.strptime(get_date, "%Y-%m-%d"))
        #     # print(str_time)
        #     # self.start_time = get_date
        #     # self.end_time = get_date
        #     for i in range(1, 400):
        #         if not self.is_work:
        #             break
        #         try:
        #             time.sleep(1)
        #             self.get_topic(user_id, i, '10')
        #         except:
        #             if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
        #                 self.change_ip()
        #             try:
        #                 time.sleep(1)
        #                 self.get_topic(user_id, i, '10')
        #             except:
        #                 if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
        #                     self.change_ip()
        #                 try:
        #                     time.sleep(1)
        #                     self.get_topic(user_id, i, '10')
        #                 except:
        #                     if '503 Service Temporarily' in traceback.format_exc() or 'requests.exceptions.SSLError' in traceback.format_exc():
        #                         self.change_ip()
        #                     print(traceback.format_exc())
        #         time.sleep(1)

                # self.get_note('5ce2a1ea0000000006016cd9')
                #
                # self.get_comment('5ce2a1ea0000000006016cd9', 20)
        for i in range(1, 400):

            logging.log(31, '主贴翻页：'+ str(i))
            if not self.is_work and i > 3:
                break
            for j in range(10):
                try:
                    self.get_topic('5e60bd92dd0a2a00013fe218', i, 6)
                    break
                except:
                    self.change_ip()
                    logging.error(traceback.format_exc())

        # self.get_topic_v2('5e60bd92dd0a2a00013fe218', 1)


if __name__ == "__main__":
    file_path = '/user/cspider_daily/nike_daily/article'
    comment_path = '/user/cspider_daily/nike_daily/articlecomments'
    spider = Spider(file_path, comment_path)
    spider.run()
    # pool = multiprocessing.Pool(1)
    #
    # pool.apply_async(spider.run)
    # pool.close()
    # start_time = time.time()
    # while True:
    #     if (float(time.time()) - float(start_time)) > 28800:
    #         logger.log(31, u'爬取时间已经达到八小时，结束进程任务')
    #         break
    #     time.sleep(1)
    # # logger.log(31, '程序结束......')
    #
    # spider.run()