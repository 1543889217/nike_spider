import requests
from lxml import etree
import json
import time
from datetime import datetime
from datetime import timedelta
import xlrd
import logging
import traceback
import random
import multiprocessing
import os
import sys
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
from config_para import get_config_para



# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./../zhihu/zhihu-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')


class ZhiHuSpider(object):
    """
    知乎爬虫，根据关键字进行搜索，爬取一周内的信息
    """
    def __init__(self, all_set_list):

        self.headers_one = {

        }

        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''
        # # 打开json文件
        # self.news_jsonfile = open('./sina_newsfile.json', 'wb')
        # self.comment_jsonfile = open('./sina_commentfile.json', 'wb')

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

        logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # 标记爬虫工作
        self.is_work = True
        self.is_stop = False
        # 翻页计数
        self.page_count = 0
        # 楼层计数
        self.floor_num = 1

        # 去重列表
        self.set_list = all_set_list

        # self.ip = proxies.res_ip()
        # self.ip = {'https':'113.133.33.252:4659'}
        # print([str(self.ip)])
        self.user_agent = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        ]
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

        self.queue = ''

    # 获取知乎列表页
    def get_questions_list_page(self, url, params, keyword):
        """
        知乎搜索出来的列表页，其中包含问答类信息和文章类信息，所以在函数中页做出了适当的判断
        :param url:
        :param params: 参数
        :return:
        """

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'cookie': '_zap=c7a27497-1e76-439f-8355-d5d4d2b59dfc; _xsrf=7042c0fb-7e8f-432a-8ad6-717f8fd10681; d_c0="ANDTuimCGBGPTnGgyKmfKZmygaq2sV-fz3w=|1586482510"; _ga=GA1.2.790586184.1586482509; _gid=GA1.2.490516832.1586482509; _gat_gtag_UA_149949619_1=1; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1586482510; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1586482510; KLBRSID=d1f07ca9b929274b65d830a00cbd719a|1586482512|1586482510; capsion_ticket="2|1:0|10:1586482512|14:capsion_ticket|44:ZWJkZDIxNzMxODU2NGUxYmJmOTg1YjVjYmE4MjIyMjA=|cb519ee23c35ef6511f4bfbe72fd4cbd5d347e118d79aa34f0b9902f9f34fc8d"',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
            # 'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        # print({'https': self.ip.strip()})
        try:
            response = requests.get(url, headers=headers, params=params, proxies=self.proxies, timeout=120)
        except requests.exceptions.ProxyError:
            self.get_questions_list_page(url, params, keyword)
            return
        logger.log(31, '正在抓取主链接:  '+ response.url)
        if response.text != None:
            data = response.content.decode()
            data = json.loads(data)
            if data['data']:  # 判断获取的json数据中的data['data']的value列表是否为空，可以间接判断是否还有下一页数据
                if len(data['data']) > 1:
                    data_list = data['data'][1:]
                else:
                    data_list = data['data']
                for news in data_list:
                    try:
                        question_title = news['highlight']['title'].replace('<em>', '').replace('</em>', '')
                        news_type = news['object']['type']
                        # 时间判断

                        if news_type == 'answer':  # 问答类信息
                            answers_url = news['object']['url']
                            question_url = news['object']['question']['url']
                            try:
                                topic_time_all = self.get_topic_time(question_url)
                            except:
                                continue
                            # topic_time_all = '2019-03-04 01:03:30'
                            question_id = question_url.split('/')[-1]
                            view_url = 'https://www.zhihu.com/question/' + question_id
                            views = self.get_view(view_url)  # 获取浏览量
                            url = 'https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&sort_by=created'.format(question_id, '0')
                            # 传入页面的url
                            source_url = 'https://www.zhihu.com/question/{}/answers/created'.format(str(question_id))
                            if url not in self.set_list:  # 对url进行简单的去重，避免重复的工作量

                                self.get_answers_page(url, question_title, source_url, keyword, views, topic_time_all)

                                self.set_list.append(url)
                        elif news_type == 'article':  # 文章类信息

                            item = {}
                            content = news['object']['content']
                            # item['type'] = '文章'
                            item['platform'] = '知乎'
                            crt_time = news['object']['created_time']
                            # #转换成localtime
                            time_local = time.localtime(float(crt_time))
                            # 转换成新的时间格式(2016-05-05 20:28:54)
                            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                            date = dt.split(' ')[0]
                            news_time = dt.split(' ')[1]
                            item['date'] = date
                            item['time'] = news_time
                            author = news['object']['author']['name']
                            item['author'] = author.replace('<em>', '')
                            item['title'] = question_title
                            # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
                            content = etree.HTML(content)
                            content = content.xpath('.//p//text()')
                            content = ''.join(content)
                            item['content'] = content
                            articles_url = news['object']['url'].split('/')[-1]
                            item['url'] = 'https://zhuanlan.zhihu.com/p/{}'.format(str(articles_url))
                            item['is_topics'] = '是'
                            item['floor'] = 0
                            item['keyword'] = keyword
                            comments_count = news['object']['comment_count']
                            item['comments_count'] = comments_count
                            item['views'] = ''
                            likes = news['object']['voteup_count']
                            item['likes'] = str(likes)
                            topic_id = articles_url
                            item['topic_id'] = topic_id
                            item['author_id'] = news['object']['author']['id']
                            item['topic_date'] = date
                            item['topic_time'] = news_time
                            item['content_id'] = topic_id
                            item['reposts_count'] = ''
                            item['file_code'] = '47'

                            # 做时间判断部分---------------  这个部分区分于另外一个部分
                            get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
                            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
                            if self.start_time != '':
                                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
                            else:
                                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
                            if float(get_news_time) < float(start_time):
                                pass
                            if float(start_time) <= float(get_news_time) <= float(end_time):
                                # print('爬取正文数据中.....')
                                # print(item)
                                # self.write_news_jsonfile(item)
                                if topic_id in self.set_list:
                                    return
                                else:
                                    self.set_list.append(topic_id)

                                    self.queue.put(item)
                                # print(self.queue.get())
                                if int(comments_count) > 0:
                                    comment_id = news['object']['id']
                                    comment_url = 'https://www.zhihu.com/api/v4/articles/{}/root_comments?include=data%5B*%5D.author%2Ccollapsed%2Creply_to_author%2Cdisliked%2Ccontent%2Cvoting%2Cvote_count%2Cis_parent_author%2Cis_author&order=normal&limit=20&offset=0&status=open'.format(str(comment_id))
                                    comment_source_url = 'https://zhuanlan.zhihu.com/p/{}'.format(str(comment_id))
                                    self.floor_num = 1
                                    self.get_comment_info(comment_url, question_title, comment_source_url, keyword, topic_id, dt)
                            else:
                                print('数据时间不符合')
                    except:
                        logger.error(traceback.format_exc())

                is_end = data['paging']['is_end']
                if not is_end:
                    next_url = data['paging']['next']
                    self.get_questions_list_page(next_url, params, keyword)

    def get_topic_time(self, url):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies)
        except requests.exceptions.ProxyError:
            self.get_topic_time(url)
            return
        dict_text = json.loads(response.text)
        created_time = dict_text['created']
        time_local = time.localtime(float(created_time))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
        return dt

    def get_view(self, url):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        # print(url)
        response = requests.get(url, headers=headers, proxies=self.proxies, timeout=120)
        data = etree.HTML(response.content.decode())
        views = data.xpath('.//div[@class="QuestionFollowStatus"]/div/div[2]/div/strong/text()')[0]
        # print('浏览量', views)
        return views

    # 获取回答信息
    def get_answers_page(self, url, question_title, source_url, keyword, views, topic_time_all):
        """
        获取问答类的回答列表，其中包含一条条的回答，这些回答可能还有评论，
        :param url:
        :param question_title: 问答的标题
        :param question_id: 问答的id
        :return:
        """
        logger.log(31, '获取回答信息:  ' + url)
        item = {}
        self.is_stop = False
        # accept-encoding': 'gzip, deflate, br' 在开发中携带了这个头信息，出现乱码情况，去掉这个头信息，问题解决
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9',
            'Connection': 'close',
            'cookie': 'tgw_l7_route=e0a07617c1a38385364125951b19eef8; _xsrf=PhxZhhuALHVLP9dntJMOL27yQZx34zUG',
            'upgrade-insecure-requests': '1',
            'user-agent': '{}'.format(random.choice(self.user_agent))
        }
        # url = 'https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&sort_by=created'.format(question_id, offset)
        # print(url)
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies, timeout=120)  # , proxies={'http':'49.79.67.253:7671'}
        except requests.exceptions.ProxyError:
            self.get_answers_page(url, question_title, source_url, keyword, views, topic_time_all)
            return

        data = json.loads(response.content)
        data_list = data['data']
        for news in data_list:
            # print(1111111111111, news)
            # item['type'] = '回答'
            item['platform'] = '知乎'
            crt_time = news['created_time']
            # #转换成localtime
            time_local = time.localtime(float(crt_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            date = dt.split(' ')[0]
            news_time = dt.split(' ')[1]
            item['date'] = date
            item['time'] = news_time
            author = news['author']['name']
            item['author'] = author.replace('<em>', '').replace('</em>', '')
            item['title'] = question_title
            # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
            content = news['content']
            content = etree.HTML(content)
            content = content.xpath('.//p//text()')
            content = ''.join(content)
            item['content'] = content
            source_id = url.split('/')[-2]
            topic_id = str(news['id'])  # 主贴id
            answer_url = 'https://www.zhihu.com/question/{}/answer/{}'.format(source_id, topic_id)
            item['url'] = answer_url
            item['is_topics'] = '是'
            item['floor'] = 0
            item['keyword'] = keyword
            comments_count = news['comment_count']
            item['comments_count'] = comments_count
            item['views'] = views
            likes = news['voteup_count']
            item['likes'] = str(likes)
            # topic_id = str(news['id'])  # 主贴id
            item['topic_id'] = topic_id
            item['author_id'] = news['author']['id']
            item['topic_date'] = topic_time_all.split(' ')[0]
            item['topic_time'] = topic_time_all.split(' ')[1]
            item['content_id'] = news['id']
            item['reposts_count'] = ''
            item['file_code'] = '47'
            # 做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                self.is_stop = True  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                break

            if float(start_time) <= float(get_news_time) <= float(end_time):
                if news['id'] in self.set_list:
                    continue
                else:
                    self.set_list.append(news['id'])
                    self.queue.put(item)
                # self.write_news_jsonfile(item)
                comment_id = news['id']
                if int(comments_count) > 0:  # 获取评论信息
                    comment_url = 'https://www.zhihu.com/api/v4/answers/{}/root_comments?include=data%5B*%5D.author%2Ccollapsed%2Creply_to_author%2Cdisliked%2Ccontent%2Cvoting%2Cvote_count%2Cis_parent_author%2Cis_author&order=normal&limit=20&offset=0&status=open'.format(str(comment_id))
                    self.floor_num = 1
                    logger.info('写入评论中')
                    self.get_comment_info(comment_url, question_title, answer_url, keyword, topic_id, topic_time_all)
            else:
                # print('数据时间不符合')
                # logger.info('数据时间不符合')
                pass
        if not self.is_stop:  # 当此次爬取标记为stop时，就不再执行翻页操作
            is_end = data['paging']['is_end']
            if not is_end:  # 判断是否有下一页数据
                next_page_url = data['paging']['next']
                self.get_answers_page(next_page_url, question_title, source_url, keyword, views, topic_time_all)

    def get_comment_info(self, url, question_title, source_url, keyword, topic_id, topic_time_all):
        """
        获取评论信息
        :url:
        :return:
        """
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9',
            'Connection': 'close',
            'upgrade-insecure-requests': '1',
            'user-agent': '{}'.format(random.choice(self.user_agent))
        }
        comment_item = {}
        logger.log(31, '爬取评论数据中:   ' + url)
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies, timeout=30)  # , proxies={'http':'49.79.67.253:7671'}
        except requests.exceptions.ProxyError:
            self.get_comment_info(url, question_title, source_url, keyword, topic_id, topic_time_all)
            return

        status_code = response.status_code
        if str(status_code) == '200':
            data = json.loads(response.content)
            comment_data = data['data']
            for comments in comment_data:
                # print(222222222222, comments)
                # comment_item['type'] = '评论'
                comment_item['platform'] = '知乎'
                crt_time = comments['created_time']
                # #转换成localtime
                time_local = time.localtime(float(crt_time))
                # 转换成新的时间格式(2016-05-05 20:28:54)
                dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                date = dt.split(' ')[0]
                news_time = dt.split(' ')[1]
                comment_item['date'] = date
                comment_item['time'] = news_time
                author = comments['author']['member']['name']
                comment_item['author'] = author.replace('<em>', '')
                comment_item['title'] = question_title
                # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
                content = comments['content']
                content = etree.HTML(content)
                content = content.xpath('.//p//text()')
                content = ''.join(content)
                comment_item['content'] = content
                comment_item['url'] = source_url
                comment_item['is_topics'] = '否'
                comment_item['floor'] = self.floor_num
                self.floor_num += 1
                comment_item['keyword'] = keyword
                comment_item['comments_count'] = 0
                comment_item['views'] = ''
                likes = comments['vote_count']
                comment_item['likes'] = str(likes)
                comment_item['topic_id'] = topic_id
                comment_item['author_id'] = comments['author']['member']['id']
                comment_item['topic_date'] = topic_time_all.split(' ')[0]
                comment_item['topic_time'] = topic_time_all.split(' ')[1]
                comment_item['content_id'] = comments['id']
                comment_item['file_code'] = '47'
                comment_item['reposts_count'] = ''
                # self.write_news_jsonfile(comment_item)
                if comments['id'] in self.set_list:
                    continue
                else:
                    self.set_list.append(comments['id'])
                    self.queue.put(comment_item)
            is_end = data['paging']['is_end']
            if not is_end:
                next_url = data['paging']['next']
                self.get_comment_info(next_url, question_title, source_url, keyword, topic_id, topic_time_all)

    def run_two(self, keyword, queue):
        self.queue = queue

        url = 'https://www.zhihu.com/api/v4/search_v3'
        params = {
            't': 'general',
            'q': keyword,
            'correction': '1',
            'offset': '0',
            'limit': '10',
            'show_all_topics': '0',
            'time_zone': 'a_week',
            # 'time_zone': 'three_months',
            # 'search_hash_id': '044b12bf9c0104be818332b9d4d6045d',
            'vertical_info': '0,0,0,0,0,0,0,0,0,0'
        }
        try:
            self.get_questions_list_page(url, params, keyword)
        except:
            logger.error(traceback.format_exc())

def get_file_name_time():
    a = str(datetime.now())
    hour = a.split(' ')[-1].split(':')[0]
    num = int(hour) / 3
    num = int(num) * 3
    if num == 0:
        num = 24
        a = str(datetime.now() - timedelta(days=1))  # 昨天时间
    num = a.split(' ')[0] + ' ' + str(num)
    return num

file_path = '/user/cspider_daily/nike_2h/qa'
file_name_time = get_file_name_time()
hdfsclient = HdfsClient(url='http://jq-chance-05:9870', user='dpp-executor')
hour = str(datetime.now()).split(' ')[-1].split(':')[0]
if str(hour) != '00':
    two_hour_ago = int(hour) - 2
    if len(str(two_hour_ago)) == 1:
        two_hour_ago = '0' + str(two_hour_ago)
    hour_name = str(two_hour_ago) + '_' + str(hour)
else:
    hour_name = '22_24'
hdfsclient.makedirs('{}/{}/{}'.format(file_path, file_name_time.split(' ')[0].replace('-', ''), hour_name))  # 创建每日文件夹
time_time = str(time.time()).split('.')[0]

def app_run(keyword, queue, all_set_list):
    spider = ZhiHuSpider(all_set_list)
    try:
        spider.run_two(keyword, queue)
    except:
        logging.error(traceback.format_exc())

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
            # with open('./47_{}_zhihu.json'.format(str(now_time)), 'ab') as f:
            #     f.write(total_item.encode("utf-8"))
            try:
                hdfsclient.new_write('{}/{}/{}/47_{}_{}_zhihu.json'.format(file_path, file_name_time.split(' ')[0].replace('-', ''), hour_name, str(datetime.now()).split(' ')[0], time_time).replace('-', '_'), total_item,encoding='utf-8')
            except:
                logging.error(traceback.format_exc())
            zero_num = 0
        else:
            zero_num += 1
        time.sleep(5)
        if zero_num > 120:
            logger.log(31, '队列中数据写入完毕......')
            break


if __name__ == "__main__":
    logger.log(31, sys.argv)
    # file_path = sys.argv[1]




    # excelfile = xlrd.open_workbook(r'./快消采集关键词_0916_v2.xlsx')
    # sheet1 = excelfile.sheet_by_name('商品精简')
    # cols = sheet1.col_values(0)
    # print(cols)
    # cols = cols[1:392]
    # print(cols)
    # print(len(cols))
    all_set_list = []
    url_list = get_config_para('nike_daily_keywords')
    logger.log(31, url_list)
    queue = multiprocessing.Manager().Queue()
    pool = multiprocessing.Pool(processes=10)
    # for i in range(10):
    pool.apply_async(write_news, args=(queue,))
    for item in url_list:
        # print(1)
        keyword = item['keywords']
        logger.log(31, keyword)
        pool.apply_async(app_run, args=(keyword, queue, all_set_list))
        # print(1)
        pool.apply_async(app_run, args=(keyword, queue, all_set_list))
    # print(2)
    pool.close()
    # pool.join()
    # 程序计时，两小时后结束任务
    start_time = time.time()
    while True:
        if (float(time.time()) - float(start_time)) > 7200:
            logger.log(31, u'爬取时间已经达到两小时，结束进程任务')
            # os.system('taskkill /F /pid {}'.format(pid))
            # os.system('kill -9 {}'.format(str(pid)))
            break
        time.sleep(1)
    logger.log(31, '程序结束......')

