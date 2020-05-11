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
import proxies
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import sys
from with_hdfs import HdfsClient

# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
file_name = "./../toutiao/toutiao-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.DEBUG,
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

        a = str(datetime.now())
        hour = a.split(' ')[-1].split(':')[0]
        num = int(hour) / 3
        num = int(num) * 3
        if num == 0:   # 对于凌晨 0 点的判断
            # 时间判断部分
            date = datetime.now() - timedelta(days=3)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        else:
            # 时间判断部分
            date = datetime.now() - timedelta(days=3)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        # 定义开始时间 y-m-d  离现在时间远
        self.start_time = news_start_time
        # self.start_time = '2019-08-01'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = yesterday
        # self.end_time = '2019-08-18'
        print('爬取时间段：{}到{}'.format(self.start_time, self.end_time))

        # 标记爬虫工作
        self.is_work = True
        # 评论页数
        self.comment_page_num = 1
        # 去重列表
        self.set_list = []
        # 搜集问答类网页的列表
        self.questions_list = []
        self.file_name_time = self.get_file_name_time()
        try:
            os.mkdir('./../toutiao/json_file/{}'.format(self.file_name_time.split(' ')[0]))
        except:
            pass
        self.file_path = '/user/cspider_daily/nike_2h/article'
        self.comment_apth = '/user/cspider_daily/nike_2h/articlecomments'
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        hour = str(datetime.now()).split(' ')[-1].split(':')[0]
        if str(hour) != '00':
            two_hour_ago = int(hour) - 2
            if len(str(two_hour_ago)) == 1:
                two_hour_ago = '0' + str(two_hour_ago)
            self.hour_name = str(two_hour_ago) + '_' + str(hour)
        else:
            self.hour_name = '22_24'

        print('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]
        self.old_url_list_file = open('./caijing_old_url_list.json', 'r+')  # 打开文件初始化历史url列表
        self.old_url_data = self.old_url_list_file.readlines()
        self.old_url_list_file.close()  # 关闭上面的文件
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

    def get_news_list(self):
        headers = {
            'accept': 'text/javascript, text/html, application/xml, text/xml, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'cookie': 'tt_webid=6788745577367651847; tt_webid=6788745577367651847; csrftoken=ff7c756b7b50460e8a4af6402acace8d; s_v_web_id=f69d8f0d4f410107faf562fe51d6df0f; ttcid=702c06d86f8c454ba28a9618a0932c9f37; WEATHER_CITY=%E5%8C%97%E4%BA%AC; __tasessionId=cfprtp25g1581993507276; tt_scid=EV90y0GdhUhUqce4eMvpu391tq7JqyNWhTbfjmnBK3XRncbi94SIJjtjXKBXdbZi54d9',
            'referer': 'https://www.toutiao.com/ch/news_car/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.106 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        url = 'https://www.toutiao.com/api/pc/feed/?category=news_finance&utm_source=toutiao&widen=1&max_behot_time=0&max_behot_time_tmp=1581982197&tadrequire=true&as=A1858E146B950DD&cp=5E4B05B08DFDBE1&_signature=Mh.2dgAgEBD0SE.7O.aZWDIft2AAGxbIsHODTNYx0DuGjsUAb4nqZRiQCSSqx9aQvSV034K.hP6GK.mBA-8AhYtnIksTbCVNhgSfGOUkEih01laoZPm1lIMblEnqtPuj0wG'
        response = requests.get(url, headers=headers, proxies=self.proxies)
        json_data = eval(response.text.replace('false', '0').replace('true', '1'))
        # print(json_data)
        data_list = json_data['data']
        for data in data_list:
            news_url = 'https://www.toutiao.com/a' + str(data['item_id'])
            print(news_url)
            if (news_url + '\n') not in self.old_url_data:  # 如果url不在历史数据中
                if news_url != 'https://www.toutiao.com/apc':
                    self.old_url_data.append(url)  # 将新的url添加进判断url列表中， 做去重使用
                    with open('./old_url_list.json', 'a') as f:  # 打开历史数据的url文件，将新的url写入，保存url记录
                        f.write(url + '\n')
                    try:
                        self.get_news_page(news_url)
                    except:
                        logging.error(traceback.format_exc())

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
                # 做时间判断部分---------------
                get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
                end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
                if self.start_time != '':
                    start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
                else:
                    start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))

                if float(start_time) <= float(get_news_time):  # 符合时间段的内容
                    with open('./../toutiao/json_file/{}/{}_comment_url.json'.format(self.file_name_time.split(' ')[0], self.file_name_time.split(' ')[0]), 'a') as f:
                        f.write(url+'\n')

                    print(item)
                    self.write_news_jsonfile(item)
                    self.get_comment_info(url, title, date, create_time)
                else:
                    print(item)
                    print('不符合抓取时间段的文章 URL:{}'.format(url))
                # if float(get_news_time) > float(end_time):
                #     # with open('./../toutiao/{}_url.txt'.format(now_time), 'a') as f:
                #     #     f.write(url + '\n')
                #     pass
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

        # comment_url = 'http://lf.snssdk.com/article/v1/tab_comments/?count=50&item_id={}&group_id={}&offset={}'.format(url_id, url_id, page_id)
        print('评论爬取中......')
        # print(comment_url)
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

    def change_ip(self):
        logger.log(31, '开始切换ip')
        url = 'http://proxy.abuyun.com/switch-ip'
        time.sleep(random.randint(1, 15))
        response = requests.get(url, proxies=self.proxies)
        logger.log(31, '现使用ip：'+ response.text)

    # 写入json文件
    def write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.hdfsclient.new_write('{}/{}/{}/24_{}_{}_toutiao_news.json'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def write_comment_jsonfile(self, item):
        # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.hdfsclient.new_write('{}/{}/{}/38_{}_{}_toutiao_comment.json'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

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
        # logger.info('开始读取url文件,进行新闻爬取')
        # for url in open('./../toutiao/new_url_file.json'):
        #     if url in set_list:
        #         continue
        #     else:
        #         set_list.append(url)
        #     if url:
        #         logger.info('打开new_url_file.json文件，读取要爬取的url')
        #         url = url.strip()
        #         print('一个爬虫正在爬取网址{}'.format(url))
        #         logger.info('一个爬虫正在爬取网址{}'.format(url))
        #         try:
        #             self.get_news_page(url)
        #         except:
        #             print(traceback.format_exc())
        #             try:
        #                 self.get_news_page(url)
        #             except:
        #                 print('错误')
        #     print('一个网址爬虫结束.....')
        # logger.info('爬取完毕......')

        i = 0
        while i < 40:
            try:
                self.get_news_list()
            except:
                self.change_ip()
                print(traceback.format_exc())
            i += 1



if __name__ == "__main__":
    toutiao = TouTiaoSpider()
    toutiao.run()
