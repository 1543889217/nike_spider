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
import urllib.parse
from with_hdfs import HdfsClient

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./log_data/{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
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

user_agent_list = [
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
]


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''

        # 时间部分,按小时抓取
        # 爬虫开始抓取的日期
        date = datetime.now() - timedelta(days=7)
        news_start_time = str(date).split(' ')[0]

        # 爬虫结束的抓取日期
        current_time = datetime.now()  # 当前日期
        current_day = str(current_time).split(' ')[0]

        print('爬取时间段：{}到{}'.format(news_start_time, current_day))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, current_day))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = current_day

        # 标记爬虫工作1
        self.is_break = False
        # 标记爬虫工作2
        self.is_work = False
        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/forum/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹
        self.time_data = str(time.time()).split('.')[0]

    # 替换所有的HTML标签
    def re_html(self, data):
        # 替换抓取数据中的html标签
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub(r'                                -', '', ret1)
            ret3 = re.sub(r'                                                            ', '', ret2)
            ret4 = re.sub(r"hot\(.*\d?','", '', ret3)
            ret5 = re.sub(r'\[', '', ret4)
            ret6 = re.sub(r'\]', '', ret5)
            ret7 = re.sub(r"',", "", ret6)
            ret8 = re.sub(r"'", "", ret7)
            return ret8
        except:
            pass

    # 过滤月销量里面的非数字
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

    def parse_goods_id(self, key_word):
        try:
            # key_word_data = urllib.parse.quote(key_word)
            url = 'http://bbs.dahe.cn/search.php?mod=forum'
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Cookie': 's8hO_404f_saltkey=tvEEW5wV; s8hO_404f_lastvisit=1568680094; s8hO_404f_sid=IHtErs; PHPSESSID=nr01ffrg19e81likscg0lmejb2; __asc=be50d61716d3cda6bb0dc6485ed; __auc=be50d61716d3cda6bb0dc6485ed; Hm_lvt_49fc517ed1175ad0089c07fe695a54c4=1568684010; s8hO_404f_lastact=1568683853%09search.php%09forum; Hm_lpvt_49fc517ed1175ad0089c07fe695a54c4=1568684168',
                'Host': 'bbs.dahe.cn',
                'Origin': 'http://bbs.dahe.cn',
                'Pragma': 'no-cache',
                'Referer': 'http://bbs.dahe.cn/search.php?mod=forum',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            form_data = {
                'formhash': '89e49222',
                'srchtxt': key_word.encode('gbk'),
                'searchsubmit': 'yes'
            }
            try:
                time.sleep(0.2)
                response = requests.post(url=url, headers=headers, data=form_data)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.post(url=url, headers=headers, proxies=proxies, data=form_data)
                except:
                    time.sleep(0.2)
                    response = requests.post(url=url, headers=headers, proxies=proxies, data=form_data)
            response.encoding = 'gbk'
            print(response.url)
            searchid = self.re_not_number(response.url.split('&')[1])
            print('关键词解析对应id是：', searchid)
            is_break = self.is_break
            insert_time = time.strftime('%Y-%m-%d %H:%M:%S')
            url = 'http://bbs.dahe.cn/search.php?mod=forum&searchid={}&orderby=dateline&ascdesc=desc&searchsubmit=yes&page={}'
            # print(url)
            headers = {
                'Content-Type': 'text/html; charset=gbk',
                # 'Cookie': 's8hO_404f_saltkey=T4WK2597; s8hO_404f_lastvisit=1566265382; PHPSESSID=hp8k3kq01k4p4et54us1vljsu7; Hm_lvt_49fc517ed1175ad0089c07fe695a54c4=1566269243; yfx_c_g_u_id_10000033=_ck19082010472216611967379906556; __auc=d9a596fe16cacec003e8f31e310; s8hO_404f_atarget=1; __asc=cbf1082316cb721670e06723157; zycna=tzGXcwYAChsBAXxONRbq5Xoc; yfx_c_g_u_id_10000007=_ck19082210393212688365475513495; yfx_f_l_v_t_10000007=f_t_1566441572262__r_t_1566441572262__v_t_1566441572262__r_c_0; wdcid=0cb840f230762783; s8hO_404f_yy_ad_status=2; yfx_f_l_v_t_10000033=f_t_1566269242659__r_t_1566440515358__v_t_1566442626841__r_c_1; s8hO_404f_st_t=0%7C1566443342%7Ce4370d9ec8f238172511195afa70bf43; s8hO_404f_forum_lastvisit=D_1496_1566440306D_1880_1566440345D_2988_1566443342; s8hO_404f_st_p=0%7C1566443988%7C5efa9cc93f4efcd80a2db1e41de54594; s8hO_404f_visitedfid=261D2988D1889D1006D780D1875D1213D1778D1880D1496; s8hO_404f_viewid=tid_1240948; s8hO_404f_sendmail=1; s8hO_404f_sid=HXxXR3; s8hO_404f_lastact=1566444218%09search.php%09forum; Hm_lpvt_49fc517ed1175ad0089c07fe695a54c4=1566444478',
                'Host': 'bbs.dahe.cn',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response1 = requests.get(url=url.format(searchid, 1), headers=headers, allow_redirects=False)
            except:
                try:
                    time.sleep(0.2)
                    response1 = requests.get(url=url.format(searchid, 1), headers=headers, allow_redirects=False, proxies=proxies)
                except:
                    time.sleep(0.2)
                    response1 = requests.get(url=url.format(searchid, 1), headers=headers, allow_redirects=False, proxies=proxies)
            response1.encoding = 'gbk'
            # print(response.text)
            # 将响应转换成一个element对象
            html = etree.HTML(response1.text)
            # 获取帖子总数
            topic_num = self.re_not_number(self.re_html(html.xpath('//div[@class="sttl mbn"]/h2/em/text()')))
            if int(topic_num) == 0:
                logger.log(31, '*******-------关键词:%s 搜索不到内容-------*******' % key_word)
            else:
                # 获取帖子页数
                pages_num = int(math.ceil(float(int(topic_num) / 40)))
                logger.log(31, '---关键词: %s ,搜到的帖子总数是: %s ,帖子总页数是: %s ---' % (key_word, topic_num, pages_num))
                for i in range(1, int(pages_num)+1):
                    topic_url = url.format(searchid, key_word, i)
                    # logger.log(31, '抓取第%s页数商品数据' % i)
                    try:
                        time.sleep(0.2)
                        response2 = requests.get(url=topic_url, headers=headers, allow_redirects=False)
                    except:
                        try:
                            time.sleep(0.2)
                            response2 = requests.get(url=topic_url, headers=headers, allow_redirects=False, proxies=proxies)
                        except:
                            time.sleep(0.2)
                            response2 = requests.get(url=topic_url, headers=headers, allow_redirects=False, proxies=proxies)
                    # 将响应转换成一个element对象
                    html1 = etree.HTML(response2.text)
                    # 获取帖子列表
                    topic_list = html1.xpath('//div[@class="tl"]/div[2]/ul/li')
                    # print(len(topic_list))
                    da_he_dict = dict()
                    # 遍历帖子列表
                    for data in topic_list:
                        date_time_data = data.xpath('./p[3]/span[1]/text()')[0]
                        # print(date_time_data)
                        date_data_test = date_time_data.split(' ')[0].strip()
                        # print(date_data_test)
                        # 年, 月， 日
                        year_data = date_data_test.split('-')[0]
                        month_test = date_data_test.split('-')[1]
                        day_test = date_data_test.split('-')[2]
                        if len(month_test) == 2:
                            month_data = month_test
                        else:
                            month_data = '0' + month_test
                        if len(day_test) == 2:
                            day_data = day_test
                        else:
                            day_data = '0' + day_test
                        date_data = (year_data + '-' + month_data + '-' + day_data).strip()
                        time_data = (date_time_data.split(' ')[1] + ':00').strip()
                        if self.start_time <= date_data:
                            da_he_dict['platform'] = '大河网'
                            da_he_dict['source_date'] = date_data
                            da_he_dict['source_time'] = time_data
                            da_he_dict['date'] = date_data
                            da_he_dict['time'] = time_data
                            da_he_dict['insert_time'] = insert_time
                            da_he_dict['author'] = data.xpath('./p[3]/span[2]/a/text()')[0]
                            da_he_dict['author_url'] = 'http://bbs.dahe.cn/' + data.xpath('./p[3]/span[2]/a/@href')[0]
                            da_he_dict['author_id'] = self.re_not_number(data.xpath('./p[3]/span[2]/a/@href')[0])
                            da_he_dict['title'] = self.re_html(data.xpath('./h3/a//text()'))
                            da_he_dict['url'] = 'http://bbs.dahe.cn/' + data.xpath('./h3/a/@href')[0]
                            da_he_dict['brand'] = ''
                            da_he_dict['carseries'] = ''
                            da_he_dict['series_url'] = ''
                            # print(da_he_dict)
                            self.parse_topic_data(da_he_dict)
                        if date_data < self.start_time:
                            is_break = True
                    if is_break:
                        break
        except:
            print(111111111111111111111, traceback.format_exc())

    # 解析帖子内容
    def parse_topic_data(self, da_he_dict):
        try:
            url = da_he_dict['url']
            headers = {
                'Content-Type': 'text/html; charset=gbk',
                'Host': 'bbs.dahe.cn',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            # print(url)
            logger.log(31, url)
            try:
                # time.sleep(0.5)
                response = requests.get(url=url, headers=headers, allow_redirects=False)
            except:
                try:
                    # time.sleep(0.5)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, proxies=proxies)
                except:
                    # time.sleep(0.5)
                    response = requests.get(url=url, headers=headers, allow_redirects=False, proxies=proxies)
            response.encoding = 'gbk'
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # print(response.text)
            # # 获取发帖时间
            # time_data_test = self.clean_date(self.re_html(html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div[1]/div/div[2]/em/text()|//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div[1]/div/div[2]/em/span/text()')).replace('\\xa0', ' ').replace('发表于  ', '').replace('发表于 ', ''))
            # # print(url)
            # print(time_data_test)
            # time_data = time_data_test.split(' ')[1]
            # lang = len(time_data_test.split(':'))
            # if int(lang) == 3:
            #     time_data_1 = time_data
            # else:
            #     time_data_1 = time_data.split(':')[0] + ':' + time_data.split(':')[1] + ':' + '00'
            # print(da_he_dict['date'], '--------', time_data_1)
            # da_he_dict['source_time'] = time_data_1
            # da_he_dict['time'] = time_data_1
            # 获取浏览数，回复数
            reply_data = html.xpath('//div[@id="postlist" and @class="pl bm"]/table[1]/tr/td[1]/div/span/text()')
            # print(reply_data)
            da_he_dict['reply_no'] = reply_data[4]
            da_he_dict['views'] = reply_data[1]
            # 获取发帖人客户端
            post_client = html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div[1]/div/div[2]/span[1]/a//text()')
            if post_client == []:
                da_he_dict['post_client'] = ''
            else:
                da_he_dict['post_client'] = post_client[0] + post_client[1]
            da_he_dict['content'] = self.re_html(html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div[2]/div/div[1]/table/tr/td//text()')).replace('\\r', '').replace('\\n', '').replace('\\u3000', '').replace('\\xa0', '')
            da_he_dict['imageurl'] = html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div[2]/div/div[1]//img/@src')
            da_he_dict['audiourl'] = ''
            da_he_dict['content_id'] = da_he_dict['url'].split('-')[1]
            da_he_dict['from'] = ''
            da_he_dict['is_topics'] = '是'
            da_he_dict['floor'] = html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/table/tr[1]/td[2]/div/strong/a/text()')[0].strip()
            da_he_dict['identification'] = ''
            da_he_dict['favorite'] = ''
            da_he_dict['signin_time'] = ''
            da_he_dict['likes'] = ''
            # 判断是否是热帖
            is_elite = html.xpath('//div[@id="postlist" and @class="pl bm"]/div[1]/div/img/@title')
            if is_elite == []:
                da_he_dict['is_elite'] = '否'
            else:
                da_he_dict['is_elite'] = '是'
            da_he_dict['topic_count'] = ''
            da_he_dict['reply_count'] = ''
            da_he_dict['pick_count'] = ''
            da_he_dict['follows'] = ''
            da_he_dict['topic_categroy'] = ''
            da_he_dict['topic_type'] = ''
            da_he_dict['reposts_count'] = ''
            da_he_dict['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            da_he_dict['topic_id'] = da_he_dict['url'].split('-')[1]
            da_he_dict['file_code'] = '182'
            # logger.log(31, '----------------正在写入主贴----------------')
            # print(da_he_dict)
            item = json.dumps(dict(da_he_dict), ensure_ascii=False) + '\n'
            self.hdfsclient.new_write('/user/cspider_daily/nike_daily/forum/{}/182_{}_{}_dahe_Nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

            if int(da_he_dict['reply_no']) == 0:
                # logger.log(31, '没有回帖')
                pass
            else:
                # 获取回帖页数
                pages_num = int(math.ceil(float(int(da_he_dict['reply_no']) / 10)))
                # logger.log(31, '回帖数: %s 回帖总页数是：%s' % (da_he_dict['reply_no'], pages_num))
                self.parse_reply(pages_num, da_he_dict)
        except:
            print(222222222222222222222, traceback.format_exc())

    # 抓取回帖内容
    def parse_reply(self, pages_num, da_he_dict):
        try:
            is_work = self.is_work
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            headers = {
                'Content-Type': 'text/html; charset=gbk',
                'Host': 'bbs.dahe.cn',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            for i in range(pages_num, 0, -1):
                url = 'http://bbs.dahe.cn/thread-{}-{}-1.html'.format(da_he_dict['topic_id'], i)
                try:
                    # time.sleep(1)
                    response = requests.get(url=url, headers=headers, allow_redirects=False)
                except:
                    try:
                        # time.sleep(1)
                        response = requests.get(url=url, headers=headers, allow_redirects=False, proxies=proxies)
                    except:
                        # time.sleep(1)
                        response = requests.get(url=url, headers=headers, allow_redirects=False, proxies=proxies)
                response.encoding = 'gbk'
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)
                reply_dict = dict()
                # 获取回帖列表
                reply_list = html.xpath('//div[@id="postlist" and @class="pl bm"]/div')
                # print(len(reply_list))
                for item in reply_list[::-1]:
                    floor_data = self.re_html(item.xpath('./table/tr[1]/td[2]/div/strong/a/text()|./tr[1]/td[2]/div[1]/strong/a//text()')).replace('\\r', '').replace('\\n', '').replace('#', '').replace(' ', '')
                    # print(floor_data)
                    url_data = response.url
                    floor_test = floor_data
                    date_time_test = item.xpath('./table/tr[1]/td[2]/div[1]/div/div[2]/em/text()|./table/tr[1]/td[2]/div[1]/div/div[2]/em/span/text()')
                    # print(date_time_test)
                    if date_time_test == []:
                        pass
                    else:
                        date_time_data = self.re_html(date_time_test).replace('发表于  ', '').replace('\\xa0', ' ').replace('发表于 ', '')
                        # print(date_time_data)
                        if re.search(r'前天|昨天', date_time_data) != None:
                            datetime_data = self.clean_date(date_time_data.split(' ')[0]).split(' ')[0] + ' ' + date_time_data.split(' ')[1]
                        elif re.search(r'天前', date_time_data) != None:
                            datetime_data = self.clean_date(date_time_data)
                        else:
                            datetime_data = date_time_data
                        # print(datetime_data)
                        # 发表日期
                        date_data = datetime_data.split(' ')[0].strip()
                        date_data_test = date_data.split('-')
                        if len(date_data_test[1]) == 1 and len(date_data_test[2]) == 1:
                            date_data_parse = date_data_test[0] + '-0' + date_data_test[1] + '-0' + date_data_test[2]
                        elif len(date_data_test[1]) == 1 and len(date_data_test[2]) != 1:
                            date_data_parse = date_data_test[0] + '-0' + date_data_test[1] + '-' + date_data_test[2]
                        elif len(date_data_test[1]) != 1 and len(date_data_test[2]) == 1:
                            date_data_parse = date_data_test[0] + '-' + date_data_test[1] + '-0' + date_data_test[2]
                        else:
                            date_data_parse = date_data_test[0] + '-' + date_data_test[1] + '-' + date_data_test[2]
                        # 发表时间
                        time_data_test = datetime_data.split(' ')[1]
                        lang = len(time_data_test.split(':'))
                        if int(lang) == 3:
                            time_data = time_data_test.strip()
                        else:
                            time_data = (time_data_test.split(':')[0] + ':' + time_data_test.split(':')[1] + ':' + '00').strip()
                        # print(date_data, '*******', time_data)
                        if self.start_time <= date_data_parse.strip():
                            reply_dict['platform'] = da_he_dict['platform']
                            reply_dict['source_date'] = da_he_dict['date']
                            reply_dict['source_time'] = da_he_dict['time']
                            reply_dict['date'] = date_data_parse.strip()
                            reply_dict['time'] = time_data
                            reply_dict['author'] = item.xpath('./table/tr[1]/td[1]/div/div[1]/div/a/text()')[0]
                            reply_dict['author_url'] = 'http://bbs.dahe.cn/' + item.xpath('./table/tr[1]/td[1]/div/div[1]/div/a/@href')[0]
                            reply_dict['author_id'] = self.re_not_number(item.xpath('./table/tr[1]/td[1]/div/div[1]/div/a/@href')[0])
                            reply_dict['post_client'] = da_he_dict['post_client']
                            reply_dict['title'] = da_he_dict['title']
                            reply_dict['content'] = self.re_html(item.xpath('./table/tr[1]/td[2]/div[2]/div/div[1]/table/tr/td//text()')).replace('\\r', '')
                            reply_dict['imageurl'] = ''
                            reply_dict['audiourl'] = ''
                            reply_dict['content_id'] = self.re_not_number(item.xpath('./@id')[0])
                            reply_dict['brand'] = ''
                            reply_dict['carseries'] = ''
                            reply_dict['from'] = ''
                            reply_dict['series_url'] = ''
                            reply_dict['url'] = url_data
                            reply_dict['is_topics'] = '否'
                            reply_dict['floor'] = floor_test
                            reply_dict['identification'] = ''
                            reply_dict['favorite'] = ''
                            reply_dict['signin_time'] = ''
                            reply_dict['reply_no'] = ''
                            reply_dict['views'] = ''
                            reply_dict['likes'] = ''
                            reply_dict['is_elite'] = da_he_dict['is_elite']
                            reply_dict['topic_count'] = ''
                            reply_dict['reply_count'] = ''
                            reply_dict['pick_count'] = ''
                            reply_dict['follows'] = ''
                            reply_dict['topic_categroy'] = ''
                            reply_dict['topic_type'] = ''
                            reply_dict['reposts_count'] = ''
                            reply_dict['insert_time'] = start_time
                            reply_dict['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
                            reply_dict['topic_id'] = da_he_dict['topic_id']
                            reply_dict['file_code'] = '182'
                            # logger.log(31, '******************开始写入回帖数据**********************')
                            # print(reply_dict)
                            item = json.dumps(dict(reply_dict), ensure_ascii=False) + '\n'
                            self.hdfsclient.new_write('/user/cspider_daily/nike_daily/forum/{}/182_{}_{}_dahe_Nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')

                        if date_data < self.start_time:
                            is_work = True
                    if is_work:
                        break
        except:
            print(333333333333333333333, traceback.format_exc())

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
        for item_data in key_word_list:
            time.sleep(10)
            # print(item_data['关键词'])
            self.parse_goods_id(item_data['关键词'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()