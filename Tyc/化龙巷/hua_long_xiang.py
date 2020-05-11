import os
import requests
from lxml import etree
import json
import re
import random
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing
import math
import urllib.parse
import xlrd
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
        # 时间部分
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

    # 根据关键词搜索请求得到帖子信息
    def parse_goods(self, key_word):
        try:
            print('正在抓取的关键词是：%s' % key_word)
            insert_time = time.strftime('%Y-%m-%d %H:%M:%S')
            key_word_data = urllib.parse.quote(key_word)
            is_break = self.is_break
            url = 'http://so.hualongxiang.com/?keyword={}&desc=time'
            # print(url)
            headers = {
                # 'Content-Type': 'text/html; charset=UTF-8',
                # 'Host': 'so.hualongxiang.com',
                # 'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            proxies_list = [
                {"http": "222.89.32.136:9999"},
                # {"http": "117.80.86.239:3128"}
            ]
            # print('调用代理是:%s' % random.choice(proxies_list))
            time.sleep(10)
            response = requests.get(url=url.format(key_word), headers=headers, allow_redirects=False)
            # 将响应转换成一个element对象
            html = etree.HTML(response.text)
            # 获取帖子总数
            topic_num = self.re_not_number(html.xpath('//div[@class="wapper"]/p/text()')[0].split('，')[0])
            # print(topic_num)
            if int(topic_num) == 0:
                logger.log(31, '*******-------关键词:%s 搜索不到内容-------*******' % key_word)
            else:
                # 获取帖子页数
                pages_num = int(math.ceil(float(int(topic_num) / 20)))
                # logger.log(31, '关键词: %s , 搜索帖子总数是: %s , 帖子总页数是：%s' % (key_word, topic_num, pages_num))

                for i in range(1, int(pages_num)+1):
                    topic_url = 'http://so.hualongxiang.com/search/index?keyword={}&desc=time&page={}'.format(key_word, i)
                    # print(topic_url, '调用代理是:%s' % random.choice(proxies_list))
                    time.sleep(10)
                    response1 = requests.get(url=topic_url, headers=headers, allow_redirects=False)
                    # 将响应转换成一个element对象
                    html1 = etree.HTML(response1.text)
                    # 获取帖子列表
                    topic_list = html1.xpath('//div[@class="shopper-list-long"]/ul/li')
                    # print(len(topic_list))
                    hua_long_xiang = dict()
                    # 遍历帖子列表
                    for data in topic_list:
                        date_time_data = self.clean_date(self.re_html(data.xpath('./div[@class="time"]/span/text()')[1]))
                        # print(date_time_data)
                        date_data = date_time_data.split(' ')[0].strip()
                        if self.start_time <= date_data:
                            hua_long_xiang['platform'] = '化龙巷'
                            hua_long_xiang['date'] = date_data
                            hua_long_xiang['insert_time'] = insert_time
                            hua_long_xiang['author'] = data.xpath('./div[@class="time"]/a/text()')[0]
                            hua_long_xiang['author_url'] = data.xpath('./div[@class="time"]/a/@href')[0]
                            hua_long_xiang['post_client'] = '化龙巷APP'
                            hua_long_xiang['title'] = self.re_html(data.xpath('./div[@class="title"]/a/@onclick')[0]).replace("'", '').replace(')', '')
                            hua_long_xiang['url'] = data.xpath('./div[@class="title"]/a/@href')[0]
                            hua_long_xiang['content_id'] = self.re_not_number(hua_long_xiang['url'])
                            hua_long_xiang['brand'] = ''
                            hua_long_xiang['carseries'] = ''
                            hua_long_xiang['series_url'] = ''
                            # print(hua_long_xiang)
                            response.close()
                            response1.close()
                            self.parse_topic_data(hua_long_xiang['url'], hua_long_xiang)
                        if date_data < self.start_time:
                            is_break = True
                    if is_break:
                        logger.log(31, '没有符合时间的帖子')
                        break
        except:
            print(111111111111111111111, traceback.format_exc())

    # 解析帖子内容
    def parse_topic_data(self, url, hua_long_xiang):
        try:
            headers = {
                # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                # 'Accept-Encoding': 'gzip, deflate',
                # 'Accept-Language': 'zh-CN,zh;q=0.9',
                # 'Cache-Control': 'no-cache',
                # 'Connection': 'keep-alive',
                'Cookie': 'srcurl=687474703a2f2f7777772e6875616c6f6e677869616e672e636f6d2f6368617a756f2f3135303537343135;f04e6_lastpos=T15057415;f04e6_ipstate=1573461495;security_session_verify=acc65a1e29d3f4b165840dab4d94db31;security_session_mid_verify=428b31ce793e13908b5c599759e876a4;f04e6_lastvisit=19992%091573462167%09%2Frewrite.php%3Fpychazuo%2F15057415;f04e6_ci=read%091573462167%0915057415%09103;Hm_lpvt_82d62f38b0397423b12572434961fe6c=1573462167',
                # 'Host': 'www.hualongxiang.com',
                # 'Pragma': 'no-cache',
                # 'Referer': 'http://www.hualongxiang.com/chazuo/14994153?security_verify_data=313932302c31303830',
                # 'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            proxies_list = [
                {"http": "222.89.32.136:9999"},
                # {"http": "117.80.86.239:3128"}
                ]
            # print(url, '调用代理是:%s' % random.choice(proxies_list))
            try:
                time.sleep(10)
                response = requests.get(url=url, headers=headers, allow_redirects=False)
            except:
                try:
                    time.sleep(10)
                    response = requests.get(url=url, headers=headers, allow_redirects=False)
                except:
                    time.sleep(10)
                    response = requests.get(url=url, headers=headers, allow_redirects=False)
            response.encoding = 'gbk'
            if '发表于' not in response.text:
                logger.log(31, '主贴:' + url + '请求失败，重新发起请求')
                time.sleep(20)
                self.parse_topic_data(url, hua_long_xiang)
            else:
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)
                # print(response.text)
                # 获取发帖时间
                time_data_test = re.search(r'发表于：.*?</p>', response.text)
                time_test = time_data_test.group().replace('发表于：', '').replace('</p>', '').split(' ')[1]
                lang = len(time_test.split(':'))
                if int(lang) == 3:
                    time_data = time_data_test
                else:
                    time_data = time_test.split(':')[0] + ':' + time_test.split(':')[1] + ':' + '00'
                hua_long_xiang['time'] = time_data
                hua_long_xiang['content'] = self.re_html(html.xpath('//div[@class="fs16 mb10" and @id="read_tpc"]//text()')).replace('\\r', '').replace('\\n', '').replace('\\t', '').replace('\\xa0', '')
                hua_long_xiang['imageurl'] = html.xpath('//div[@class="fs16 mb10" and @id="read_tpc"]//img/@src')
                hua_long_xiang['audiourl'] = ''
                hua_long_xiang['from'] = ''
                hua_long_xiang['is_topics'] = '是'
                hua_long_xiang['floor'] = html.xpath('//div[@class="fr gc6"]/a[2]/text()')[0]
                hua_long_xiang['author_id'] = self.re_not_number(re.search(r"uid=.*?'", html.xpath('//div[@class="fr gc6"]/a[1]/@onclick')[0]).group())
                hua_long_xiang['identification'] = ''
                hua_long_xiang['favorite'] = ''
                hua_long_xiang['signin_time'] = ''
                hua_long_xiang['reply_no'] = html.xpath('//ul[@class="data"]/li[2]/span/text()')[0]
                hua_long_xiang['views'] = html.xpath('//ul[@class="data"]/li[1]/span/text()')[0]
                hua_long_xiang['likes'] = ''
                hua_long_xiang['is_elite'] = ''
                hua_long_xiang['topic_count'] = html.xpath('//span[@class="user-info2" and @id="showface_0"]/ul/li[1]/a/text()')[0]
                hua_long_xiang['reply_count'] = ''
                hua_long_xiang['pick_count'] = ''
                hua_long_xiang['follows'] = ''
                hua_long_xiang['topic_categroy'] = ''
                hua_long_xiang['topic_type'] = ''
                hua_long_xiang['reposts_count'] = ''
                hua_long_xiang['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
                hua_long_xiang['topic_id'] = hua_long_xiang['content_id']
                hua_long_xiang['file_code'] = '187'

                # logger.log(31, '----------------正在写入主贴----------------')
                # print(hua_long_xiang)
                response.close()
                item = json.dumps(dict(hua_long_xiang), ensure_ascii=False) + '\n'
                self.hdfsclient.new_write('/user/cspider_daily/nike_daily/forum/{}/187_{}_{}_hualongxiang_Nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                if int(hua_long_xiang['reply_no']) == 0:
                    logger.log(31, '没有回帖')
                else:
                    # 获取回帖页数
                    pages_num = int(math.ceil(float(int(hua_long_xiang['reply_no']) / 25)))
                    for i in range(pages_num, 0, -1):
                        url_topic = 'http://www.hualongxiang.com/read.php?tid={}&pd=0&page={}'.format(hua_long_xiang['content_id'], i)
                        self.parse_reply(url_topic, hua_long_xiang)
        except:
            print(url, '请求主贴失败，重新发起请求')
            time.sleep(20)
            self.parse_topic_data(url, hua_long_xiang)
            print(222222222222222222222, traceback.format_exc())

    # 抓取回帖内容
    def parse_reply(self, url_topic, hua_long_xiang):
        try:
            is_work = self.is_work
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            headers = {
                # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                # 'Accept-Encoding': 'gzip, deflate',
                # 'Accept-Language': 'zh-CN,zh;q=0.9',
                # 'Cache-Control': 'no-cache',
                # 'Connection': 'keep-alive',
                'Cookie': 'srcurl=687474703a2f2f7777772e6875616c6f6e677869616e672e636f6d2f6368617a756f2f3135303537343135;f04e6_lastpos=T15057415;f04e6_ipstate=1573461495;security_session_verify=acc65a1e29d3f4b165840dab4d94db31;security_session_mid_verify=428b31ce793e13908b5c599759e876a4;f04e6_lastvisit=19992%091573462167%09%2Frewrite.php%3Fpychazuo%2F15057415;f04e6_ci=read%091573462167%0915057415%09103;Hm_lpvt_82d62f38b0397423b12572434961fe6c=1573462167',
                # 'Host': 'www.hualongxiang.com',
                # 'Pragma': 'no-cache',
                # 'Referer': 'http://www.hualongxiang.com/chazuo/14994153?security_verify_data=313932302c31303830',
                # 'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            proxies_list = [
                {"http": "222.89.32.136:9999"},
                # {"http": "117.80.86.239:3128"}
                ]
            # print('调用代理是:%s' % random.choice(proxies_list))
            try:
                time.sleep(10)
                response = requests.get(url=url_topic, headers=headers, allow_redirects=False)
            except:
                try:
                    time.sleep(10)
                    response = requests.get(url=url_topic, headers=headers, allow_redirects=False)
                except:
                    time.sleep(10)
                    response = requests.get(url=url_topic, headers=headers, allow_redirects=False)
            response.encoding = 'gbk'
            if '发表于' not in response.text:
                logger.log(31, '回帖:' + url_topic + '请求失败，重新发起请求')
                time.sleep(20)
                self.parse_reply(url_topic, hua_long_xiang)
            else:
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)

                reply_dict = dict()
                # 获取回帖列表
                reply_list = html.xpath('//div[@class="read_t"]/table[@class="floot"]')
                for item in reply_list[::-1]:
                    floor_data = item.xpath('./tr[1]/td[2]/div[2]/div/a[2]/text()')[0]
                    print(floor_data)
                    if floor_data == '楼主' or floor_data == '置顶':
                        pass
                    else:
                        url_data = response.url
                        floor_test = floor_data
                        date_time_test = item.xpath('./tr[1]/td[2]/div[2]/p/text()')[0].replace('发表于 ', '').strip()
                        print(date_time_test)
                        # 发表日期
                        date_data = date_time_test.split(' ')[0].strip()
                        # 发表时间
                        time_data_test = date_time_test.split(' ')[1]
                        lang = len(time_data_test.split(':'))
                        if int(lang) == 3:
                            time_data = time_data_test.strip()
                        else:
                            time_data = (time_data_test.split(':')[0] + ':' + time_data_test.split(':')[1] + ':' + '00').strip()
                        if self.start_time <= date_data:
                            reply_dict['platform'] = hua_long_xiang['platform']
                            reply_dict['date'] = date_data
                            reply_dict['time'] = time_data
                            reply_dict['author'] = item.xpath('./tr[1]/td[1]/div/div[1]/span[3]/text()')[0]
                            reply_dict['author_url'] = item.xpath('./tr[1]/td[1]/div/div[2]/a/@href')[0]
                            reply_dict['author_id'] = self.re_not_number(re.search(r"uid=.*?'", item.xpath('./tr[1]/td[2]/div[2]/div/a[1]/@onclick')[0]).group())
                            reply_dict['post_client'] = hua_long_xiang['post_client']
                            reply_dict['title'] = hua_long_xiang['title']
                            reply_dict['content'] = item.xpath('./tr[1]/td[2]/div[4]/div/div[2]/text()')[0]
                            reply_dict['imageurl'] = ''
                            reply_dict['audiourl'] = ''
                            reply_dict['content_id'] = self.re_not_number(item.xpath('./tr[1]/td[2]/div[4]/div/div[2]/@id')[0])
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
                            reply_dict['is_elite'] = ''
                            reply_dict['topic_count'] = item.xpath('./tr[1]/td[1]/div/span/ul/li[1]/a/text()')[0]
                            reply_dict['reply_count'] = ''
                            reply_dict['pick_count'] = ''
                            reply_dict['follows'] = ''
                            reply_dict['topic_categroy'] = ''
                            reply_dict['topic_type'] = ''
                            reply_dict['reposts_count'] = ''
                            reply_dict['insert_time'] = start_time
                            reply_dict['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
                            reply_dict['topic_id'] = hua_long_xiang['topic_id']
                            reply_dict['file_code'] = '187'
                            # logger.log(31, '******************开始写入回帖数据**********************')
                            # print(reply_dict)
                            response.close()
                            item = json.dumps(dict(reply_dict), ensure_ascii=False) + '\n'
                            self.hdfsclient.new_write('/user/cspider_daily/nike_daily/forum/{}/187_{}_{}_hualongxiang_Nike.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d'), self.time_data), item, encoding='utf-8')
                        if date_data < self.start_time:
                            is_work = True
                    if is_work:
                        break
        except:
            print(url_topic, '请求回贴失败，重新发起请求')
            time.sleep(20)
            self.parse_topic_data(url_topic, hua_long_xiang)
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
            key_word_list.append(item)
        for item_data in key_word_list:
            self.parse_goods(item_data['关键词'])


def HLX_run():
    spider = Spider()
    try:
        spider.run()
    except:
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    pid = os.getpid()
    pool = multiprocessing.Pool(processes=1)
    for i in range(1):
        pool.apply_async(HLX_run, args=())
    pool.close()
    # pool.join()

    # 程序计时，两小时后结束任务
    py_start_time = time.time()
    while True:
        if (float(time.time()) - float(py_start_time)) > 27000:
            logger.log(31, u'爬取时间已经达到7.5小时，结束进程任务：' + str(pid))
            pool.terminate()
            break
        time.sleep(1)