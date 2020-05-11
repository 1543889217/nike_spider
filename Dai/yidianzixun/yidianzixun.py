import requests
from lxml import etree
import json
import re
import math
import time
import ast
# import execjs
import random
from datetime import datetime, date, timedelta
import logging
import proxies
import traceback
import sys
sys.path.append("./spider-dgb/")
from with_hdfs import HdfsClient
import multiprocessing



# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./../yidianzixun/yidianzixun-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')


class YiDianSpider(object):

    def __init__(self, file_path, comment_path):
        self.headers_two = {
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, sdch',
            'Accept-Language':'zh-CN,zh;q=0.8',
            'Cache-Control':'max-age=0',
            # 'Connection':'keep-alive',
            'Cookie':'cn_1255169715_dplus=%7B%22distinct_id%22%3A%20%2216730471952668-0ecf0ba7ae41cb-414f0120-15f900-16730471953461%22%2C%22sp%22%3A%20%7B%22%24_sessionid%22%3A%200%2C%22%24_sessionTime%22%3A%201542776168%2C%22%24dp%22%3A%200%2C%22%24_sessionPVTime%22%3A%201542776168%7D%7D; UM_distinctid=16730471952668-0ecf0ba7ae41cb-414f0120-15f900-16730471953461; JSESSIONID=208cee9fea61049d61e7d18f9e9c275ecf530a9e308a94dde36658adc01a0594; wuid=154945905891357; wuid_createAt=2018-11-21 12:56:9',
            'Host':'www.baidu.com',
            'Referer':'http://www.yidianzixun.com/channel/c11',
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
            'X-Requested-With':'XMLHttpRequest'
        }
        self.proxies = ['218.95.55.154:4243']

        # 去重列表
        self.set_list = []
        #
        self.error_url_list = []
        self.headers_one = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Host': 'www.baidu.com',
            # 'Proxy-Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        self.user_agent = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        ]

        # 通过系统时间自动计算时间间隔
        old_date = datetime.now() - timedelta(days=300)  # 七天前的时间，不包括今天，
        str_time = str(old_date).split(' ')[0]

        yesterday = datetime.now() - timedelta(days=1)  # 昨天时间

        yesterday = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(str_time, yesterday))

        logging.info('爬取时间段：{}到{}'.format(str_time, now_time))
        # 定义开始时间 y-m-d  离现在时间远
        self.start_time = str_time
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = now_time
        try:
            self.page_ip = proxies.res_ip()
            print('ip: ',self.page_ip)
            # self.page_ip = '116.248.160.138:4261'
        except:
            time.sleep(3)
            print('调用ip时发生错误：{}'.format(traceback.format_exc()))
            logger.error('调用ip时发生错误：{}'.format(traceback.format_exc()))
            self.page_ip = proxies.res_ip()
        self.ip_count = 0

        # 定义评论的抓取时间范围
        # self.comment_start_time = yesterday  # 一天回复
        self.comment_start_time = ''  # 不限定时间回复
        self.comment_end_time = yesterday
        # self.comment_end_time = yesterday
        self.is_get_comment = True

        self.file_path = file_path
        self.comment_path = comment_path
        self.hdfsclient = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')
        self.hdfsclient.makedirs('{}/{}'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}'.format(self.comment_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]


    def get_channel_id(self):
        url = 'http://www.yidianzixun.com/channel/c11'
        try:
            response = requests.get(url, proxies={'http': self.page_ip}, timeout=30)
            data = response.content.decode()
            data = re.search('channel_id(.*?)汽车', data).group(0)
            channel_id = re.search('\d{8,15}', data).group(0)
            cokies = response.headers['Set-Cookie']
            print(cokies)
            id = re.search('JSESSIONID=([a-z0-9]{30,80});', cokies).group(1)

            return channel_id, id
        except:
            print(traceback.format_exc())

            if self.ip_count < 10:
                self.page_ip = proxies.res_ip()
                print('跟换ip中: ', self.page_ip)
                self.ip_count += 1
                time.sleep(5)
                self.get_channel_id()
            else:
                raise IndexError

    def get_news_list_port(self, url):
        headers_port = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Host': 'www.yidianzixun.com',
            'Connection': 'keep-alive',
            # 'Upgrade-Insecure-Requests': '1',
            'Referer': 'http://www.yidianzixun.com/',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Mobile Safari/537.36',
            'Cookie': 'wuid=905306721386857; wuid_createAt=2019-10-29 16:16:20; Hm_lvt_15fafbae2b9b11d280c79eff3b840e45=1572336980; Hm_lpvt_15fafbae2b9b11d280c79eff3b840e45=1572336980; JSESSIONID=95c6398e4a11856a27afc57fb05e004777d22e14f3df2e2bc66fffda05d5dc4a',
            'X-Requested-With': 'XMLHttpRequest'
            }

        # headers_port['Cookie'] = cookie
        response = requests.get(url, headers=headers_port, proxies={'http': self.page_ip})
        # print(response.url)
        # print(response.text)
        data = response.content.decode()
        data = json.loads(data)
        data = data['result']
        # print(data)
        for news in data:
            item = {}
            try:
                title = news['title']
            except:
                continue
            item['title'] = title
            itemid = news['docid']
            url = 'http://www.yidianzixun.com/article/' + itemid
            print(url)
            news_date = news['date']
            if 'V_' not in itemid:
                if url not in self.set_list:
                    # self.write_news_jsonfile(item)
                    try:
                        self.get_news_page_info(url)
                    except:
                        print(traceback.format_exc())
                    self.set_list.append(url)

    # 获取通过js生成的spt的值
    def get_spt(self, start, channel_id):
        # start = 10
        end = start + 10
        n = "/home/q/news_list_for_channel?channel_id=11756176923&cstart=0&cend=10&infinite=true&refresh=1&__from__=pc&multi=5"
        e = str(channel_id)
        # ctx = execjs.compile(
        #     '''
        #     function good (n,e,i,t){
        #         for (var o = "sptoken", a = "", c = 1; c < arguments.length; c++){
        #             o += arguments[c];
        #         }
        #         for (var c = 0; c < o.length; c++) {
        #             var r = 10 ^ o.charCodeAt(c);
        #             a += String.fromCharCode(r)
        #         }
        #         return a
        #     }
        #     '''
        # )
        # spt = ctx.call('good', n, e, start, end)
        # return spt

    def get_news_page_info(self, url):
        item = {}
        response = requests.get(url)
        print(response.url)
        data = etree.HTML(response.content.decode())
        title = data.xpath('.//h2/text()')[0]
        if data.xpath('.//a[@class="doc-source"]/text()'):
            source = data.xpath('.//a[@class="doc-source"]/text()')[0]
        else:
            source = data.xpath('.//div[@class="meta"]/span[1]/text()')[0]
        # date_time = data.xpath('.//div[@class="meta"]/span[2]/text()')[0]
        if data.xpath('.//div[@id="imedia-article"]//text()'):
            content = data.xpath('.//div[@id="imedia-article"]//text()')
        elif data.xpath('.//div[@id="imedia-article"]/article/p//text()'):
            content = data.xpath('.//div[@id="imedia-article"]/article/p//text()')
        elif data.xpath('.//div[@id="imedia-article"]/section/section//text()'):
            content = data.xpath('.//div[@id="imedia-article"]/section/section//text()')
        elif data.xpath('.//div[@class="content-bd"]/div/div//text()'):
            content = data.xpath('.//div[@class="content-bd"]/div/div//text()')
        elif data.xpath('.//div[@class="content-bd"]/p//text()'):
            content = data.xpath('.//div[@class="content-bd"]/p//text()')
        elif data.xpath('.//div[@class="content-bd"]/div/div/text()'):
            content = data.xpath('.//div[@class="content-bd"]/div/div//text()')
        elif data.xpath('.//div[@class="content-bd"]/section//text()'):
            content = data.xpath('.//div[@class="content-bd"]/section//text()')
        elif data.xpath('.//div[@class="content-bd"]/section/text()'):
            content = data.xpath('.//div[@class="content-bd"]/section/text()')
        elif data.xpath('.//div[@class="content-bd"]//text()'):
            content = data.xpath('.//div[@class="content-bd"]//text()')
        else:
            content = data.xpath('.//div[@id="imedia-article"]/section/section/section/p//text()')
        content = ''.join(content)

        # get_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['platform'] = '一点资讯'
        item['title'] = title
        item['article_source'] = source  # 文章来源
        item['article_author'] = ''  # 文章作者
        item['content'] = content
        # if len(data.xpath('.//div[@class="meta"]/span')) == 3:
        #     date_all = data.xpath('.//div[@class="meta"]/span[3]/text()')[0]
        # elif len(data.xpath('.//div[@class="meta"]/span')) == 2:
        #     date_all = data.xpath('.//div[@class="meta"]/span[2]/text()')[0]
        # else:
        date_all = data.xpath('.//div[@class="meta"]/span//text()')
        date_all = ''.join(date_all).strip()

        try:
            if date_all == '昨天' or '小时前' in date_all:
                yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
                # print(date_all,  yesterday)
                item['date'] = yesterday
            elif date_all == '2天前':
                yesterday = datetime.now() - timedelta(days=2)  # 2前天时间
                yesterday = str(yesterday).split(' ')[0]
                # print(date_all, yesterday)
                item['date'] = yesterday
            elif date_all == '3天前':
                yesterday = datetime.now() - timedelta(days=3)  # 3前天时间
                yesterday = str(yesterday).split(' ')[0]
                # print(date_all, yesterday)
                item['date'] = yesterday
            else:
                news_date = re.search(r'\d{4}\.\d{1,2}\.\d{1,2}', date_all).group(0)
                # print(222222, news_date)
                # print(33333, date_all)
                item['date'] = news_date.replace('.', '-')
        except:
            item['date'] = self.comment_end_time
        # print(item)
        item['time'] = ''
        item['likes'] = ''
        item['clicks'] = ''
        item['views'] = ''
        item['keyword'] = ''
        item['comments_count'] = ''
        item['article_url'] = url  # 文章详情URL
        item['dislikes'] = ''  # 踩人数
        item['series_url'] = ''  # 车系首页
        item['list_url'] = 'http://www.yidianzixun.com/channel/c11'  # 文章列表URL
        item['article_type_1st'] = ''  # 文章类型
        item['article_type_2nd'] = ''  # 文章类型
        item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
        item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
        item['content_id'] = url.split('/')[-1].split('?')[0]  # 文章id
        item['topic_id'] = url.split('/')[-1].split('?')[0]  # 主贴id
        item['author_id'] = ''  # 作者id
        item['file_code'] = '26'  # 文件编号

        # 做时间判断部分---------------  这个部分区分于另外一个部分
        # if date_all == '昨天' or date_all == '2天前' or date_all == '3天前' or '小时前' in date_all:
        # print(date_all, '时间符合')
        # print(item)
        self.write_news_jsonfile(item)
        news_id = url.split('/')[-1]
        self.is_get_comment = True
        self.get_commnet_info(news_id, title, url, item['date'])

    # 获取评论信息
    def get_commnet_info(self, news_id, title, source_url, source_date, last_comment_id=''):
        item = {}
        url = 'http://www.yidianzixun.com/home/q/getcomments?_=1542864983174&docid={}&s=&count=30&last_comment_id={}&appid=web_yidian'.format(str(news_id), last_comment_id)
        response = requests.get(url)
        data = json.loads(response.content.decode())
        comments = data['comments']
        if comments:
            for comment in comments:
                # print(comment)
                # print('爬取评论中')
                item['platform'] = '一点资讯'
                item['title'] = title
                content = comment['comment']
                item['content'] = content
                author = comment['nickname']
                item['author'] = author
                date_all = comment['createAt']
                comment_date = date_all.split(' ')[0]
                comment_time = date_all.split(' ')[1]
                #  评论部分做时间判断部分---------------
                get_news_time = time.mktime(time.strptime(str(comment_date), "%Y-%m-%d"))
                end_time = time.mktime(time.strptime(self.comment_end_time, "%Y-%m-%d"))
                if self.comment_start_time != '':
                    start_time = time.mktime(time.strptime(self.comment_start_time, "%Y-%m-%d"))
                else:
                    start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
                if float(get_news_time) < float(start_time):
                    self.is_get_comment = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                    break
                elif float(start_time) <= float(get_news_time) <= float(end_time):

                    item['date'] = comment_date
                    item['time'] = comment_time
                    item['source_date'] = source_date
                    item['source_time'] = ''
                    item['source_url'] = source_url
                    item['floor'] = ''
                    item['keyword'] = ''
                    item['comment_url'] = source_url
                    item['views'] = ''
                    item['comments_count'] = ''
                    item['likes'] = ''
                    item['author_id'] = comment['userid']  # 用户id
                    item['dislikes'] = ''  # 踩人数
                    item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                    item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                    item['content_id'] = comment['comment_id']  # 内容id
                    item['topic_id'] = source_url.split('/')[-1].split('?')[0]  # 主贴id
                    item['file_code'] = '40'  # 文件编号

                    self.write_comment_jsonfile(item)
            if len(comments) == 30 and self.is_get_comment:
                last_comment_id = comments[-1]['comment_id']
                print('评论翻页')
                self.get_commnet_info(news_id, title, source_url, source_date, last_comment_id=last_comment_id)

    def write_news_jsonfile(self, item):
        logger.log(31, '正在写入新闻数据......')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../yidianzixun/26_{}_yidianzixun_news.json'.format(str(now_time)), 'ab') as f:
        #     f.write(item.encode('utf-8'))
        self.hdfsclient.new_write('{}/{}/26_{}_{}_yidianzixun_news.json'.format(self.file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0], self.time_time).replace('-', '_'), item,encoding='utf-8')

    def write_comment_jsonfile(self, item):
        logger.log(31, '正在写入评论数据......')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../yidianzixun/40_{}_yidianzixun_commnet.json'.format(str(now_time)), 'ab') as f:
        #     f.write(item.encode('utf-8'))
        self.hdfsclient.new_write('{}/{}/40_{}_{}_yidianzixun_commnet.json'.format(self.comment_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0], self.time_time).replace('-', '_'), item,encoding='utf-8')

    def get_news_url(self, num):
        """
        从百度搜索关键词，然后获取符合的新闻的url, 提取抓取数量
        """
        # 时间
        get_time = time.time()
        str_time = str(get_time)[:-4]
        date = datetime.now() - timedelta(days=7)
        a = str(date)[:-7]
        timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
        # 转换为时间戳:
        timeStamp = int(time.mktime(timeArray))
        end_time = str(timeStamp) + '.' + str_time.split('.')[1]
        print(str_time, end_time)
        # url = 'https://www.baidu.com/s?q1=汽车&q2=&q3=&q4=&gpc=stf%3D{}%2C{}%7Cstftype%3D1&ft=&q5=&q6=www.yidianzixun.com&tn=baiduadv&pn={}'.format(end_time, str_time, num)
        url = 'https://www.baidu.com/s?wd=site%3A(www.yidianzixun.com)%20%E6%B1%BD%E8%BD%A6&pn={}&oq=site%3A(www.yidianzixun.com)%20%E6%B1%BD%E8%BD%A6&ct=2097152&tn=baiduadv&ie=utf-8&si=(www.yidianzixun.com)&rsv_pq=e948db9e00097fcd&rsv_t=1273sdRx9rzb35pYERweuGf1mV6RO2BZZUthjhhdYlSidhjyUjzN%2FuD2LYJ1%2Fso&gpc=stf%3D{}%2C{}%7Cstftype%3D2&tfflag=1'.format(num, end_time, str_time)
        print(url)
        # ip = random.choice(self.proxies_list)
        response = requests.get(url, headers=self.headers_one, verify=False, timeout=30)  # , proxies={'https': ip}
        content = etree.HTML(response.content.decode())
        if content.xpath('.//h3[@class="t"]/a/@href'):
            url_list = content.xpath('.//h3[@class="t"]/a/@href')
            print(url_list)
            print(len(url_list))
            for url_ch in url_list:
                response = requests.get(url_ch, headers=self.headers_two, allow_redirects=False)
                print(response.status_code)
                news_url = response.headers['Location']
                print(news_url)
                if news_url not in self.set_list:
                    try:
                        self.get_news_page_info(news_url)
                    except Exception as e:
                        print(e)
                        time.sleep(15)
                    self.set_list.append(news_url)


    def run(self):

        url = 'http://www.yidianzixun.com/home/q/news_list_for_channel'
        get_time = time.time()
        get_time = ''.join(str(get_time).split('.'))

        url_list = [
            # 体育
            'http://www.yidianzixun.com/home/q/news_list_for_channel?channel_id=13408633425&cstart=0&cend=10&infinite=true&refresh=1&__from__=wap&_spt=yz~eaod%3B9%3E%3A2%3C99%3E8%3F%3A%3B%3A&appid=web_yidian&_={}',
            # NBA
            'http://www.yidianzixun.com/home/q/news_list_for_channel?channel_id=13408633457&cstart=0&cend=10&infinite=true&refresh=1&__from__=wap&_spt=yz~eaod%3B9%3E%3A2%3C99%3E%3F%3D%3A%3B%3A&appid=web_yidian&_={}',
            # 财经
            'http://www.yidianzixun.com/home/q/news_list_for_channel?channel_id=13408633505&cstart=0&cend=10&infinite=true&refresh=1&__from__=wap&_spt=yz~eaod%3B9%3E%3A2%3C99%3F%3A%3F%3A%3B%3A&appid=web_yidian&_={}'
        ]
        for get_url in url_list:
            for i in range(2):
                try:
                    for j in range(30):
                        url = get_url.format(str(time.time()).replace('.','')[:-4])
                        try:
                            self.get_news_list_port(url)
                        except requests.exceptions.ProxyError:
                            print(traceback.format_exc())
                            break

                except TypeError:
                    print(traceback.format_exc())
                    logger.error('内容解析错误')
                except:
                    print(traceback.format_exc())
                    logger.error('其他错误')
                self.page_ip = proxies.res_ip()

def run(file_path, comment_path):
    spider = YiDianSpider(file_path, comment_path)
    spider.run()


if __name__ == "__main__":
    # file_path = sys.argv[1]
    file_path = '/user/cspider_daily/nike_daily/article'
    # comment_path = sys.argv[2]
    comment_path = '/user/cspider_daily/nike_daily/articlecomments'
    # yidian = YiDianSpider(file_path, comment_path)

    pool = multiprocessing.Pool(processes=1)
    pool.apply_async(run, args=(file_path, comment_path, ))
    pool.close()
    # 程序计时，八小时后结束任务
    start_time = time.time()
    while True:
        if (float(time.time()) - float(start_time)) > 28800:
            logger.log(31, u'爬取时间已经达到八小时，结束进程任务')
            # os.system('taskkill /F /pid {}'.format(pid))
            # os.system('kill -9 {}'.format(str(pid)))
            pool.terminate()
            break
        time.sleep(1)

    logger.log(31, '程序结束......')
