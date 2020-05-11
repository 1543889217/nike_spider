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
import os
import multiprocessing
import sys
from with_hdfs import HdfsClient


# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./../hupu/hupu-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')

class SinaSpider(object):
    """
    虎扑爬虫
    """
    def __init__(self, need_time):

        self.headers_one = {

        }
        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''

        # # 通过系统时间自动计算时间间隔
        # date = datetime.now() - timedelta(days=1)  # 1天前的时间，不包括今天
        # str_time = str(date).split(' ')[0]
        #
        # end_time = datetime.now() - timedelta(days=0)  # 今天时间
        #
        # end_time = str(end_time).split(' ')[0]
        # print('爬取时间段：{}到{}'.format(str_time, now_time))
        #
        # logging.info('爬取时间段：{}到{}'.format(str_time, now_time))

        # get_now_time = time.time() - 126000
        get_now_time = time.time() - int(need_time)
        # get_now_time = time.time() - 604800
        # print(get_now_time)
        time_local = time.localtime(float(get_now_time))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # print(dt)
        end_t = time.time()
        # print(end_t)
        time_local = time.localtime(float(end_t))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        end_dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # print(end_dt)
        # end_time = str(end_time).split(' ')[0]
        logging.log(31, '爬取时间段：{}到{}'.format(dt, str(datetime.now())))
        # 定义开始时间 y-m-d  离现在时间远
        self.start_time = dt
        # self.start_time = '2019-09-09 00:01'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = end_dt
        # self.end_time = '2019-09-16 12:54'
        # 标记爬虫工作
        self.is_work = True
        self.all_topic_url_list = []
        self.emprty_num = 0
        self.auther_id_list = []
        self.file_name_time = self.get_file_name_time()
        self.queue_item = ''
        self.queue_topic = ''
        self.source_date =''
        self.source_time =''

        self.pid = os.getpid()


    def get_forum_list(self, url):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cookie': 'PHPSESSID=da8474303baac9d465965911aae4c69e; _dacevid3=c6b2c5bd.ab53.710d.1951.0a871bffb6f4; _cnzz_CV30020080=buzi_cookie%7Cc6b2c5bd.ab53.710d.1951.0a871bffb6f4%7C-1; __gads=ID=b57c7251f1ea8930:T=1544583356:S=ALNI_Mbh5oNFJOGikKCFOFsu9ZXWiDcfDQ; _fmdata=LDwCdQHpXdYXGJD3j0882yMyZP5Ivi4MbR2dSu%2FradlbJxKKEt5yIqZNq5noEZHoUwkCLCi1yul3Ntn7n2TqOcKBVtJSJ%2BW0Re4uaC%2Fug8M%3D; _HUPUSSOID=1eb0b12b-e700-478e-b8c4-7d40c4c4b62b; _CLT=b0c2a05996d8b48b354e1fa4ddfc1fef; u=42101303|5ZWm5ZWm5Y2h5Y2h5Y2h5ZWm|7dbb|d72e6b442fce2e4bb3ce5ef39b4990bb|2fce2e4bb3ce5ef3|aHVwdV9iNTkwZGVhNTJiZmEwNTNk; us=8850633673843fdaf6fbda6052428be4782b38fdf9cb1e24a7be78b591ef21b497db3e03f3af039ea75a35cb55547d23bca3d6205193b6b59f81716866d5d8f9; ua=386146240; Hm_lvt_39fc58a7ab8a311f2f6ca4dc1222a96e=1544583357,1544583591,1544584976; Hm_lpvt_39fc58a7ab8a311f2f6ca4dc1222a96e=1544585177; __dacevst=3d855dc7.4e206a33|1544592183554',
            'referer': 'https://bbs.hupu.com/cars-98',
            'Connection': 'close',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        logging.log(31, '爬取论坛页url:{}'.format(url))
        response = requests.get(url, headers=headers, timeout=60)
        # print(response.content.decode())
        if '你可能点击了失效的链接' in response.content.decode():
            self.is_work = False
            return

        data = response.content.decode()
        if data:
            data = etree.HTML(data)
            # .//ul[@class="for-list"]/li[1]/div[1]/a[2]/@href
            li_list = data.xpath('.//ul[@class="for-list"]/li')

            if not li_list:
                self.emprty_num += 1
            else:
                self.emprty_num = 0
            if self.emprty_num > 4:
                self.is_work = False
            for index,li in enumerate(li_list):  # 提取论坛页面帖子的信息
                try:
                    url_html = li.xpath('.//a[@class="truetit"]/@href')[0]
                    views = li.xpath('.//span[@class="ansour box"]/text()')[0].strip()
                    new_date = li.xpath('.//div[@class="endreply box"]/a/text()')[0]  # 获取最后的回复时间,与回溯三天的部分获取的时间不同
                    # print(new_date, author)
                    posted_url = 'https://bbs.hupu.com/' + url_html

                    elite = li.xpath('.//a[@title="精华帖"]')
                    if elite:
                        is_elite = '是'
                    else:
                        is_elite = '否'

                    # # 做时间判断部分---------------
                    # yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                    # yesterday = str(yesterday).split(' ')[0][5:]
                    # print(yesterday)
                    #
                    # two_day_ago = datetime.now() - timedelta(days=2)  # 两天前的时间
                    # two_day_ago = str(two_day_ago).split(' ')[0][5:]
                    #
                    # if new_date == yesterday or ':' in new_date:
                    #     print(new_date, '符合时间')
                    #     # -------------------------------------------
                    #     item = {}
                    #     item['next_page_url'] = posted_url
                    #     item['views'] = views
                    #     item['is_elite'] = is_elite
                    #     self.all_topic_url_list.append(item)
                    #
                    #     # max_page = self.get_posted_page(posted_url, views, is_elite)  # 获取帖子的详情页
                    #     # # self.get_posted_page(posted_url, views, is_elite)  # 获取帖子的详情页
                    #     # logging.info('爬取URL：{}'.format(posted_url))
                    #     # if int(max_page) > 1:  # 判断帖子的翻页数，是否进行翻页操作
                    #     #     for j in range(2, int(max_page) + 1):
                    #     #         time.sleep(2)
                    #     #         page_url = posted_url.split('.html')[0]
                    #     #         next_page_url = page_url + '-' + str(j) + '.html'
                    #     #         self.get_posted_page(next_page_url, views, is_elite)
                    #     #
                    #     #         logging.info('爬取URL：{}'.format(posted_url))
                    # elif new_date == two_day_ago:
                    #     print(new_date, '前天')
                    #     logging.info('不符合时间的 url:'+posted_url)
                    #     self.is_work = False
                    #     break

                    # if (len(new_date) == 5 and '-' in new_date) and li_list.index(li) > 1:
                    #     self.is_work = False
                    #     print('时间不符合')
                    #     return
                        # new_date = '2019-' + new_date
                    if ':' in new_date:
                        new_date = str(datetime.now()).split(' ')[0] + ' ' + new_date
                    if '-' in new_date and  len(new_date) == 5:
                        new_date = str(datetime.now()).split('-')[0] + '-' + new_date + ' 01:02'

                    # 做时间判断部分---------------
                    get_news_time = time.mktime(time.strptime(new_date, "%Y-%m-%d %H:%M"))
                    # end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
                    if self.start_time != '':
                        start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
                    else:
                        start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
                    if float(get_news_time) < float(start_time) and index > 5:
                        self.is_work = False
                        logging.log(31, '时间不符合：' + str(new_date))
                        return
                    if float(start_time) <= float(get_news_time):
                        # logger.log(31, '符合时间')
                        item = {}
                        item['next_page_url'] = posted_url
                        item['views'] = views
                        item['is_elite'] = is_elite
                        # self.all_topic_url_list.append(item)
                        self.queue_topic.put(item)
                    else:
                        pass
                        # logger.log(31, '时间不符合')
                except ValueError:
                    pass
                    # print(traceback.format_exc())
                    # logger.log(31, '时间不符合')
                except:
                    # print(traceback.format_exc())
                    logging.error('爬取时出错:{}'.format(traceback.format_exc()))
        else:
            self.get_forum_list(url)

    def get_posted_page(self, url,  views, is_elite):
        response = requests.get(url, timeout=60)
        logging.log(31, u'获取帖子的详细信息url:{}_pid={}'.format(url, self.pid))
        content = response.content.decode()
        if '由于数据量过大，服务器进行数据迁移' not in content:  # 有的帖子会报这样的错误
            data = etree.HTML(content)
            floor_list = data.xpath('.//form/div[@class="floor"]')
            max_page = data.xpath('.//h1/@data-maxpage')[0]  # 获取的帖子的翻页数
            # print('共计{}页'.format(max_page))
            for floor in floor_list:
                have_floor = floor.xpath('.//div[@class="floor-show  "] | .//div[@class="floor-show"]')
                if have_floor:  # 判断楼层是否存在
                    item = {}
                    item['platform'] = '虎扑社区'
                    floor_num = floor.xpath('.//a[@class="floornum"]/@id')[0]
                    # print(floor_num,  1111)
                    if int(floor_num) == 0:  # 楼层0的时候是楼主的帖子，xpath规则不同
                        text = floor.xpath('.//div[@class="quote-content"]//text()')
                        text = ''.join(text).strip()
                        is_topics = '是'
                        reply_no = str(views).split('/')[0].strip()
                        clicks = str(views).split('/')[1].strip()

                        topic_likes = data.xpath('.//span[@class="browse"]/span[2]/text()')
                        if topic_likes:
                            topic_likes = topic_likes[0].strip()
                        else:
                            topic_likes = 0
                        likes = topic_likes
                        create_time = floor.xpath('.//div[@class="left"]/span[@class="stime"]/text()')[0]
                        item['date'] = create_time.split(' ')[0]
                        item['time'] = create_time.split(' ')[1]
                        self.source_date = create_time.split(' ')[0]
                        self.source_time = create_time.split(' ')[1]
                        item['source_date'] = self.source_date
                        item['source_time'] = self.source_time

                        author = floor.xpath('.//div[@class="left"]/a[@class="u"]/text()')[0]
                        item['author'] = author
                        title = floor.xpath('//*[@id="j_data"]/text()')[0]
                        item['title'] = title
                        item['keyword'] = ''
                        item['content'] = text
                        item['brand'] = ''
                        item['carseries'] = ''
                        item['from'] = ''
                        item['url'] = url
                        item['is_topics'] = is_topics
                        item['floor'] = floor_num
                        item['identification'] = '无'
                        item['signin_time'] = ''
                        item['reply_no'] = reply_no
                        item['views'] = clicks
                        item['likes'] = likes.replace('亮', '').replace('/', '')
                        author_id = floor.xpath('.//div/div[2]/div[1]/div[1]/a/@href')[0].split('/')[-1]
                        item['author_id'] = author_id  # 作者id
                        item['series_url'] = ''  # 车系首页
                        # item['is_elite'] = is_elite  # 是否是精华帖
                        item['is_elite'] = '否'  # 是否是精华帖
                        # topic_count = self.get_topic_count(author_id)
                        # if author_id not in self.auther_id_list:
                        #     with open('./json_file/{}/auther_id.json'.format(self.file_name_time.split(' ')[0]), 'a') as f:
                        #         f.write(author_id.strip() + '\n')
                        #     self.auther_id_list.append(author_id)
                        item['topic_count'] = ''  # 主贴数

                        item['reply_count'] = ''  # 回帖数
                        item['pick_count'] = ''  # 精华数
                        item['topic_categroy'] = ''  # 主题分类
                        item['topic_type'] = ''  # 主题类型
                        item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                        item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                        try:
                            post_client = floor.xpath('.//small/a/text()')[0]
                        except:
                            post_client = ''
                        item['post_client'] = post_client  # 发帖人客户端
                        content_id = url.split('/')[-1].split('.')[0]
                        item['content_id'] = content_id  # 内容id
                        item['topic_id'] = str(content_id)  # 主贴id
                        item['favorite'] = ''
                        item['follows'] = ''
                        item['file_code'] = '61'
                        item['reposts_count'] = ''

                        # self.write_news_jsonfile(item)
                        self.queue_item.put(item)

                    else:  # 这是回帖内容的 xpath
                        text = floor.xpath('.//tr/td/text() | .//tr/td/p/text() | .//tr/td/br/text()')
                        text = ''.join(text).strip()
                        is_topics = '否'
                        reply_no = ''
                        clicks = ''
                        likes = floor.xpath('.//div[@class="left"]/span/span/span[@class="stime"]/text()')[0]
                        # break
                        create_time = floor.xpath('.//div[@class="left"]/span[@class="stime"]/text()')[0]

                        # 做时间判断部分---------------
                        # print(create_time)
                        get_news_time = time.mktime(time.strptime(create_time, "%Y-%m-%d  %H:%M"))
                        end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d  %H:%M"))
                        if self.start_time != '':
                            start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
                        else:
                            start_time = time.mktime(time.strptime('2010-1-1 1:01:01', "%Y-%m-%d %H:%M"))

                        if float(get_news_time) < float(start_time) and '-' in url:
                            self.is_work = False

                        if float(start_time) <= float(get_news_time) <= float(end_time):
                        # if float(start_time) <= float(get_news_time):
                            item['source_date'] = self.source_date
                            item['source_time'] = self.source_time
                            item['date'] = create_time.split(' ')[0]
                            item['time'] = create_time.split(' ')[1]
                            author = floor.xpath('.//div[@class="left"]/a[@class="u"]/text()')[0]
                            item['author'] = author
                            title = floor.xpath('//*[@id="j_data"]/text()')[0]
                            item['title'] = title
                            item['keyword'] = ''
                            item['content'] = text
                            item['brand'] = ''
                            item['carseries'] = ''
                            item['from'] = ''
                            item['url'] = url
                            item['is_topics'] = is_topics
                            item['floor'] = floor_num
                            item['identification'] = '无'
                            item['signin_time'] = ''
                            item['reply_no'] = reply_no
                            item['views'] = clicks
                            item['likes'] = likes
                            author_id = floor.xpath('.//div/div[2]/div[1]/div[1]/a/@href')[0].split('/')[-1]
                            item['author_id'] = author_id  # 作者id
                            item['series_url'] = ''  # 车系首页
                            # item['is_elite'] = is_elite  # 是否是精华帖
                            item['is_elite'] = '否'  # 是否是精华帖

                            # topic_count = self.get_topic_count(author_id)

                            # if author_id not in self.auther_id_list:
                            #     with open('./json_file/{}/auther_id.json'.format(self.file_name_time.split(' ')[0]), 'a') as f:
                            #         f.write(author_id.strip()+'\n')
                            #     self.auther_id_list.append(author_id)
                            item['topic_count'] = ''  # 主贴数
                            item['reply_count'] = ''  # 回帖数
                            item['pick_count'] = ''  # 精华数
                            item['topic_categroy'] = ''  # 主题分类
                            item['topic_type'] = ''  # 主题类型
                            item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                            item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                            try:
                                post_client = floor.xpath('.//small/a/text()')[0]
                            except:
                                post_client = ''
                            item['post_client'] = post_client  # 发帖人客户端
                            content_id = url.split('/')[-1].split('.')[0].split('-')[0]
                            item['content_id'] = floor.xpath('.//span[@uid]/@pid')[0]  # 内容id
                            item['topic_id'] = str(content_id)  # 主贴id
                            item['favorite'] = ''
                            item['follows'] = ''
                            item['file_code'] = '61'
                            item['reposts_count'] = ''

                            # self.write_news_jsonfile(item)
                            self.queue_item.put(item)

                        else:
                            # logger.log(31, '回复时间不符合')
                            pass
        else:
            max_page = 1
        return max_page

    def get_topic_count(self, au_id):
        # https://bbs.hupu.com/ajax/card.php?uid=241362555236933&ulink=https%3A%2F%2Fmy.hupu.com%2F241362555236933&fid=237&_=1551773944353
        url = 'https://bbs.hupu.com/ajax/card.php?uid={}&ulink=https%3A%2F%2Fmy.hupu.com%2F{}&fid=237&_=1551773944353'.format(str(au_id), str(au_id))
        response = requests.get(url)
        data = response.text
        data = json.loads(data)
        # print(data)
        data = data['1']
        data = etree.HTML(data)
        topic_count = data.xpath('.//a/text()')[0]
        # print(topic_count)
        return topic_count

    # 写入json文件
    def write_news_jsonfile(self, item):
        # get_time_num = self.get_file_name_time()
        # try:
        #     os.mkdir('./json_file/{}'.format(get_time_num.split(' ')[0]))
        # except:
        #     pass
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        print('写入论坛数据')
        with open('./../hupu/json_file/{}/61_{}_hupu_replay_three_hours.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
            f.write(item.encode("utf-8"))

        # with open('./61_{}_hupu_replay_three_{}.json'.format(str(now_time), str(self.pid)), 'ab') as f:
        #     f.write(item.encode("utf-8"))

    def write_comment_jsonfile(self, item):
        print('写入论坛回复')
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./../hupu/hupu_commentfile_{}.json'.format(str(now_time)), 'ab') as f:
            f.write(item.encode("utf-8"))

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

    def get_topice_info_new(self, item):
        posted_url = item['next_page_url']
        views = item['views']
        is_elite = item['is_elite']
        try:
            max_page = self.get_posted_page(posted_url, views, is_elite)  # 获取帖子的详情页
        except:
            try:
                max_page = self.get_posted_page(posted_url, views, is_elite)  # 获取帖子的详情页
            except:
                logger.error(traceback.format_exc())
                return
        # self.get_posted_page(posted_url, views, is_elite)  # 获取帖子的详情页
        logging.info('爬取URL：{}'.format(posted_url))
        self.is_work = True
        if int(max_page) > 1:  # 判断帖子的翻页数，是否进行翻页操作
            for j in range(int(max_page), 1, -1):
                if not self.is_work:
                    logging.log(31, '帖子抓取到指定时间')
                    break
                time.sleep(2)
                page_url = posted_url.split('.html')[0]
                next_page_url = page_url + '-' + str(j) + '.html'
                try:
                    self.get_posted_page(next_page_url, views, is_elite)
                except:
                    try:
                        self.get_posted_page(next_page_url, views, is_elite)
                    except:
                        logging.error(traceback.format_exc())
                logging.info('爬取URL：{}'.format(posted_url))


    def run(self, queue_topic):
        self.queue_topic = queue_topic
        # url = 'https://bbs.hupu.com/cars'
        for url in open('./hupu_url'):
            print(url)
            # self.all_topic_url_list = []
            url = url .strip()
            self.get_forum_list(url)
            self.is_work = True
            for i in range(2, 10000):
                logging.log(31, '进行论坛第{}翻页'.format(str(i)))
                # print('进行论坛第{}翻页'.format(str(i)))
                if self.is_work:
                    time.sleep(3)
                    next_url = url + '-' + str(i)
                    try:
                        self.get_forum_list(next_url)
                    except:
                        try:
                            self.get_forum_list(next_url)
                        except:
                            logging.error(traceback.format_exc())
                else:
                    # print('主贴爬取到指定时间')
                    logging.log(31, '主贴爬取到指定时间')
                    break
        # print(len(self.all_topic_url_list))
        # print('30秒后开始抓取主贴信息, url:  此版块符合时间的主贴数量为: {}'.format(str(len(self.all_topic_url_list))))
        # time.sleep(30)
        # for item in self.all_topic_url_list:
        #     print(item)
        #     print('剩余：', len(self.all_topic_url_list))
        #     try:
        #         self.get_topice_info_new(item)
        #     except:
        #         try:
        #             self.get_topice_info_new(item)
        #         except:
        #             print(traceback.format_exc())
        # item = {'next_page_url': 'https://bbs.hupu.com/28849639.html', 'views': '193/3381', 'is_elite': '否'}
        # self.get_topice_info_new(item)

    def run_two(self, queue_topic, queue_item):
        self.queue_item = queue_item
        while True:
            try:
                topic = queue_topic.get(timeout=1800)
            except:
                break
            # print('爬取主贴：', topic)
            # print('剩余：', len(self.queue_topic))
            try:
                self.get_topice_info_new(topic)
            except:
                try:
                    self.get_topice_info_new(topic)
                except:
                    logger.error(traceback.format_exc())

def run(queue_topic, need_time):
    logging.log(31, '获取所有主贴进程')
    spider = SinaSpider(need_time)
    try:
        spider.run(queue_topic)
    except:
        # print(traceback.format_exc())
        logger.error(traceback.format_exc())

def run_get_poice(queue_topic, queue_item, need_time):
    logging.log(31, '抓取主贴详情进程')
    spider = SinaSpider(need_time)
    try:
        spider.run_two(queue_topic, queue_item)
    except:
        logger.error(traceback.format_exc())

hdfsclient = HdfsClient(url='http://jq-chance-05:9870', user='dpp-executor')
file_path = '/user/cspider_daily/nike_daily/forum'
time_time = str(time.time()).split('.')[0]

def write_item(queue_item):
    zero_num = 0
    logging.log(31, '开始写入数据....')
    while True:
        q_size = queue_item.qsize()
        total_item = ''
        if q_size > 0:
            for i in range(q_size):
                item = queue_item.get(timeout=600)
                # print('写入数据中......')
                item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                total_item += item
            logging.log(31, '写入数据中......')
            # item = queue.get(timeout=600)
            # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
            # with open('./47_{}_zhihu.json'.format(str(now_time)), 'ab') as f:
            #     f.write(total_item.encode("utf-8"))
            try:
                hdfsclient.new_write('{}/{}/61_{}_{}_hupu.json'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0], time_time).replace('-', '_'), total_item,encoding='utf-8')
            except:
                print(traceback)
                logging.error(traceback.format_exc())
            zero_num = 0
        else:
            zero_num += 1
        time.sleep(5)
        if zero_num > 120:
            logger.log(31, '队列中数据写入完毕......')
            break


if __name__ == "__main__":
    logger.info(sys.argv)
    # file_path = sys.argv[1]
    # need_time = sys.argv[2]
    need_time = '86400'
    hdfsclient.makedirs('{}/{}'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹

    queue_topic = multiprocessing.Manager().Queue()
    queue_item = multiprocessing.Manager().Queue()
    pool = multiprocessing.Pool(processes=8)
    # # for i in range(10):
    pool.apply_async(run, args=(queue_topic,need_time, ))
    pool.apply_async(write_item, args=(queue_item, ))
    # for keyword in cols:
    for i in range(6):
        pool.apply_async(run_get_poice, args=(queue_topic, queue_item, need_time))
    # print(2)
    pool.close()
    pool.join()
    # run(queue_topic)
