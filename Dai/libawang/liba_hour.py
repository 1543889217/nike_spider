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
import sys
sys.path.append('./data-fly/sgm_package')
sys.path.append('./spider-dgb/')
from with_hdfs import HdfsClient
from config_para import get_config_para


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
    这是一个爬虫模板
    """
    def __init__(self, file_path):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }

        # 时间判断部分
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
        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # self.start_time = '2019-09-09'
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # self.end_time = '2019-09-16'
        logging.log(31, '爬取时间段：{}到{}'.format(self.start_time, self.end_time))
        # 定义评论的抓取时间范围
        self.comment_start_time = yesterday  # 一天回复
        # self.comment_start_time = '2019-08-01'  # 一天回复
        # self.comment_start_time = ''  # 不限定时间回复
        self.comment_end_time = yesterday
        # self.comment_end_time = yesterday
        # 标记爬虫工作
        self.is_work = True
        self.file_name_time = self.get_file_name_time()
        self.file_path = file_path
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
        self.time_time = str(time.time()).split('.')[0]

    def get_search_page(self, url, keyword):
        logger.log(31, '搜索url:' + url)

        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode('utf-8', 'ignore'))
        li_list = data.xpath('.//ul[@class="ui-list"]/li')
        for li in li_list:
            title = li.xpath('.//h2/a/text()')[0]
            news_url = li.xpath('.//h2/a/@href')[0]
            topic_time = ''.join(li.xpath('.//div[@class="ui-topic-attr"]/span[2]/text()')).strip().split('：')[1]
            last_rreplay_time = li.xpath('.//div[@class="ui-topic-attr"]/span[3]/text()')[0].split('：')[1]
            views_replay = li.xpath('.//div[@class="ui-topic-attr"]/span[4]/text()')[0]
            # 做时间判断部分---------------
            get_time = self.time_change(topic_time)
            get_news_time = time.mktime(time.strptime(get_time, "%Y-%m-%d"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                self.is_work = False

            if float(start_time) <= float(get_news_time) < float(end_time):  # 符合时间段的内容
                self.get_topic_page(news_url, title, views_replay, keyword)

        a_list = data.xpath('.//a[@class="ui-page-cell"]')  # 翻页判断和操作
        for a in a_list:
            get_text = a.xpath('.//parent::a/text()')
            get_text = ''.join(get_text)
            if '下一页' in get_text:
                next_url = 'https://www.libaclub.com/' + a.xpath('.//parent::a/@href')[0]
                self.get_search_page(next_url, keyword)

    def get_topic_page(self, url, title, views_replay, keyword):
        logger.log(31, '主贴url: ' + url)
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode('utf-8', 'ignore'))
        div_list = data.xpath('.//div[@class="ui-topic"]')
        total_item = ''
        for div in div_list:
            content = div.xpath('.//div[@class="ui-topic-content fn-break"]/text()')[0]
            item = {}
            item['platform'] = '篱笆网'
            date_all = div.xpath('.//div[@class="ui-topic-operate"]/div[@class="fn-left"]/text()')[0]

            item['date'] = date_all.split(' ')[0]
            item['time'] = date_all.split(' ')[1]
            try:
                item['author'] = div.xpath('.//div[@class="ui-topic-author"]/p[1]/a/text()')[0]
                item['author_id'] = div.xpath('.//div[@class="ui-topic-author"]/p[1]/a/@href')[0].split('/')[-1]
            except:
                item['author'] = div.xpath('.//div[@class="ui-topic-author"]/p[@class="ui-topic-author-name ui-topic-author-anonymityName"]/text()')[0]
                item['author_id'] = ''
            try:
                item['post_client'] = div.xpath('.//div[@class="from-iphone"]/a/text()')[0]
            except:
                item['post_client'] = ''
            item['title'] = title
            item['content'] = content.strip()
            item['brand'] = ''
            item['carseries'] = ''
            try:
                item['from'] = div.xpath('.//div[@class="ui-topic-author"]/p[4]/text()')[0]
            except:
                item['from'] = ''
            item['series_url'] = ''
            item['url'] = url
            floor = div.xpath('.//span[@class="ui-dropdown-self"]/text()')[0]
            item['floor'] = floor
            item['identification'] = ''
            item['favorite'] = ''
            try:
                item['signin_time'] = div.xpath('.//div[@class="ui-topic-author"]/p[3]/text()')[0]
            except:
                item['signin_time'] = ''

            if floor == '楼主':
                item['views'] = views_replay.split('/')[0]
                item['reply_no'] = views_replay.split('/')[1]
                item['is_topics'] = '是'
                item['content_id'] = url.split('.h')[0].split('_')[-2]
                item['topic_id'] = url.split('.h')[0].split('_')[-2]
            else:
                item['reply_no'] = ''
                item['views'] = ''
                item['is_topics'] = '否'
                item['content_id'] = div.xpath('.//div[@class="ui-topic-operate"]/div[@class="fn-right"]/a[1]/@href')[0].split('/')[-1].split('.')[0].split('_')[-1]
                item['topic_id'] = url.split('.h')[0].split('_')[-2]
            item['likes'] = ''
            item['is_elite'] = ''
            item['topic_count'] = ''
            item['reply_count'] = ''
            item['pick_count'] = ''
            item['follows'] = ''
            item['topic_categroy'] = ''
            item['topic_type'] = ''
            item['insert_time'] = str(datetime.now()).split('.')[0]
            item['update_time'] = str(datetime.now()).split('.')[0]
            item['reply_floor'] = ''
            item['keyword'] = keyword
            item['file_code'] = '185'
            item['reposts_count'] = ''
            # print(item)
            item = json.dumps(dict(item), ensure_ascii=False) + '\n'
            total_item += item

        self.__write_news_jsonfile(total_item)

        if data.xpath('.//a[@class="ui-paging-next"]/@href'):  # 判断是否有下一页， 翻页操作
            next_page_url = 'https://www.libaclub.com' + data.xpath('.//a[@class="ui-paging-next"]/@href')[0]
            self.get_topic_page(next_page_url, title, views_replay, keyword)

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
    def __write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./../libawang/{}_liba_news_nike.json'.format(str(datetime.now()).split(' ')[0]), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/185_{}_{}_liba_news.json'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''),self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def time_change(self, str_time):
        """
        时间可是转换， 将‘分钟前’，‘小时前’，‘昨天’，‘前天’, '天前'，转换成标准时间格式Y-m-d h:m:s
        :param str_time:
        :return:
        """
        if '秒' in str_time or '刚刚' in str_time:
            get_time = str(datetime.now()).split('.')[0]
            return get_time

        elif '分钟' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '小时' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '昨天' in str_time:
            try:
                part_time = str_time.split(' ')[1]
                yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
            except:
                yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
            return yesterday

        elif '前天' in str_time:
            part_time = str_time.split(' ')[1]
            two_days_ago = datetime.now() - timedelta(days=2)  # 昨天时间
            two_days_ago = str(two_days_ago).split(' ')[0] + ' ' + part_time.replace('点', ':').replace('分', '')
            return two_days_ago

        elif '天前' in str_time:
            part_time = str_time.split('天前')[0]
            two_days_ago = datetime.now() - timedelta(days=int(part_time))  # 昨天时间
            two_days_ago = str(two_days_ago).split(' ')[0]
            return two_days_ago

        elif '年' in str_time:
            str_time = str_time.replace('年', '-').replace('月', '-').replace('日', '')
            return str_time

        elif ' ' in str_time and '202' not in str_time:
            str_time = str(datetime.now()).split('-')[0] + '-'  + str_time.split(' ')[0]
            return str_time
        else:
            # str_time = '2019-' + str_time.replace('月', '-').replace('日', '')
            return str_time

    def run(self):
        url = 'https://www.libaclub.com/facade.php?act=search&searchAction=keyword&keyword={}&sId=&timetype=2&timeBegin=1563938285&timeEnd=1566530285&sid=0&searchScope=0&orderBy=0&page=1'
        url_list = get_config_para('nike_daily_keywords')
        logger.log(31, url_list)
        for item in url_list:
            # print(1)
            keyword = item['keywords']
            logger.log(31, keyword)
            if keyword:
                search_url = url.format(keyword)
                try:
                    self.get_search_page(search_url, keyword)
                except:
                    logger.error(traceback.format_exc())


if __name__ == "__main__":
    print(sys.argv)
    file_path = sys.argv[1]
    spider = Spider(file_path)
    try:
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.log(31, '程序结束......')
