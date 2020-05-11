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
from with_hdfs import HdfsClient


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
    网易体育新闻
    """
    def __init__(self, file_path, comment_path, need_time):

        self.headers_one = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        # 评论接口模板
        self.comment_port_url = 'http://comment.api.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/{}/comments/newList?ibc=newspc&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2&offset={}&callback=jsonp_1542355418897&_=1542355418898'

        # # get_now_time = time.time() - 86400
        # get_now_time = time.time() - int(need_time)
        # time_local = time.localtime(float(get_now_time))
        # # 转换成新的时间格式(2016-05-05 20:28:54)
        # dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # end_t = time.time()
        # time_local = time.localtime(float(end_t))
        # # 转换成新的时间格式(2016-05-05 20:28:54)
        # end_dt = time.strftime("%Y-%m-%d %H:%M", time_local)  # "%Y-%m-%d %H:%M:%S"
        # # end_time = str(end_time).split(' ')[0]
        # logging.log(31, '爬取时间段：{}到{}'.format(dt, str(datetime.now())))
        # # 定义开始时间 y-m-d  离现在时间远
        # self.start_time = dt
        # # self.start_time = '2019-09-09 00:01'
        # # 定义结束时间 y-m-d  离现在时间近
        # self.end_time = end_dt
        # # self.end_time = '2019-09-16 12:57'

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
        self.start_time = news_start_time + ' 0:00'
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday + ' 23:59'
        # 标记爬虫工作
        self.is_work = True
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
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.hdfsclient.makedirs('{}/{}/{}'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name))  # 创建每日文件夹
        self.time_time = str(time.time()).split('.')[0]


    def get_list_page(self, url):
        logger.log(31, '列表页url:  ' + url)
        response = requests.get(url, headers=self.headers_one)
        # print(response.text)
        data_list = re.findall('t:.*?l:.*?p:"2019?', response.text)
        for data in data_list:
            # print(data)
            news_url = re.search('http.*html', data).group(0)
            try:
                self.get_news_info_page(news_url, '', '')
            except:
                try:
                    self.get_news_info_page(news_url, '', '')
                except:
                    print(traceback.format_exc())


    def get_list_page_two(self, url):
        logger.log(31, '列表页url:  ' + url)
        response = requests.get(url, headers=self.headers_one)
        data = etree.HTML(response.content.decode('gb2312', 'ignore'))
        li_list = data.xpath('.//ul[@class="articleList"]/li')
        for li in li_list:
            title = li.xpath('.//a/text()')[0]
            news_url = li.xpath('.//a/@href')[0]
            try:
                self.get_news_info_page(news_url, '', '')
            except:
                try:
                    self.get_news_info_page(news_url, '', '')
                except:
                    logger.error(traceback.format_exc())

    # 获取新闻详情页
    def get_news_info_page(self, news_url, comment_count, page_list):
        logger.log(31, '文章url:  ' + news_url)
        item = {}
        response = requests.get(news_url, headers=self.headers_one)
        status_code = response.status_code
        if status_code == 200:
            try:
                data = response.content.decode('gbk')
            except(UnicodeDecodeError,):
                data = response.content.decode('utf-8')
            data = etree.HTML(data)
            news_id = news_url.split('/')[-1].split('.')[0]
            try:
                title = data.xpath('.//div[@id="epContentLeft"]/h1/text()')[0]
            except:
                title = data.xpath('.//h1/text()')[0]
            try:
                date_all = data.xpath('.//div[@class="post_time_source"]/text()')[0]
                date_all = re.findall('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_all)[0]
            except:
                date_all = data.xpath('.//div[@class="headline"]/span/text()')[0]

            # 获取评论数
            try:
                comment_response = requests.get('http://comment.tie.163.com/' + str(news_id) + '.html', headers=self.headers_one)
                # print('http://comment.tie.163.com/' + str(news_id) + '.html')
                # comment_data = comment_response.content.decode()
                count = re.search('"tcount":\d{0,10}', comment_response.text).group(0)
                count = count.split(":")[1]
                comment_id = news_id
            except AttributeError:
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
                    'Host': 'comment.tie.163.com',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                    'Upgrade-Insecure-Requests': '1',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                }
                comment_id = re.search('docId" :  "(.*)?",', response.text).group(1)
                # print(comment_id)
                # print('http://comment.tie.163.com/' + str(comment_id) + '.html')
                comment_response = requests.get('http://comment.tie.163.com/' + str(comment_id) + '.html', headers=headers)
                count = re.search('"tcount":\d{0,10}', comment_response.text).group(0)
                count = count.split(":")[1]
            except:
                # print(traceback.format_exc())
                count = ''

            # 网站
            item['platform'] = '网易新闻'
            # 日期date
            #  评论部分做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(str(date_all.split(' ')[0]).strip(), "%Y-%m-%d"))
            # end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time.split(' ')[0], "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                print('时间不符合')
            elif float(start_time) <= float(get_news_time):

                date = date_all.strip().split(' ')[0]
                item['date'] = date
                news_time = date_all.strip().split(' ')[1]
                item['time'] = news_time
                item['title'] = title
                # 来源
                try:
                    source = data.xpath('.//div[@class="post_time_source"]/a/text()')[0]
                    item['article_source'] = source  # 文章来源
                except:
                    item['article_source'] = ''
                try:
                    item['article_author'] = data.xpath('.//span[@class="ep-editor"]/text()')[0]
                except:
                    item['article_author'] = ''
                # 正文内容
                content = data.xpath('.//div[@id="endText"]/p/text() | .//div[@id="endText"]/p/a/text() |.//div[@class="overview"]//p/text()')
                images_url = data.xpath('.//div[@id="endText"]//img/@src')

                content = ''.join(content)
                content = content.replace('\n', '')
                content = content.replace(' ', '')
                item['content'] = content
                item['keyword'] = ''
                item['views'] = ''
                item['comments_count'] = count
                item['likes'] = ''
                item['clicks'] = ''
                item['article_url'] = news_url  # 文章详情URL
                item['dislikes'] = ''  # 踩人数
                item['series_url'] = ''  # 车系首页
                item['list_url'] = page_list  # 文章列表URL
                if 'buy' in page_list:
                    news_type = '购车'
                elif 'nauto' in page_list:
                    news_type = '新车'
                elif 'drive' in page_list:
                    news_type = '试驾'
                elif 'buyers_guides' in page_list:
                    news_type = '导购'
                elif 'auto_newenergy' in page_list:
                    news_type = '新能源'
                elif 'news' in page_list:
                    news_type = '行业'
                else:
                    news_type = ''

                item['article_type_1st'] = news_type  # 文章类型
                item['article_type_2nd'] = ''  # 文章类型
                item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                content_id = news_url.split('/')[-1].split('.')[0]
                item['content_id'] = content_id
                item['topic_id'] = str(content_id)  # 主贴id
                item['author_id'] = ''  # 作者id
                item['content_id'] = str(content_id)
                item['file_code'] = '15'
                item['reposts_count'] = ''
                item['imageurl'] = images_url
                item['audiourl'] = []
                # print(item)
                self.__write_news_jsonfile(item)

                # 调用爬取评论的函数
                # http://comment.api.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/E0IBEEA10008856S/comments/newList?ibc=newspc&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2&offset=0&callback=jsonp_1542355418897&_=1542355418898
                self.is_get_comment = True
                self.comment_page_num = 30
                self.get_comment_info(self.comment_port_url.format(comment_id, "0"), news_id, date, news_time, title, news_url)
                # with open('./../wangyi/json_file/{}/{}_news_id.json'.format(self.file_name_time.split(' ')[0],self.file_name_time.split(' ')[0]),'a') as f:
                #     com_item = {}
                #     com_item['url'] = self.comment_port_url.format(comment_id, "0")
                #     com_item['news_id'] = news_id
                #     com_item['date'] = date
                #     com_item['news_time'] = news_time
                #     com_item['title'] = title
                #     com_item['news_url'] = news_url
                #     f.write(str(com_item) + '\n')

    # 获取评论信息
    def get_comment_info(self, url, news_id, source_date, source_time, source_title, source_url):
        # time.sleep(1)

        s = requests.session()
        s.keep_alive = False
        respnse = requests.get(url, headers=self.headers_one)
        status_code = respnse.status_code
        if status_code == 200:
            data = respnse.content.decode()
            try:
                data = re.findall(r'{"commentIds.*newListSize":\d{0,10}}', data)[0]
                data = json.loads(data)
                comment_data = data['comments']
                comment_id = data['commentIds']
                if comment_id:
                    total_item = ''
                    for comment_info in comment_data.items():
                        # print(comment_info)
                        item = {}
                        comment_info = comment_info[1]
                        # 网站
                        item['platform'] = '网易新闻'
                        # 日期时间
                        date_all = comment_info['createTime']
                        get_date = date_all[:-3]
                        #  评论部分做时间判断部分---------------
                        logger.log(31, date_all)
                        logger.log(31, get_date)
                        get_news_time = time.mktime(time.strptime(str(get_date), "%Y-%m-%d %H:%M"))
                        end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M"))
                        if self.start_time != '':
                            start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M"))
                        else:
                            start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d %H:%M"))
                        if float(get_news_time) < float(start_time):
                            self.is_get_comment = False  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                            break
                        elif float(start_time) <= float(get_news_time) <= float(end_time):
                            item['date'] = get_date
                            comment_time = date_all.split(' ')[1]
                            item['time'] = comment_time
                            # 发帖作者
                            try:
                                author = comment_info['user']['nickname']
                            except KeyError:
                                author = comment_info['user']['location'] + '网友'
                            item['author'] = author

                            item['author_id'] = comment_info['user']['userId']  # 用户id
                            # 内容
                            content = comment_info['content']
                            item['content'] = content
                            # 点赞数
                            item['likes'] = comment_info['vote']
                            # 原文发布日期时间
                            item['source_date'] = source_date
                            item['source_time'] = source_time
                            # 原文标题
                            item['title'] = source_title
                            # 原文url
                            item['source_url'] = source_url
                            item['keyword'] = ''
                            item['floor'] = ''
                            item['comment_url'] = 'http://comment.tie.163.com/' + str(news_id) + '.html'
                            item['comments_count'] = ''
                            item['views'] = ''
                            item['dislikes'] = comment_info['against']  # 踩人数
                            item['insert_time'] = str(datetime.now()).split('.')[0]  # 初始爬取时间
                            item['update_time'] = str(datetime.now()).split('.')[0]  # 最后爬取时间
                            item['content_id'] = comment_info['commentId']
                            content_id = source_url.split('/')[-1].split('.')[0]
                            item['topic_id'] = str(content_id)  # 主贴id
                            item['content_id'] = comment_info['commentId']  # 主贴id
                            item['file_code'] = '29'
                            item['reposts_count'] = ''
                            item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                            total_item += item
                            # print(item)
                    self.__write_comment_jsonfile(total_item)
                    if self.is_get_comment:
                        self.comment_page_num += 30
                        # print(self.comment_page_num, '111111111111111111111111')
                        self.get_comment_info(self.comment_port_url.format(news_id, str(self.comment_page_num)), news_id, source_date, source_time, source_title, source_url)
                else:
                    logger.log(31, '评论爬取完毕')
                    self.comment_page_num = 30
            except:
                logger.error(traceback.format_exc())

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
        # with open('./../wangyi/json_file/{}/{}_wangyi_news.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/15_{}_{}_wangyi_news.json'.format(self.file_path, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name,  str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def __write_comment_jsonfile(self, item):

        # with open('./../wangyi/json_file/{}/{}_wangyi_news_comment.json'.format(self.file_name_time.split(' ')[0], self.file_name_time), 'ab') as f:
        #     f.write(item.encode("utf-8"))
        self.hdfsclient.new_write('{}/{}/{}/29_{}_{}_wangyi_news_comment.json'.format(self.comment_apth, self.file_name_time.split(' ')[0].replace('-', ''), self.hour_name, str(datetime.now()).split(' ')[0].replace('-', '_'), self.time_time), item,encoding='utf-8')

    def run(self):
        # self.get_list_page('http://sports.163.com/special/0005rt/news_json.js?0.4744335570460496')
        #
        self.get_list_page_two('http://sports.163.com/special/0005rt/sportsgd.html')
        for i in range(2,5):
            if len(str(i)) == 1:
                i = '0'+str(i)
            self.get_list_page_two('http://sports.163.com/special/0005rt/sportsgd_{}.html'.format(str(i)))

        # self.get_news_info_page('http://sports.163.com/photoview/00D80005/166218.html', '', '')
        #
        # self.get_comment_info()

if __name__ == "__main__":
    print(sys.argv)
    file_path = sys.argv[1]
    comment_path = sys.argv[2]
    need_time = sys.argv[3]
    spider = Spider(file_path, comment_path, need_time)
    try:
        spider.run()
    except:
        logger.critical(traceback.format_exc())

    logger.log(31, '程序结束......')
