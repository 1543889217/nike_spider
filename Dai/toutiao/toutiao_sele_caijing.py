from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from lxml import etree
import json
import requests
import re
import time
import logging
from datetime import datetime
import traceback
import os
from datetime import datetime
from datetime import timedelta


# # 设置日志记录
# LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
# DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# # file_name = r"./../toutiao/toutiao-{}.log".format(str(datetime.now()).split(' ')[0])
# logging.basicConfig(level=logging.DEBUG,
#                     format=LOG_FORMAT,
#                     datefmt=DATE_FORMAT,
#                     # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
#                     )
# # headle = logging.FileHandler(filename=file_name, encoding='utf-8')
# logger = logging.getLogger()
# # logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')


class TouTiao(object):

    def __init__(self):
        self.headers_one = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': 'tt_webid=6628733243796178436; tt_webid=6628733243796178436; csrftoken=3a6f2dc0f315bd1fe957319a75bba4ed; uuid="w:2203d39caf3249c0bcda19ee5839b850"; UM_distinctid=1675827673a27a-0dd556679b3f63-3a3a5d0c-15f900-1675827673b22c; __tasessionId=qb2c0x9mb1543386267822; CNZZDATA1259612802=992935523-1543369669-%7C1543385869',
        'referer': 'https://www.toutiao.com/ch/news_car/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36'
    }


        self.old_url_list_file = open('./../toutiao/caijing_old_url_list.json', 'r+')  # 打开文件初始化历史url列表
        self.old_url_data = self.old_url_list_file.readlines()
        self.old_url_list_file.close()  # 关闭上面的文件
        self.new_url_file = open('./../toutiao/new_url_file.json', 'w')  # 初始化中打开一个最新新闻的url列表的存放文件，以供爬虫的增量爬取
        self.new_url_list = []

        # 搜集问答类网页的列表
        self.questions_list = []

        self.is_fresh = False
        self.file_name_time = self.get_file_name_time()

    def get_page(self):

        proxy = [
            '--proxy=%s' % "222.185.137.143:4216",  # 设置的代理ip
            '--proxy-type=http',  # 代理类型
            '--ignore-ssl-errors=true',  # 忽略https错误
        ]
        # option = webdriver.ChromeOptions()
        # option.add_argument('--headless')  # 设置无头浏览器
        # os.environ["webdriver.chrome.driver"] = r'E:\chance_03\toutiao\chromedriver.exe'
        self_webdrive = webdriver.Chrome(executable_path='./chromedriver.exe')  # options=option
        # self_webdrive.set_page_load_timeout(30)
        # self_webdrive = webdriver.PhantomJS(executable_path='./phantomjs-2.1.1-windows/bin/phantomjs.exe', service_args=proxy)
        try:

            self_webdrive.get("https://www.toutiao.com/ch/news_finance/")  # Load page
            time.sleep(10)
        except:
            print(traceback.format_exc())
            print('超时')
            # logger.info('超时')
            self_webdrive.refresh()
            time.sleep(10)
            try:
                self_webdrive.find_element_by_xpath('.//div[@class="title-box"]')
            except:
                self.is_fresh = True

            while self.is_fresh:
                print('页面刷新重试中......')
                # logger.info('页面刷新重试中......')
                self_webdrive.refresh()
                time.sleep(10)
                try:
                    self_webdrive.find_element_by_xpath('.//div[@class="title-box"]')
                    self.is_fresh = False
                except:
                    pass

        print('获取页面中.....')
        # time.sleep(10)
        cheshi_data = self_webdrive.page_source
        cheshi_data = etree.HTML(cheshi_data)
        ceshi_data_list = cheshi_data.xpath('.//li[@class="item    "]/div/div[1]')
        if ceshi_data_list:
            j = 4000  # 设定翻页的最大数， 建议不要设置太大，否者selenium会卡顿
            k = 0
            for i in range(j):
                print('第{}/{}次翻页'.format(str(i), str(j)))
                # logger.info('第{}/{}次翻页'.format(str(i), str(j)))
                time.sleep(0.1)
                # if k > 10:
                #     time.sleep(8)
                #     k = 0
                # k += 1
                ActionChains(self_webdrive).key_down(Keys.DOWN).perform()  # 模拟键盘的向下按键
            data = self_webdrive.page_source
            data = etree.HTML(data)
            data_list = data.xpath('.//li[@class="item    "]/div/div[1]')
            for child in data_list:
                url = child.xpath('.//div/div[1]/a/@href')[0]
                source_author = child.xpath('.//div/div[1]/a/text()')
                source_author = ''.join(source_author)
                if '悟空问答' not in source_author:  # 筛选掉悟空问答的链接，可以在此处修改
                    print(source_author)
                    date_all = child.xpath('.//div[@class="y-box footer"]/div[@class="y-left"]/span/text()')[0].strip()

                    # if '分钟' in date_all or '刚刚' in date_all or date_all == '1小时前' or date_all == '1小时前' or

                    url = 'https://www.toutiao.com/a' + url.split('/')[2]
                    if (url + '\n') not in self.old_url_data:  # 如果url不在历史数据中

                        if url != 'https://www.toutiao.com/apc':
                            self.new_url_list.append(url)  # 将新的url添加进新的url列表中
                            self.old_url_data.append(url)  # 将新的url添加进判断url列表中， 做去重使用
                            with open('./../toutiao/caijing_old_url_list.json', 'a') as f:  # 打开历史数据的url文件，将新的url写入，保存url记录
                                f.write(url + '\n')
                            self.new_url_file.write(url + '\n')
                        # try:
                        #     time.sleep(3)
                        #     self.get_news_page(url)
                        # except Exception as e:
                        #     print(e)
            print(self.old_url_data)
            print(self.new_url_list)
        else:
            print('首页未加载......')
            self_webdrive.quit()

    # def get_news_page(self, url):
    #     item = {}
    #     response = requests.get(url, headers=self.headers_one)
    #     data_all = response.content.decode()
    #     try:
    #         data = re.search(r"articleInfo: {([\s\S]*time: '\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})",data_all).group(1)
    #         data = '{' + data + "'}}"
    #         print(data)
    #         data = re.sub('\n', '', data)
    #         data = unescape(data)
    #         data = data.replace('&quot;', '"').replace('&#x3D;', '=')
    #         content = re.search('content: ([\s\S]*)groupId', data).group(1).strip()[1:][:-2]
    #         content = etree.HTML(content)
    #         text = content.xpath('.//p//text()')
    #         text_con = ''.join(text)
    #         date, create_time = re.search('(\d{4}-\d{1,2}-\d{1,2}) (\d{1,2}:\d{1,2}:\d{1,2})', data).group(1, 2)
    #         id_num = re.search("groupId: '(\d{1,50}).*itemId", data).group(1)
    #         source = re.search("source: '(.*)time", data).group(1).strip()[:-2]
    #         comment_count = re.search("commentCount: '(\d{0,10})[\s\S]*ban_comment", data_all).group(1)
    #         item['content'] = text_con
    #         item['id'] = id_num
    #         item['source'] = source
    #         item['date'] = date
    #         item['time'] = create_time
    #         item['comment_count'] = comment_count
    #         self.write_news_jsonfile(item)
    #     except AttributeError:
    #         print('问答类网页', url)
    #         self.questions_list.append(url)
    #         print(self.questions_list)

    # # 写入json文件
    # def write_news_jsonfile(self, item):
    #     item = json.dumps(dict(item), ensure_ascii=False) + ',\n'
    #     self.news_jsonfile.write(item.encode("utf-8"))
    #
    # def write_comment_jsonfile(self, item):
    #     item = json.dumps(dict(item), ensure_ascii=False) + ',\n'
    #     self.comment_jsonfile.write(item.encode("utf-8"))
    #

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

    def time_change(self, str_time):
        """
        时间可是转换， 将‘分钟前’，‘小时前’，‘昨天’，‘前天’，转换成标准时间格式Y-m-d h:m:s
        :param str_time:
        :return:
        """
        # print(str_time, 55555555555)
        if '秒' in str_time:
            get_time = str(datetime.now()).split('.')[0]
            return get_time

        elif '分钟' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '小时' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '今天' in str_time:
            part_time = str_time.split(' ')[1]
            yesterday = datetime.now() - timedelta(days=0)  # 今天时间
            dt = str(yesterday).split(' ')[0] + ' ' + part_time
            return dt

        elif '昨天' in str_time:
            part_time = str_time.split(' ')[1]
            yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0] + ' ' + part_time
            return yesterday

        elif '前天' in str_time:
            part_time = str_time.split(' ')[1]
            two_days_ago = datetime.now() - timedelta(days=2)  # 前天时间
            two_days_ago = str(two_days_ago).split(' ')[0] + ' ' + part_time.replace('点', ':').replace('分', '')
            return two_days_ago

        elif '天前' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60 * 24
            # print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '201' not in str_time:
            str_time = '2019-' + str_time
            return str_time
        else:
            return str_time

    def close_file(self):
        self.new_url_file.close()

    def run(self):
        # try:
        self.get_page()
        self.close_file()
        # except:
        #
        #     print(traceback.format_exc())
        #     logger.error(traceback.format_exc())
        #     time.sleep(30)
        #     try:
        #         self.get_page()
        #         self.close_file()
        #     except:
        #         logger.error(traceback.format_exc())


if __name__ == "__main__":
    toutiao = TouTiao()
    toutiao.run()
