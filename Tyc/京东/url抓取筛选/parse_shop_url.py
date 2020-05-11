import os
import requests
from lxml import etree
import json
import re
import random
import math
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import multiprocessing

# 获取文件名称
name = os.path.basename(__file__)
name = str(name).split('.')[0]
# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '  # 配置输出时间的格式，注意月份和天数不要搞乱了
file_name = r"./{}-{}.log".format(name, str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=file_name,  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
logger.addHandler(headle)

# 代理服务器
proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
# proxyUser = "H7307T4706B25G4D"
# proxyPass = "05B4877CC39192C0"
proxyUser = "HEW657EL99F83S9D"
proxyPass = "8916B1F3F10B1979"

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

        # 时间部分
        # 爬虫开始抓取的日期
        date = datetime.now() - timedelta(days=7)
        news_start_time = str(date).split(' ')[0]

        # 比对爬虫停止时间
        stop_day = datetime.now() - timedelta(days=8)
        stop_date = str(stop_day).split(' ')[0]

        # 前一天的日期
        yesterday_data = datetime.now() - timedelta(days=1)
        yesterday = str(yesterday_data).split(' ')[0]

        # 爬虫结束的抓取日期
        current_time = datetime.now()  # 当前日期
        current_day = str(current_time).split(' ')[0]

        print('爬取时间段：{}到{}'.format(news_start_time, current_day))
        logging.info('爬取时间段：{}到{}'.format(news_start_time, current_day))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = current_day
        # 前一天的日期
        self.yesterday_time = yesterday
        # 爬虫比对停止时间
        self.stop_date_day = stop_date
        # 标记爬虫工作
        self.is_work = True

    # 替换所有的HTML标签
    def re_html(self, data):
        # 替换抓取数据中的html标签
        try:
            message = str(data)
            re_h = re.compile('</?\w+[^>]*>')  # html标签
            ret1 = re_h.sub('', message)
            ret2 = re.sub('<a.*></a>', '', ret1)
            return ret2
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

    def parse_goods(self):
        try:
            url = 'https://module-jshop.jd.com/module/allGoods/goods.html?sortType=0&appId=400904&pageInstanceId=103275864&searchWord=&pageNo={}&direction=1&instanceId=172796537&modulePrototypeId=55555&moduleTemplateId=905542&refer=https://mall.jd.com/view_search-400904-0-99-1-24-1.html'
            # print(url)

            headers = {
                # 'cookie': '__jdu=15652424975692015701957; areaId=2; PCSYCityID=CN_310000_310100_310115; shshshfpa=6ef9572c-9d31-ee8d-f08b-194c070938ee-1565242499; shshshfpb=zGtS4I01Y%208H9G4W7jqLNTg%3D%3D; user-key=ef1314cd-65b4-4961-9a20-ed3c8afc3489; cn=0; ipLoc-djd=2-2830-51800-0; __jdv=76161171|p.egou.com|t_1_930182|tuiguang|1cf807c91d844f1babaa461c04199729|1565317934300; __jdc=122270672; 3AB9D23F7A4B3C9B=BCKIK7XGYYV4P2ONKGDICXQ5HFNNAPH2MAGK6P3RCO2TJFLCTEDHOQ2Z6NXJ5L7YQ2ULPWBGQ7KNT57X334EHDEU5E; shshshfp=49835de22b39c0648b3d656ad18aafcd; __jda=122270672.15652424975692015701957.1565242498.1565317934.1565322041.6; shshshsID=748458c3ed4482155c61a9135677d864_3_1565322065950; __jdb=122270672.3.15652424975692015701957|6.1565322041; JSESSIONID=E7C4241C97209B7B15864FEBE71A41B0.s1',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            time.sleep(0.2)
            response = requests.get(url=url.format(1), headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            response.encoding = 'utf-8'
            # print(response.text)
            # 商品总数
            num_data = re.search(r'共.*件商品', response.text)
            goods_num = self.re_not_number(num_data.group())
            print('店铺商品总数是： %s个.............' % goods_num)
            page_num = int(math.ceil(float(int(goods_num) / 20)))
            print('商品页数是： %s页.............' % page_num)
            for i in range(1, int(page_num)+1):
                print('**************正在抓取第: %s 页商品数据*****************' % i)

                try:
                    time.sleep(0.2)
                    response1 = requests.get(url=url.format(i), headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except (requests.exceptions.ConnectionError, ConnectionResetError):
                    try:
                        time.sleep(0.2)
                        response1 = requests.get(url=url.format(i), headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except (requests.exceptions.ConnectionError, ConnectionResetError):
                        time.sleep(0.2)
                        response1 = requests.get(url=url.format(i), headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                response1.encoding = 'utf-8'
                # print(response1.text)
                # 将响应转换成一个element对象
                html = etree.HTML(response1.text)
                items = html.xpath('//ul[@class="gl-warp clearfix"]/li')
                print(len(items))
                # 遍历商品信息节点列表
                goods_dict = dict()
                for node in items:
                    goods_dict['平台'] = '京东'
                    goods_dict['关键词'] = '耐克'
                    goods_dict['商品名'] = node.xpath('.//div[@class="jDesc"]/a/@title')[0]
                    goods_dict['商品图片'] = 'https:' + node.xpath('.//div[@class="jPic"]/a/img/@original')[0]
                    goods_dict['URL'] = 'https:' + node.xpath('.//div[@class="jDesc"]/a/@href')[0]
                    goods_dict['goods_id'] = self.re_not_number(node.xpath('.//div[@class="jDesc"]/a/@href')[0])
                    goods_dict['shop_name'] = '耐克（NIKE）京东自营专区'
                    goods_dict['品牌'] = 'NIKE'
                    goods_dict['月销量'] = ''
                    print(goods_dict)
                    self.write_Nike_jsonfile(goods_dict)
                    # print(node)
        except:
            print(11111111111111111, traceback.format_exc())

    # 写入json文件
    def write_Nike_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_jd_shop_img_url.json'.format(time.strftime('%Y%m%d')), 'ab') as f:
            f.write(item.encode("utf-8"))

    def run(self):
        # 商品数据，总共100页，从1开始
        self.parse_goods()


if __name__ == "__main__":
    spider = Spider()
    spider.run()
