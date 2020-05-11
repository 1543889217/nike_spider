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
import xlrd


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
            ret2 = re.sub(r'\n', '', ret1)
            ret3 = re.sub(r'\u3000', '', ret2)
            ret4 = re.sub(r'品牌:', '', ret3)
            ret5 = re.sub(r'\xa0', '', ret4)
            ret6 = re.sub(r'&rarr;_&rarr;', '', ret5)
            ret7 = re.sub(r'&hellip;', '', ret6)
            ret8 = re.sub(r'https:', '', ret7)
            ret9 = re.sub(r'\[', '', ret8)
            ret10 = re.sub(r'\]', '', ret9)
            ret11 = re.sub(r"'", "", ret10)
            return ret11
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

    # ***************************************************根据关键词搜索请求得到前30个没有隐藏的商品信息
    def parse_goods(self, key_word, brand):
        try:
            # 商品数据，总共100页，从1开始
            for i_goods in range(1, 101):
                print('-------------------正在抓取第%s页商品信息, 关键词是：%s -------------------' % (i_goods, key_word))
                # 根据关键词,例如：洗发水,抓取商品信息
                url = 'https://search.jd.com/Search?keyword={}&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&suggest=2.def.0.V11--featuredump%2C&wq={}&stock=1&page={}&s={}&click=0&psort=3'.format(key_word, key_word, 2*i_goods-1, 30*i_goods+1)
                # print(url)

                headers = {
                    'authority': 'search.jd.com',
                    'method': 'GET',
                    # 'cookie': 'areaId=2; PCSYCityID=2; shshshfpa=32a16413-dbf0-50ea-e5b3-fc0700600c82-1555380265; shshshfpb=xpfj85AdZf7nEIXa%2FfPnKQA%3D%3D; unpl=V2_ZzNtbRdXFkciABZUcxtaB2JWEglLUEJGJ1hPVH0YWA1mChMPclRCFX0UR1FnGVsUZwIZXEBcRhZFCEdkeB5fA2AFEFlBZxVLK14bADlNDEY1WnwHBAJfFn0PTlJ7GFQFYwIabXJUQyV1CXZUfx1YB24CEVpHUUIQdQpFUX0fXQJiByJtRWdzJX0LRlx%2bH2wEVwIiHxYLSxZwD0FVNhlYAWMBG1xBUEYTdA1GVngcWgNmBBdZclZzFg%3d%3d; __jdv=122270672|google-search|t_262767352_googlesearch|cpc|kwd-296971091509_0_d1dcf9a193734d1e860bca817059080c|1555380273485; user-key=76e73b75-478f-450a-843d-e6bc97ab6f57; cn=0; xtest=9235.cf6b6759; ipLoc-djd=1-72-2799-0; __jdc=122270672; qrsc=3; __jdu=15553802647041324770645; _gcl_au=1.1.1158913021.1555577809; rkv=V0100; __jda=122270672.15553802647041324770645.1555380265.1555577794.1555642213.5; shshshfp=683f54342763379c330390761dd557d0; wlfstk_smdl=65rksxxy9g1y0rt33g66ck4pa10bjbcp; TrackID=1JkU9AvzDgHTRRBHhgHdYahMQFpg9HwywXxp4mumaDTg3wgCwgl-Om3llO2sZlBTQ7ojPYO3q3E7f1jiEFu3roH67lDo9yP-tEUKh5hPh0R0; thor=9D4FA5FC698214A799CE81E106F7FE2DB0DCAAA9B40E7C517B03A13BD2DEC6A4E386F3C1A031BCB4E9B02C9E911A9AC5575DF4CC502865CBE3CF846DDD4DA6377BD82D023C43A92A0A49FC1AFEE942DDBB945906391EC27DC5D7B36E454C109E4AF54AAD57757CC168F5858AB65CC1B809428E8F6C132D5BF20891137DBA880C1592FA590D2BD7D5F736F4FA0BF53133; pinId=0ng4x50EOTPaVd8k7Hb6MA; pin=t15239619067; unick=t152*****067; ceshi3.com=000; _tp=WXVubGec3KjciXDtJzPQhA%3D%3D; _pst=t15239619067; __jdb=122270672.24.15553802647041324770645|5.1555642213; shshshsID=4ecabb818431cf7caefc265364e5b589_22_1555644246658; 3AB9D23F7A4B3C9B=HPX726VSHMRMSR3STZRR7N5NRDNPYWVN43VETWWM5H7ZKTJNQRUDNAN3OFAJHRA4GMFUVMZ4HQPSNV63PBO6R5QDQI',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
                }
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.2)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                response.encoding = 'utf-8'
                # print(response.text)
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)
                # 判断是否有符合关键词的商品
                is_goods = html.xpath('//div[@class="check-error"]//text()')
                # print(is_goods)
                if is_goods != []:
                    print('关键词:%s 搜索不到对应商品' % key_word)
                    break
                else:
                    # 获取商品列表信息节点
                    goods_list = html.xpath('//div[@id="J_goodsList"]/ul/li')
                    # print(len(goods_list))
                    print(len(goods_list))
                    if int(len(goods_list)) == 0:
                        break
                    else:
                        # 获取所有的商品的id，从而获取商品的评论数
                        goods_id_list = html.xpath('//div[@id="J_goodsList"]/ul/li/div/div[1]/a/@href')
                        # print(goods_id_list)
                        get_goods_id_list = ''
                        for goods_url in goods_id_list:
                            # print(auhtor_url)
                            get_goods_id_list = get_goods_id_list + self.re_not_number(goods_url.split('com/')[1]) + ','
                        # print(get_goods_id_list)

                        comments_dict = self.get_comments(get_goods_id_list)

                        # 遍历商品信息节点列表
                        for node in goods_list:
                            goods_dict = dict()
                            goods_dict['平台'] = '京东'
                            goods_dict['关键词'] = key_word
                            goods_dict['商品名'] = self.re_html(node.xpath('./div/div[4]/a/em//text()|./div/div[3]/a/em//text()')).replace('\\', '').replace(',', '')
                            goods_dict['商品图片'] = 'https:' + node.xpath('.//div[@class="p-img"]/a/img/@source-data-lazy-img')[0]
                            goods_dict['URL'] = 'https:' + self.re_html(node.xpath('./div/div[1]/a/@href')[0])
                            try:
                                goods_dict['shop_name'] = node.xpath('.//span[@class="J_im_icon"]/a/text()')[0]
                            except:
                                goods_dict['shop_name'] = ''
                            try:
                                goods_dict['价格'] = node.xpath('./div/div[@class="p-price"]/strong/i/text()')[0]
                            except:
                                goods_dict['价格'] = ''
                            goods_dict['goods_id'] = self.re_not_number(goods_dict['URL'].split('com/')[1])
                            goods_dict['comments'] = comments_dict[goods_dict['goods_id']]
                            goods_dict['品牌'] = brand
                            print(goods_dict)
                            self.write_Nike_jsonfile(goods_dict)
        except:
            print(1111111111111111111111111, traceback.format_exc())

    # ************************************************解析请求得到每页隐藏的商品信息
    def parse_goods1(self, key_word, brand):
        try:
            # 隐藏的后30个商品数据，总共100页，从1开始
            for i in range(1, 101):
                print('-------------------正在抓取第%s页商品信息, 关键词是：%s -------------------' % (i, key_word))
                # 获取当前时间戳，保留五位小数
                a = time.time()
                b = '%.5f' % a
                print(b)
                headers = {
                    'authority': 'search.jd.com',
                    'method': 'GET',
                    'path': '/s_new.php?keyword=%E6%B4%97%E5%8F%91%E6%B0%B4&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&suggest=2.def.0.V11--featuredump%2C&wq=%E6%B4%97%E5%8F%91%E6%B0%B4&stock=1&page=2&s=30&scrolling=y&log_id=1555896550.91572&tpl=3_M&show_items=6038430,1710001,1759004,3596428,628044,3166589,206673,206709,528481,4734164,12515520881,6582008,1709989,206805,188059,4003319,2647927,5433246,2372854,6024969,7576103,4214248,7160395,3389442,3596606,1989565,100000040552,206786,3759085,100000040906',
                    'scheme': 'https',
                    # 'cookie': 'areaId=2; PCSYCityID=2; shshshfpa=32a16413-dbf0-50ea-e5b3-fc0700600c82-1555380265; shshshfpb=xpfj85AdZf7nEIXa%2FfPnKQA%3D%3D; user-key=76e73b75-478f-450a-843d-e6bc97ab6f57; xtest=9235.cf6b6759; ipLoc-djd=1-72-2799-0; qrsc=3; TrackID=1JkU9AvzDgHTRRBHhgHdYahMQFpg9HwywXxp4mumaDTg3wgCwgl-Om3llO2sZlBTQ7ojPYO3q3E7f1jiEFu3roH67lDo9yP-tEUKh5hPh0R0; pinId=0ng4x50EOTPaVd8k7Hb6MA; pin=t15239619067; unick=t152*****067; _tp=WXVubGec3KjciXDtJzPQhA%3D%3D; _pst=t15239619067; mt_xid=V2_52007VwMWVllaW1scThxaBGIDEFFYXlRbGEwdbARlBkJVVVBVRhwZHV4ZYgRGVEEIVgpMVRxbAWYEQlNfUFQPF3kaXQVvHxNXQVhaSx9JEl8NbAAbYl9oUmoWQRhYBGULEFRVWltTGkkcWgZiMxdb; unpl=V2_ZzNtbRBSRkd2CBFULxxcBmIBFV0SUxYRfFsTAHweWAdiChReclRCFX0UR1FnGVQUZwYZXktcQRRFCEdkeB5fA2AFEFlBZxVLK14bADlNDEY1WnwHBAJfFn0PTlJ7GFQFYwIabXJUQyV1CXZUfx1YB24CEVpHUUIQdQpFUX0fXQJiByJtRWdzJXEMQFF6GGwEVwIiHxYLSxV2CkdTNhlYAWMBG1xBUEYTdA1GVngcWgNmBBdZclZzFg%3d%3d; __jdv=122270672|google-search|t_262767352_googlesearch|cpc|kwd-296971091509_0_c44c21f1e4124361a5d58bde66534872|1555655309636; cn=1; _gcl_au=1.1.1967935789.1555659711; 3AB9D23F7A4B3C9B=HPX726VSHMRMSR3STZRR7N5NRDNPYWVN43VETWWM5H7ZKTJNQRUDNAN3OFAJHRA4GMFUVMZ4HQPSNV63PBO6R5QDQI; __jdc=122270672; rkv=V0100; __jda=122270672.15553802647041324770645.1555380265.1555666848.1555895765.9; shshshfp=683f54342763379c330390761dd557d0; __jdb=122270672.3.15553802647041324770645|9.1555895765; shshshsID=a7f2c0f7d88734579aaff424821d23de_3_1555896554446; __jdu=15553802647041324770645',
                    'referer': 'https://search.jd.com/Search?keyword=%E6%B4%97%E5%8F%91%E6%B0%B4&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&suggest=2.def.0.V11--featuredump%2C&wq=%E6%B4%97%E5%8F%91%E6%B0%B4&stock=1&page=1&s=1&click=0',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                }
                # 每页隐藏的剩下30个商品信息的URL
                url = 'https://search.jd.com/s_new.php?keyword={}&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq={}&page={}&s={}&scrolling=y&log_id={}&psort=3'.format(key_word, key_word, 2*i, 60*i-29, b)

                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    try:
                        time.sleep(0.2)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                    except:
                        time.sleep(0.2)
                        response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                response.encoding = 'utf-8'
                # 将响应转换成一个element对象
                html = etree.HTML(response.text)
                # 判断是否有符合关键词的商品
                is_goods = html.xpath('//div[@class="check-error"]//text()')
                # print(is_goods)
                if is_goods != []:
                    print('关键词:%s 搜索不到对应商品' % key_word)
                    break
                else:
                    # 获取商品列表信息节点
                    goods_list = html.xpath('//li[@class="gl-item"]')
                    # print(len(goods_list))
                    if int(len(goods_list)) == 0:
                        break
                    else:
                        # 获取所有的商品的id，从而获取商品的评论数
                        goods_id_list = html.xpath('//li[@class="gl-item"]/div/div[1]/a/@href')
                        # print(goods_id_list)
                        get_goods_id_list = ''
                        for goods_url in goods_id_list:
                            # print(auhtor_url)
                            get_goods_id_list = get_goods_id_list + self.re_not_number(goods_url.split('com/')[1]) + ','
                        # print(get_goods_id_list)

                        comments_dict = self.get_comments(get_goods_id_list)
                        # 遍历商品信息节点列表
                        for node in goods_list:
                            goods_dict = dict()
                            goods_dict['平台'] = '京东'
                            goods_dict['关键词'] = key_word
                            goods_dict['商品名'] = self.re_html(node.xpath('./div/div[4]/a/em//text()')).replace('\\', '').replace(',', '')
                            goods_dict['商品图片'] = 'https:' + node.xpath('.//div[@class="p-img"]/a/img/@source-data-lazy-img')[0]
                            goods_dict['URL'] = 'https:' + self.re_html(node.xpath('./div/div[1]/a/@href')[0])
                            try:
                                goods_dict['shop_name'] = node.xpath('.//span[@class="J_im_icon"]/a/text()')[0]
                            except:
                                goods_dict['shop_name'] = ''
                            try:
                                goods_dict['价格'] = node.xpath('./div/div[@class="p-price"]/strong/i/text()')[0]
                            except:
                                goods_dict['价格'] = ''
                            goods_dict['goods_id'] = self.re_not_number(goods_dict['URL'].split('com/')[1])
                            goods_dict['comments'] = comments_dict[goods_dict['goods_id']]
                            goods_dict['品牌'] = brand
                            print(goods_dict)
                            self.write_Nike_jsonfile(goods_dict)
        except:
            print(2222222222222222222, traceback.format_exc())

    # 获取商品评价数
    def get_comments(self, get_goods_id_list):
        try:
            url = 'https://club.jd.com/comment/productCommentSummaries.action?referenceIds={}'.format(get_goods_id_list)
            headers = {
                # 'content-type': 'text/html;charset=GBK',
                # 'cookie': 'shshshfpa=564ea642-01a4-a306-a5e3-753fe22992f3-1568611784; shshshfpb=cmlH7Wt2uC28Rm78KvdqxQg%3D%3D; __jdu=15680190759401091279657; __jdv=76161171|direct|-|none|-|1574845817147; PCSYCityID=CN_310000_310100_310115; areaId=2; ipLoc-djd=2-2830-51800-0; user-key=814bdea3-0a65-4772-95b5-7eb551e353ef; cn=0; __jda=122270672.15680190759401091279657.1568019076.1574845817.1574994013.18; __jdc=122270672; 3AB9D23F7A4B3C9B=7D3JTP7J5XFCNBO4K6N552KV7M6JX4DRLQUF7HUK7M55GOOY6EEG3SI4EOEEAW4V65AJUHPWP27V2G6SMBXOUSSVT4; shshshfp=e85f6d7205cbe5fd67b86c8f8f68cfdc',
                'pragma': 'no-cache',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
            }
            try:
                time.sleep(0.2)
                response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
            except:
                try:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)
                except:
                    time.sleep(0.2)
                    response = requests.get(url=url, headers=headers, proxies=proxies, allow_redirects=False, timeout=30)

            dict_text = json.loads(response.text)
            comments_list = dict_text['CommentsCount']
            # print(comments_list)
            comments_dict = dict()
            for data in comments_list:
                goods_id = data['SkuId']
                coments_num = data['CommentCount']
                comments_dict[str(goods_id)] = coments_num
            return comments_dict
        except:
            print(333333333333333, traceback.format_exc())

    # 写入json文件
    def write_Nike_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./goods_url/{}_jingdong_img_url.json'.format(time.strftime('%Y%m%d')), 'ab') as f:
            f.write(item.encode("utf-8"))

    # 读取excel获取关键词
    def parse_xlsx(self):
        # 设置路径
        path = './../快消采集关键词_v3_20200330.xlsx'
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
            keyword = data[0]
            brand = data[2]
            # print(brand)
            yield {
                '关键词': keyword,
                '品牌': brand
            }

    def run(self):
        key_word_list = []
        for item in self.parse_xlsx():
            # print(item)
            key_word_list.append(item)
        for key_word in key_word_list[0::]:
            print(key_word['关键词'])
            # self.parse_goods(key_word['关键词'], key_word['品牌'])
            self.parse_goods1(key_word['关键词'], key_word['品牌'])


if __name__ == "__main__":
    spider = Spider()
    spider.run()
