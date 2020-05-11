import mitmproxy.http
from mitmproxy import ctx, http
import json
import time
from datetime import datetime
from datetime import timedelta
import traceback
import redis
from config import device_name_list
import re
import requests
from lxml import etree



pool = redis.ConnectionPool(host='127.0.0.1')  # 实现一个连接池

redis_example = redis.Redis(connection_pool=pool)


is_work = True


def write_search_shop_info(item):
    """搜索出来的商品信息"""
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    print('搜索出来的商品信息......')
    with open('./json_file/搜索商品列表.json', 'ab') as f:
        f.write(item.encode("utf-8"))

def write_shop_page_info(item):
    """商品详情信息"""
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    print('商品详情信息......')
    with open('./json_file/商品详情信息.json', 'ab') as f:
        f.write(item.encode("utf-8"))

def write_shop_comment(item):
    print('写入评论数据中......')
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    with open('./json_file/商品评论信息.json', 'ab') as f:
        f.write(item.encode("utf-8"))

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

def response(flow: mitmproxy.http.HTTPFlow):
    # # 定义开始时间 y-m-d  离现在时间远
    # str_start_time = news_start_time
    start_time = '2019-03-23'
    # # 定义结束时间 y-m-d  离现在时间近
    # str_end_time = yesterday
    end_time = '2019-04-28'
    if 'app.poizon.com/api/v1/app/product/ice/search' in flow.request.url:
        phone_name = flow.request.headers['dudevicetrait']
        keyword = ''
        for li in device_name_list:
            if phone_name in li['name']:
                keyword = redis_example.get(phone_name).decode('utf8')
                print('关键词：', keyword)
                break

        text = flow.response.text
        json_text = json.loads(text)
        shop_list = json_text['data']['productList']
        print(shop_list)
        for shop in shop_list:
            item = {}
            item['shop_title'] = shop['title']
            item['sold_num'] = shop['soldNum']
            item['price'] = str(shop['price'])[:-2]
            item['productId'] = shop['productId']
            item['logoUrl'] = shop['logoUrl']
            item['keyword'] = keyword  # 关键词
            write_search_shop_info(item)

    if '//app.poizon.com/api/v1/app/index/ice/flow/product/detail' in flow.request.url:
        # print(flow.request.headers)
        phone_name = flow.request.headers['dudevicetrait']
        keyword = ''
        for li in device_name_list:
            if phone_name in li['name']:
                keyword = redis_example.get(phone_name).decode('utf8')
                print('关键词：', keyword)
                break

        text = flow.response.text
        json_text = json.loads(text)
        item = {}
        item["title"] = json_text["data"]["detail"]["title"]
        item['achieve_num'] = json_text["data"]["evaluate"]["count"]
        item["rateMsg"] = json_text["data"]["evaluate"]["rateMsg"]
        item["sizeMsg"] = json_text["data"]["evaluate"]["sizeMsg"]
        item["productId"] = json_text["data"]["detail"]["productId"]
        item["brandId"] = json_text["data"]["detail"]["brandId"]
        item["sellDate"] = json_text["data"]["detail"]["sellDate"]
        item["soldNum"] = json_text["data"]["detail"]["soldNum"]
        item["url"] = json_text["data"]["shareLinkUrl"]
        item['keyword'] = keyword  # 关键词
        write_shop_page_info(item)

    if 'app.poizon.com/api/v1/app/comments/app/commentList' in flow.request.url:
        keyword = ''
        phone_name = flow.request.headers['dudevicetrait']
        # print(phone_name)
        # print(redis_example.get(phone_name).decode('utf8'))
        for li in device_name_list:
            print(li)
            if phone_name in li['name']:
                keyword = redis_example.get(phone_name).decode('utf8')
                print('关键词：', keyword)
                break
        text = flow.response.text
        json_text = json.loads(text)
        url = flow.request.url
        productId = re.search('productId=(\d{1,10})?&', url).group(1)
        print(productId)
        comment_list = json_text['data']['list']
        for comment in comment_list:
            item = {}

            item['platform'] = '毒'
            item['productId'] = productId
            item['topic_id'] = comment['commentId']
            comment_date = comment['date']
            date_now = datetime.now() - timedelta(days=3)
            news_start_time = str(date_now).split(' ')[0]
            # news_start_time = '2018-01-01'
            yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
            # 做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(comment_date, "%Y-%m-%d"))
            # print(news_start_time, yesterday)
            end_time = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))

            start_time = time.mktime(time.strptime(news_start_time, "%Y-%m-%d"))

            if float(start_time) <= float(get_news_time) <= float(end_time):

                item['date'] = comment['date']
                item['time'] = ''
                item['user_name'] = comment['nickName']
                item['achieve'] = comment['size']['name']  # 尺码感受
                item['views'] = ''
                item['likes'] = ''
                item['author_id'] = comment['userId']
                item['size'] = comment['size']['size']  # 选购尺码
                item['commonSize'] = comment['size']['commonSize']  # 常穿尺码
                item['keyword'] = keyword  # 关键词
                write_shop_comment(item)

            else:
                print('时间不符合', comment_date)

    # 最热资讯部分的拦截逻辑
    if 'm.poizon.com/news/list?newsTitleId=0' in flow.request.url or 'm.poizon.com/posts/newsList' in flow.request.url:

        text = flow.response.text
        json_text = json.loads(text)
        news_list = json_text['data']['list']
        for news in news_list:
            # newsId = news['news']['newsId']
            # title = news['news']["title"]
            # readCount = news['news']["readCount"]
            # formatTime = news['news']["formatTime"]
            # # get_response = requests.get('https://m.poizon.com/news/share?newsId='+str(newsId))
            # # data = etree.HTML(get_response.content.decode())
            # # title = data.xpath('.//h1/text()')[0]
            # # print(title)
            # print(formatTime)
            # if '分钟' in formatTime or '秒' in formatTime or '1小时前' == formatTime or '2小时前' == formatTime or '小时' in formatTime:
            # #     print(title)
            #     redis_example.lpush('du_hot_news', newsId)
            newsId = news['posts']['postsId']
            title = news['posts']["title"]
            readCount = news['posts']["readCount"]
            userid = news['posts']["userInfo"]['userId']
            formatTime = news['posts']["addTime"]
            # get_response = requests.get('https://m.poizon.com/news/share?newsId='+str(newsId))
            # data = etree.HTML(get_response.content.decode())
            # title = data.xpath('.//h1/text()')[0]
            # print(title)
            print(formatTime)
            time_local = time.localtime(float(formatTime))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            print(dt)
            start_time_get = datetime.now() - timedelta(days=7)  # 昨天时间
            start_time_get = str(start_time_get).split(' ')[0]

            yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
            # 做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(dt.split(' ')[0], "%Y-%m-%d"))
            # print(news_start_time, yesterday)
            end_time = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))
            start_time = time.mktime(time.strptime(start_time_get, "%Y-%m-%d"))
            if float(start_time) <= float(get_news_time) <= float(end_time):
                print('符合时间')
                news_item = {}
                news_item['newsId'] = newsId
                news_item['dt'] = dt
                news_item['userid'] = userid
                news_item['readCount'] = readCount
                redis_example.lpush('du_hot_news', str(news_item))

    # 达人模块的拦截逻辑
    if '/m.poizon.com/question/expert' in flow.request.url and 'expertUserId' in flow.request.url:

        text = flow.response.text
        json_text = json.loads(text)
        ques_list = json_text['data']['list']
        for ques in ques_list:
            questionId = ques['questionId']
            title = ques['title']
            formatTime = ques["formatTime"]
            print(formatTime)
            # if '分钟' in formatTime or '秒' in formatTime or '1小时前' == formatTime or '2小时前' == formatTime or '小时' == formatTime:
            if '分钟' in formatTime or '小时' in formatTime or '1天' in formatTime or '2天' in formatTime:

                #     print(title)
                redis_example.lpush('du_hot_people', questionId)


