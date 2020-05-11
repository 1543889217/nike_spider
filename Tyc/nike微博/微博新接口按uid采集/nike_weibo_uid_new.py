# -*- coding:UTF-8 -*-
import requests
import pandas as pd
import datetime
import json
import threading
import pickle
from datetime import timedelta
import re
import os
import traceback
import sys
import time
import multiprocessing
# import logo
from with_hdfs import HdfsClient

CurrentPath = os.path.abspath(__file__)
SuperiorPath = os.path.dirname(CurrentPath)
FatherPath = os.path.dirname(SuperiorPath)
sys.path.insert(0, SuperiorPath)
# logger = logo.create_logo(os.getpid(), __name__, '{}_crawer.log'.format(time.strftime('%Y%m%d')))

code = '2.00o4_w1HrAaeYBedf38e38b8SnITmD'
APP_KEY = "1428199813"
APP_SECRET = "c25e5caf32f3b72319867990e59c2b1f"
keyword_file = os.path.join(SuperiorPath, 'nike_官方账号列表_1202_v3-id.xlsx')  # 打开关键词文件进行读取


class Spider(object):
    """
    这是一个爬虫模板
    """

    def __init__(self):
        # 时间判断部分
        date = datetime.datetime.now() - timedelta(days=1)
        news_start_time = str(date).split(' ')[0]

        now_date = datetime.datetime.now() - timedelta(days=0)  # 当前时间
        now_time = str(now_date).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, now_time))
        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = now_time
        self.is_break = False
        self.pid = os.getpid()

        # 链接hdfs
        self.hdfsclient = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
        self.hdfsclient.makedirs('/user/cspider_daily/nike_daily/weibo/{}'.format(time.strftime('%Y%m%d')))  # 创建每日文件夹

    # 使用try清理数据
    def clean_data(self, data):
        try:
            clean_data = data
        except:
            clean_data = ''
        return clean_data

    # 时间格式转换
    def changetime(self, timestr):
        fmt2 = '%a %b %d  %H:%M:%S %z %Y'
        timestrp = time.strptime(timestr, fmt2)
        # temp_time = time.strftime("%Y-%m-%d %H:%M:%S", timestrp)
        # logger.info(f"last time {temp_time}, continue request")
        timestampstr = time.mktime(timestrp)
        timeArray = time.localtime(int(timestampstr))
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        # print(otherStyleTime)  # 2013--10--10 23:40:00
        return otherStyleTime

    # 请求获取用户age
    def parse_age(self, uid):
        try:
            weibo_dict = dict()
            # 请求接口需要的携带参数
            data = {'access_token': code,  # 访问许可
                    'uid': '{}'.format(uid),  # 搜索关键词
                    }
            url = 'https://c.api.weibo.com/2/users/birthday/other.json'
            try:
                time.sleep(0.1)
                response = requests.get(url, data, timeout=30)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
                except:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
            # print('000000000000000000', response.text)
            if 'birthday_visible' in response.text:
                # print('有用户年龄')
                age_data = json.loads(response.text)['birthday']
                birthday_visible = json.loads(response.text)['birthday_visible']
                if int(birthday_visible) == 3:
                    if age_data == '':
                        weibo_dict['age'] = ''
                        # print('111111111111111111', weibo_dict)
                        self.parse_weibo(weibo_dict, uid)
                    elif int(age_data) >= 1900:
                        weibo_dict['age'] = age_data
                        # print('111111111111111111', weibo_dict)
                        self.parse_weibo(weibo_dict, uid)
                    else:
                        weibo_dict['age'] = ''
                        # print('111111111111111111', weibo_dict)
                        self.parse_weibo(weibo_dict, uid)
            else:
                weibo_dict['age'] = ''
                # print('111111111111111111', weibo_dict)
                self.parse_weibo(weibo_dict, uid)

        except:
            self.parse_age(uid)
            try:
                self.parse_age(uid)
            except:
                print(00000000000000, traceback.format_exc())

    # 根据关键词搜索请求得到微博信息
    def parse_weibo(self, weibo_dict, uid):
        try:
            is_break = self.is_break
            date = time.strftime("%Y%m%d")
            st = int(time.mktime(time.strptime(date, '%Y%m%d')))  # 自定义起始时间 '2019-10-21 00:00:00'
            et = st - 86400  # 自定义终止时间 '2018-11-26 00:00:00'

            url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'  # 接口链接
            # 请求接口需要的携带参数
            data = {'access_token': code,  # 访问许可
                    'uid': '{}'.format(uid),
                    'endtime': '{}'.format(st),  # 首次采集终止点为当前日期的零点，'2019-10-23 00:00:00'
                    'count': 20
                    }  # 单页返回的记录条数，最大不超过100，超过100以100处理，默认为20。数据是从当前零点往前一天零点时间递减出现
            try:
                time.sleep(0.1)
                response = requests.get(url, data, timeout=30)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
                except:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
            # print(weibo_dict)
            # print(response.text)
            if 'statuses' in response.text:
                data_list = json.loads(response.text, strict=False)['statuses']
                # print(len(data_list))
                for item in data_list:
                    date_time_data = item['created_at']
                    # print(self.changetime(date_time_data))
                    try:
                        date_data = self.changetime(date_time_data).split(' ')[0]
                    except:
                        date_data = ''
                    try:
                        time_data = self.changetime(date_time_data).split(' ')[1]
                    except:
                        time_data = ''
                    # print(date_data, time_data)
                    weibo_dict['platform'] = '微博'
                    weibo_dict['keyword'] = str(uid)
                    weibo_dict['date'] = date_data.strip()
                    weibo_dict['time'] = time_data.strip()
                    weibo_dict['weibo_id'] = str(item['id'])
                    weibo_dict['mid'] = str(item['mid'])
                    weibo_dict['idstr'] = str(item['idstr'])
                    try:
                        weibo_dict['content'] = item['longText']['longTextContent'].replace('\u200b', ' ').replace('\u200e', ' ').replace('\u200c', ' ').replace('\n', ' ')
                    except:
                        weibo_dict['content'] = item['text'].replace('\u200b', ' ').replace('\u200e', ' ').replace('\u200c', ' ').replace('\n', ' ')
                    weibo_dict['source'] = item['source']
                    weibo_dict['favorited'] = item['favorited']
                    weibo_dict['truncated'] = item['truncated']
                    try:
                        location_data = item['user']['location']
                    except:
                        location_data = ''
                    try:
                        weibo_dict['province_name'] = location_data.split(' ')[0]
                        weibo_dict['address'] = location_data.split(' ')[1]
                    except:
                        weibo_dict['province_name'] = location_data
                        weibo_dict['address'] = ''
                    # print(weibo_dict['province_name'], weibo_dict['address'])
                    try:
                        weibo_dict['pinyin'] = item['pinyin']
                    except:
                        weibo_dict['pinyin'] = ''
                    weibo_dict['uid'] = str(item['user']['id'])
                    try:
                        weibo_dict['screen_name'] = item['user']['screen_name']
                    except:
                        weibo_dict['screen_name'] = ''
                    try:
                        weibo_dict['name'] = item['user']['name']
                    except:
                        weibo_dict['name'] = ''
                    try:
                        weibo_dict['province'] = item['user']['province']
                    except:
                        weibo_dict['province'] = ''
                    try:
                        weibo_dict['city'] = item['user']['city']
                    except:
                        weibo_dict['city'] = ''
                    try:
                        weibo_dict['location'] = item['user']['location']
                    except:
                        weibo_dict['location'] = ''
                    try:
                        weibo_dict['gender'] = item['user']['gender']
                    except:
                        weibo_dict['gender'] = ''
                    try:
                        weibo_dict['allow_all_act_msg'] = item['user']['allow_all_act_msg']
                    except:
                        weibo_dict['allow_all_act_msg'] = ''
                    try:
                        weibo_dict['geo_enabled'] = item['user']['geo_enabled']
                    except:
                        weibo_dict['geo_enabled'] = ''
                    try:
                        weibo_dict['verified'] = item['user']['verified']
                    except:
                        weibo_dict['verified'] = ''
                    try:
                        weibo_dict['verified_reason'] = item['user']['verified_reason']
                    except:
                        weibo_dict['verified_reason'] = ''
                    weibo_dict['likes'] = item['attitudes_count']
                    try:
                        weibo_dict['views'] = item['views']
                    except:
                        weibo_dict['views'] = ''
                    try:
                        weibo_dict['retweeted_status'] = str(item['retweeted_status'])
                    except:
                        weibo_dict['retweeted_status'] = ''
                    weibo_dict['reposts_count'] = item['reposts_count']
                    weibo_dict['comments_count'] = item['comments_count']
                    weibo_dict['attitudes_count'] = item['attitudes_count']
                    weibo_dict['visible'] = str(item['visible'])
                    weibo_dict['pic_ids'] = str(item['pic_ids'])
                    try:
                        weibo_dict['ad'] = item['ad']
                    except:
                        weibo_dict['ad'] = ''
                    weibo_dict['isLongText'] = item['isLongText']
                    weibo_dict['url'] = 'http://m.weibo.cn/' + str(item['user']['id']) + '/' + str(item['idstr'])
                    try:
                        weibo_dict['followers_count'] = item['user']['followers_count']
                    except:
                        weibo_dict['followers_count'] = ''
                    try:
                        weibo_dict['favourites_count'] = item['user']['favourites_count']
                    except:
                        weibo_dict['favourites_count'] = ''
                    try:
                        weibo_dict['friends_count'] = item['user']['friends_count']
                    except:
                        weibo_dict['friends_count'] = ''
                    try:
                        weibo_dict['statuses_count'] = item['user']['statuses_count']
                    except:
                        weibo_dict['statuses_count'] = ''
                    try:
                        weibo_dict['bi_followers_count'] = item['user']['bi_followers_count']
                    except:
                        weibo_dict['bi_followers_count'] = ''
                    try:
                        weibo_dict['avatar_large'] = item['user']['avatar_large']
                    except:
                        weibo_dict['avatar_large'] = ''
                    try:
                        weibo_dict['avatar_hd'] = item['user']['avatar_hd']
                    except:
                        weibo_dict['avatar_hd'] = ''
                    try:
                        weibo_dict['retweeted_time'] = item['retweeted_status']['created_at']
                    except:
                        weibo_dict['retweeted_time'] = ''
                    try:
                        weibo_dict['retweeted_post_id'] = item['retweeted_status']['id']
                    except:
                        weibo_dict['retweeted_post_id'] = ''
                    try:
                        weibo_dict['retweeted_author'] = item['retweeted_status']['in_reply_to_screen_name']
                    except:
                        weibo_dict['retweeted_author'] = ''
                    try:
                        weibo_dict['retweeted_author_id'] = item['retweeted_status']['in_reply_to_status_id']
                    except:
                        weibo_dict['retweeted_author_id'] = ''
                    try:
                        weibo_dict['profile_url'] = item['user']['profile_url']
                    except:
                        weibo_dict['profile_url'] = ''
                    try:
                        weibo_dict['domain'] = item['user']['domain']
                    except:
                        weibo_dict['domain'] = ''
                    try:
                        weibo_dict['user_url'] = item['user']['domain']
                    except:
                        weibo_dict['user_url'] = ''
                    weibo_dict['author_url'] = 'http://m.weibo.cn/' + str(item['user']['id'])
                    weibo_dict['tags'] = self.parse_tags(weibo_dict)

                    # 图片列表判断
                    img_list = item['pic_ids']
                    if len(img_list) == 0:
                        weibo_dict['imageurl'] = ''
                        weibo_dict['audiourl'] = ''
                    else:
                        weibo_img = []
                        original_pic = item['original_pic'].split('large/')[0] + 'large/'
                        for img in img_list:
                            img_data = original_pic + img + '.jpg'
                            weibo_img.append(img_data)
                        weibo_dict['imageurl'] = weibo_img
                        weibo_dict['audiourl'] = ''

                    # print(weibo_dict['imageurl'])
                    self.write_goods_jsonfile(weibo_dict)
                    index_num = data_list.index(item)
                    if index_num == len(data_list) - 1:
                        # print(index_num)
                        last_time = self.changetime(data_list[int(index_num)]['created_at'])
                        last_date = self.changetime(data_list[int(index_num)]['created_at']).split(' ')[0]
                        # print(last_time)
                        # print(last_date)
                        if self.start_time <= last_date:
                            # 将其转换为时间数组
                            timeArray = time.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                            # 转换为时间戳:
                            timeStamp = int(time.mktime(timeArray))
                            # print('最后一个时间%s转换成时间戳是: ' % last_time, timeStamp)
                            self.parse_weibo_data(weibo_dict, uid, timeStamp)
                            # pass
                        if self.start_time > last_date:
                            is_break = True
                    if is_break:
                        break
        except:
            print(111111111111111111111111, traceback.format_exc())

    # 根据关键词搜索请求得到微博信息
    def parse_weibo_data(self, weibo_dict, uid, timeStamp):
        try:
            is_break = self.is_break
            url = 'https://c.api.weibo.com/2/search/statuses/limited.json'  # 接口链接
            # 请求接口需要的携带参数
            data = {'access_token': code,  # 访问许可
                    'uid': '{}'.format(uid),
                    'endtime': '{}'.format(timeStamp),  # 首次采集终止点为当前日期的零点，'2019-10-23 00:00:00'
                    'count': 20
                    }  # 单页返回的记录条数，最大不超过100，超过100以100处理，默认为20。数据是从当前零点往前一天零点时间递减出现
            try:
                time.sleep(0.1)
                response = requests.get(url, data, timeout=30)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
                except:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
            # print(response.text)
            if 'statuses' in response.text:
                data_list = json.loads(response.text, strict=False)['statuses']
                # print(len(data_list))
                for item in data_list:
                    date_time_data = item['created_at']
                    # print(self.changetime(date_time_data))
                    try:
                        date_data = self.changetime(date_time_data).split(' ')[0]
                    except:
                        date_data = ''
                    try:
                        time_data = self.changetime(date_time_data).split(' ')[1]
                    except:
                        time_data = ''
                    # print(date_data, time_data)
                    weibo_dict['platform'] = '微博'
                    weibo_dict['keyword'] = str(uid)
                    weibo_dict['date'] = date_data.strip()
                    weibo_dict['time'] = time_data.strip()
                    weibo_dict['weibo_id'] = str(item['id'])
                    weibo_dict['mid'] = str(item['mid'])
                    weibo_dict['idstr'] = str(item['idstr'])
                    try:
                        weibo_dict['content'] = item['longText']['longTextContent'].replace('\u200b', ' ').replace('\u200e', ' ').replace('\u200c', ' ').replace('\n', ' ')
                    except:
                        weibo_dict['content'] = item['text'].replace('\u200b', ' ').replace('\u200e', ' ').replace('\u200c', ' ').replace('\n', ' ')
                    weibo_dict['source'] = item['source']
                    weibo_dict['favorited'] = item['favorited']
                    weibo_dict['truncated'] = item['truncated']
                    try:
                        location_data = item['user']['location']
                    except:
                        location_data = ''
                    try:
                        weibo_dict['province_name'] = location_data.split(' ')[0]
                        weibo_dict['address'] = location_data.split(' ')[1]
                    except:
                        weibo_dict['province_name'] = location_data
                        weibo_dict['address'] = ''
                    # print(weibo_dict['province_name'], weibo_dict['address'])
                    try:
                        weibo_dict['pinyin'] = item['pinyin']
                    except:
                        weibo_dict['pinyin'] = ''
                    weibo_dict['uid'] = str(item['user']['id'])
                    try:
                        weibo_dict['screen_name'] = item['user']['screen_name']
                    except:
                        weibo_dict['screen_name'] = ''
                    try:
                        weibo_dict['name'] = item['user']['name']
                    except:
                        weibo_dict['name'] = ''
                    try:
                        weibo_dict['province'] = item['user']['province']
                    except:
                        weibo_dict['province'] = ''
                    try:
                        weibo_dict['city'] = item['user']['city']
                    except:
                        weibo_dict['city'] = ''
                    try:
                        weibo_dict['location'] = item['user']['location']
                    except:
                        weibo_dict['location'] = ''
                    try:
                        weibo_dict['gender'] = item['user']['gender']
                    except:
                        weibo_dict['gender'] = ''
                    try:
                        weibo_dict['allow_all_act_msg'] = item['user']['allow_all_act_msg']
                    except:
                        weibo_dict['allow_all_act_msg'] = ''
                    try:
                        weibo_dict['geo_enabled'] = item['user']['geo_enabled']
                    except:
                        weibo_dict['geo_enabled'] = ''
                    try:
                        weibo_dict['verified'] = item['user']['verified']
                    except:
                        weibo_dict['verified'] = ''
                    try:
                        weibo_dict['verified_reason'] = item['user']['verified_reason']
                    except:
                        weibo_dict['verified_reason'] = ''
                    weibo_dict['likes'] = item['attitudes_count']
                    try:
                        weibo_dict['views'] = item['views']
                    except:
                        weibo_dict['views'] = ''
                    try:
                        weibo_dict['retweeted_status'] = str(item['retweeted_status'])
                    except:
                        weibo_dict['retweeted_status'] = ''
                    weibo_dict['reposts_count'] = item['reposts_count']
                    weibo_dict['comments_count'] = item['comments_count']
                    weibo_dict['attitudes_count'] = item['attitudes_count']
                    weibo_dict['visible'] = str(item['visible'])
                    weibo_dict['pic_ids'] = str(item['pic_ids'])
                    try:
                        weibo_dict['ad'] = item['ad']
                    except:
                        weibo_dict['ad'] = ''
                    weibo_dict['isLongText'] = item['isLongText']
                    weibo_dict['url'] = 'http://m.weibo.cn/' + str(item['user']['id']) + '/' + str(item['idstr'])
                    try:
                        weibo_dict['followers_count'] = item['user']['followers_count']
                    except:
                        weibo_dict['followers_count'] = ''
                    try:
                        weibo_dict['favourites_count'] = item['user']['favourites_count']
                    except:
                        weibo_dict['favourites_count'] = ''
                    try:
                        weibo_dict['friends_count'] = item['user']['friends_count']
                    except:
                        weibo_dict['friends_count'] = ''
                    try:
                        weibo_dict['statuses_count'] = item['user']['statuses_count']
                    except:
                        weibo_dict['statuses_count'] = ''
                    try:
                        weibo_dict['bi_followers_count'] = item['user']['bi_followers_count']
                    except:
                        weibo_dict['bi_followers_count'] = ''
                    try:
                        weibo_dict['avatar_large'] = item['user']['avatar_large']
                    except:
                        weibo_dict['avatar_large'] = ''
                    try:
                        weibo_dict['avatar_hd'] = item['user']['avatar_hd']
                    except:
                        weibo_dict['avatar_hd'] = ''
                    try:
                        weibo_dict['retweeted_time'] = item['retweeted_status']['created_at']
                    except:
                        weibo_dict['retweeted_time'] = ''
                    try:
                        weibo_dict['retweeted_post_id'] = item['retweeted_status']['id']
                    except:
                        weibo_dict['retweeted_post_id'] = ''
                    try:
                        weibo_dict['retweeted_author'] = item['retweeted_status']['in_reply_to_screen_name']
                    except:
                        weibo_dict['retweeted_author'] = ''
                    try:
                        weibo_dict['retweeted_author_id'] = item['retweeted_status']['in_reply_to_status_id']
                    except:
                        weibo_dict['retweeted_author_id'] = ''
                    try:
                        weibo_dict['profile_url'] = item['user']['profile_url']
                    except:
                        weibo_dict['profile_url'] = ''
                    try:
                        weibo_dict['domain'] = item['user']['domain']
                    except:
                        weibo_dict['domain'] = ''
                    try:
                        weibo_dict['user_url'] = item['user']['domain']
                    except:
                        weibo_dict['user_url'] = ''
                    weibo_dict['author_url'] = 'http://m.weibo.cn/' + str(item['user']['id'])
                    weibo_dict['tags'] = self.parse_tags(weibo_dict)

                    # 图片列表判断
                    img_list = item['pic_ids']
                    if len(img_list) == 0:
                        weibo_dict['imageurl'] = ''
                        weibo_dict['audiourl'] = ''
                    else:
                        weibo_img = []
                        original_pic = item['original_pic'].split('large/')[0] + 'large/'
                        for img in img_list:
                            img_data = original_pic + img + '.jpg'
                            weibo_img.append(img_data)
                        weibo_dict['imageurl'] = weibo_img
                        weibo_dict['audiourl'] = ''

                    # print(weibo_dict['imageurl'])
                    self.write_goods_jsonfile(weibo_dict)
                    index_num = data_list.index(item)
                    if index_num == len(data_list) - 1:
                        # print(index_num)
                        last_time = self.changetime(data_list[int(index_num)]['created_at'])
                        last_date = self.changetime(data_list[int(index_num)]['created_at']).split(' ')[0]
                        # print(last_time)
                        # print(last_date)
                        if self.start_time <= last_date:
                            # a = "2019-10-27 23:37:07"
                            # 将其转换为时间数组
                            timeArray = time.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                            # 转换为时间戳:
                            timeStamp1 = int(time.mktime(timeArray))
                            # print('最后一个时间%s转换成时间戳是: ' % last_time, timeStamp)
                            self.parse_weibo_data(weibo_dict, uid, timeStamp1)
                        if self.start_time > last_date:
                            is_break = True
                    if is_break:
                        break
        except:
            print(22222222222222222222, traceback.format_exc())

    # 请求获取tags
    def parse_tags(self, weibo_dict):
        try:
            # 请求接口需要的携带参数
            data = {'access_token': code,  # 访问许可
                    'uids': '{}'.format(weibo_dict['uid']),  # 搜索关键词
                    }
            url = 'https://c.api.weibo.com/2/tags/tags_batch/other.json'  # 接口链接
            try:
                time.sleep(0.1)
                response = requests.get(url, data, timeout=30)
            except:
                try:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
                except:
                    time.sleep(0.1)
                    response = requests.get(url, data, timeout=30)
            # print(response.text)
            if 'tags' in response.text:
                tags = re.search(r'"tags":\[{.*?"}\]', response.text).group().replace('"tags":', '')
                return tags
            else:
                return ''
        except:
            print(555555555555555555555555, traceback.format_exc())

    # 写入json文件
    def write_goods_jsonfile(self, item):
        # print(item)
        item_data = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.hdfsclient.new_write('/user/cspider_daily/nike_daily/weibo/{}/104_{}_weibo_nike_uid.json'.format(time.strftime('%Y%m%d'), time.strftime('%Y%m%d')), item_data, encoding='utf-8')
        # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # with open('./104_new_weibo_uid_{}.json'.format(time.strftime('%Y%m%d')), 'ab') as f:
        #     f.write(item.encode("utf-8"))

    def run(self, keyword):
        print(keyword)
        self.parse_age(keyword)


def app_run(keyword):
    # lock = threading.Lock()
    spider = Spider()
    try:
        spider.run(keyword)
    except:
        # logger.error('pid={}\n错误为{}'.format(str(os.getpid()), str(traceback.format_exc())))
        print(traceback.format_exc())


if __name__ == "__main__":
    t0 = time.time()
    keyword_list = pd.read_excel(keyword_file)['官方微博id'].tolist()  # 关键词列表
    pool = multiprocessing.Pool(processes=1)
    for keyword in keyword_list:
        # print(keyword)
        pool.apply_async(app_run, args=(keyword, ))
    pool.close()
    pool.join()
    print(time.time() - t0)


