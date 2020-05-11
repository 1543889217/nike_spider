import mitmproxy.http
from mitmproxy import ctx, http
import json
import time
from datetime import datetime
from datetime import timedelta
import traceback
import redis
from config import device_name_list

# date = datetime.now() - timedelta(days=30)
# news_start_time = str(date).split(' ')[0]
# yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
# yesterday = str(yesterday).split(' ')[0]
# print('爬取时间段：{}到{}'.format(news_start_time, yesterday))
#
#
# # 定义开始时间 y-m-d  离现在时间远
# # str_start_time = news_start_time
# start_time = '2019-03-23'
# # # 定义结束时间 y-m-d  离现在时间近
# # str_end_time = yesterday
# end_time = '2019-04-28'
# # print(11111, end_time, start_time)

pool = redis.ConnectionPool(host='127.0.0.1')  # 实现一个连接池

redis_example = redis.Redis(connection_pool=pool)


is_work = True


def write_new(item):
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    print('写入视频数据中......')
    with open('./131_douyin_video_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_')), 'ab') as f:
        f.write(item.encode("utf-8"))

def write_comment(item):
    print('写入评论数据中......')
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    with open('./132_douyin_video_comment_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_')), 'ab') as f:
        f.write(item.encode("utf-8"))

def write_daren_info(item):
    print('写入达人数据中......')
    item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    with open('./da_ren_info.json', 'ab') as f:
        f.write(item.encode("utf-8"))
title_item = {}

def response(flow: mitmproxy.http.HTTPFlow):
    # # 定义开始时间 y-m-d  离现在时间远
    # str_start_time = news_start_time
    start_time = '2019-03-23'
    # # 定义结束时间 y-m-d  离现在时间近
    # str_end_time = yesterday
    end_time = '2019-04-28'


    if 'aweme/v1/general/search/single' in flow.request.url or 'aweme/v1/search/item' in flow.request.url:

        headers_li = flow.request.headers
        user_agent = headers_li['user-agent']
        print(user_agent)
        # if 'MI 6' in user_agent:
        #     keyword = redis_example.get('MI 6').decode('utf8')
        #     print(11111, keyword)
        #
        # elif 'OPPO R11' in user_agent:
        #     keyword = redis_example.get('OPPO R11').decode('utf8')
        #     print(11111, keyword)
        # else:
        #     keyword = ''
        keyword = ''
        headers_url = flow.request.url

        # ------------------------------------
        for li in device_name_list:
            device_id = li['device_id']
            if device_id in headers_url:
                keyword = redis_example.get(device_id).decode('utf8')
                print(device_id, ':关键词：', keyword)
                break
        # ------------------------------------

        # if keyword == '奔驰 GLE':
        #     return

        text = flow.response.text

        dict_text = json.loads(text)

        # new_keyword = get_keyword()
        try:
            data_list = dict_text['data']
            for data in data_list:  # 视频
                # print(data)
                # print('----------------------------------------------------------------------------------')
                try:
                    item = {}
                    item['platform'] = '抖音'
                    title = data['aweme_info']['desc']
                    item['title'] = title
                    author = data['aweme_info']['author']['nickname']
                    item['source_author'] = author
                    item['author_id'] = data['aweme_info']['author']['uid']
                    item['description'] = ''  # 视频描述
                    item['followers_count'] = ''  # 粉丝数
                    item['clicks'] = ''  # 点击量
                    item['play'] = ''  # 播放量
                    item['url'] = ''
                    item['categroy'] = ''
                    comment_count = data['aweme_info']['statistics']['comment_count']
                    item['comments_count'] = comment_count
                    likes = data['aweme_info']['statistics']['digg_count']
                    item['likes'] = likes
                    aweme_id = data['aweme_info']['statistics']['aweme_id']
                    item['topic_id'] = aweme_id
                    create_time = data['aweme_info']['create_time']
                    # #转换成localtime
                    time_local = time.localtime(float(create_time))
                    # 转换成新的时间格式(2016-05-05 20:28:54)
                    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                    item['date'] = dt.split(' ')[0]
                    item['time'] = dt.split(' ')[1]
                    item['keyword'] = keyword
                    item['file_code'] = '131'
                    item['reposts_count'] = ''

                    news_start_time = '2019-09-09'
                    start_time_get = datetime.now() - timedelta(days=2)  # 昨天时间
                    start_time_get = str(start_time_get).split(' ')[0]

                    yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
                    yesterday = str(yesterday).split(' ')[0]
                    # 做时间判断部分---------------
                    get_news_time = time.mktime(time.strptime(dt.split(' ')[0], "%Y-%m-%d"))
                    # print(news_start_time, yesterday)
                    end_time = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))
                    start_time = time.mktime(time.strptime(start_time_get, "%Y-%m-%d"))
                    if float(start_time) <= float(get_news_time) <= float(end_time):
                        write_new(item)
                        title_item[aweme_id] = [title, dt.split(' ')[0], dt.split(' ')[1]]

                except KeyError:
                    print('无用信息......')
        except KeyError:  # json数据的结构不同
            data_list = dict_text["aweme_list"]
            for data in data_list:  # 视频
                # print(data)
                # print('----------------------------------------------------------------------------------')
                try:
                    item = {}
                    item['platform'] = '抖音'
                    title = data['desc']
                    item['title'] = title
                    author = data['author']['nickname']
                    item['source_author'] = author
                    item['author_id'] = data['author']['uid']
                    item['description'] = ''  # 视频描述
                    item['followers_count'] = ''  # 粉丝数
                    item['clicks'] = ''  # 点击量
                    item['play'] = ''  # 播放量
                    item['share_count'] = data['statistics']['share_count']  # 分享量
                    item['url'] = ''
                    item['categroy'] = ''
                    comment_count = data['statistics']['comment_count']
                    item['comments_count'] = comment_count
                    likes = data['statistics']['digg_count']
                    item['likes'] = likes
                    aweme_id = data['statistics']['aweme_id']
                    item['topic_id'] = aweme_id
                    create_time = data['create_time']
                    # #转换成localtime
                    time_local = time.localtime(float(create_time))
                    # 转换成新的时间格式(2016-05-05 20:28:54)
                    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"

                    item['date'] = dt.split(' ')[0]
                    item['time'] = dt.split(' ')[1]
                    item['keyword'] = keyword
                    item['file_code'] = '131'
                    item['reposts_count'] = ''

                    news_start_time = '2019-09-09'
                    start_time_get = datetime.now() - timedelta(days=2)  # 昨天时间
                    start_time_get = str(start_time_get).split(' ')[0]

                    yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
                    yesterday = str(yesterday).split(' ')[0]
                    # 做时间判断部分---------------
                    get_news_time = time.mktime(time.strptime(dt.split(' ')[0], "%Y-%m-%d"))
                    # print(news_start_time, yesterday)
                    end_time = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))
                    start_time = time.mktime(time.strptime(start_time_get, "%Y-%m-%d"))
                    if float(start_time) <= float(get_news_time) <= float(end_time):

                        write_new(item)
                        title_item[str(aweme_id)] = [title, dt.split(' ')[0], dt.split(' ')[1]]
                except KeyError:
                    print('无用信息......')

    # 评论
    # if 'https://aweme.snssdk.com/aweme/v2/comment/list' in flow.request.url or 'https://api.amemv.com/aweme/v2/comment/list' in flow.request.url or 'aweme-hl.snssdk.com/aweme/v2/comment/list' in flow.request.url:
    if '/aweme/v2/comment/list' in flow.request.url:
        # print(title_item)
        headers_li = flow.request.headers
        # print(flow.request.url)
        user_agent = headers_li['user-agent']
        # print(user_agent)
        # if 'MI 6' in user_agent:
        #     keyword = redis_example.get('MI 6').decode('utf8')
        #     print(11111, keyword)
        #
        # elif 'OPPO R11' in user_agent:
        #     keyword = redis_example.get('OPPO R11').decode('utf8')
        #     print(11111, keyword)
        #
        # else:
        #     keyword = ''
        keyword = ''
# -------------------------------------------
        headers_url = flow.request.url

        # ------------------------------------
        for li in device_name_list:
            device_id = li['device_id']
            if device_id in headers_url:
                keyword = redis_example.get(device_id).decode('utf8')
                print(device_id , ':关键词：', keyword)
                break
# --------------------------------------------
        # if keyword == '奔驰 GLE':
        #     return
        text = flow.response.text
        dict_text = json.loads(text)
        # print(dict_text)
        comment_list = dict_text['comments']
        comment_item = {}
        for comment in comment_list:
            # print(comment)
            try:
                comment_item['platform'] = '抖音'
                aweme_id = comment['aweme_id']
                comment_item['topic_id'] = aweme_id  # 原视频id
                # print(aweme_id)
                try:
                    title = title_item[str(aweme_id)][0]
                    source_date = title_item[str(aweme_id)][1]
                    source_time = title_item[str(aweme_id)][2]
                    # print(11111111111111)
                except KeyError:
                    title = ''
                    source_date = ''
                    source_time = ''
                comment_item['title'] = title
                comment_id = comment['cid']
                comment_item['comment_id'] = comment_id
                likes = comment['digg_count']
                comment_item['likes'] = likes
                create_time = comment['create_time']
                # #转换成localtime
                time_local = time.localtime(float(create_time))
                # 转换成新的时间格式(2016-05-05 20:28:54)
                dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                comment_date = dt.split(' ')[0]
                comment_item['date'] = dt.split(' ')[0]
                comment_item['time'] = dt.split(' ')[1]
                comment_item['source_date'] = source_date
                comment_item['source_time'] = source_time

                # date_now = datetime.now() - timedelta(days=7)
                # news_start_time = str(date_now).split(' ')[0]
                news_start_time = '2019-09-09'
                start_time_get = datetime.now() - timedelta(days=1)  # 昨天时间
                start_time_get = str(start_time_get).split(' ')[0]

                yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
                # 做时间判断部分---------------
                get_news_time = time.mktime(time.strptime(comment_date, "%Y-%m-%d"))
                # print(news_start_time, yesterday)
                end_time = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))
                start_time = time.mktime(time.strptime(start_time_get, "%Y-%m-%d"))
                if float(start_time) <= float(get_news_time) <= float(end_time):
                    comment_author = comment['user']['nickname']
                    comment_item['author'] = comment_author
                    comment_author_id = comment['user']['uid']
                    comment_item['author_id'] = comment_author_id
                    comment_item['keyword'] = keyword
                    comment_item['content'] = comment['text']
                    comment_item['comment_url'] = ''
                    comment_item['source_url'] = ''
                    comment_item['comments_count'] = ''
                    comment_item['views'] = ''
                    comment_item['file_code'] = '132'
                    comment_item['reposts_count'] = ''
                    write_comment(comment_item)
            except:
                print(traceback.format_exc())
                continue

    if 'aweme-eagle-hl.snssdk.com/aweme/v1/user' in flow.request.url:
        text = flow.response.text

        dict_text = json.loads(text)

        author = dict_text["user"]["nickname"]  # 作者
        country = dict_text["user"]["country"]  # 国家
        province = dict_text["user"]["province"]  # 省份
        city = dict_text["user"]["city"]    # 城市
        district = dict_text["user"]["district"]  # 地区
        custom_verify = dict_text["user"]["custom_verify"]  # 认证身份
        uid = dict_text["user"]["uid"]  # uid
        birthday = dict_text["user"]["birthday"]  # 生日
        short_id = dict_text["user"]["short_id"]  # 抖音号
        if str(short_id) == '0':
            short_id = dict_text["user"]["unique_id"]  # 抖音号
        dongtai_count = dict_text["user"]["dongtai_count"]  # 动态数
        aweme_count = dict_text["user"]["aweme_count"]  # 作品数
        favoriting_count = dict_text["user"]["favoriting_count"]  # 喜欢数
        avatar = dict_text["user"]["avatar_168x168"]["url_list"]  # 头像
        total_favorited = dict_text["user"]["total_favorited"]  # 获赞数
        following_count = dict_text["user"]["following_count"]  # 关注数
        try:
            mplatform_followers_count = dict_text["user"]["sprint_support_user_info"]['sprint']  # 打榜数
        except:
            mplatform_followers_count = ''

        sprint = dict_text["user"]["follower_count"]  # 粉丝数

        share_url = dict_text["user"]["share_info"]["share_url"]  # url
        try:
            star_billboard_rank = dict_text["user"]["star_billboard_rank"]  # 爱DOU榜
        except:
            star_billboard_rank = ''
        try:
            signature = dict_text["user"]["signature"]  # 签名
        except KeyError:
            signature = ''

        item = {}
        item['平台'] = '抖音'
        item['平台编号'] = ''
        item['作者'] = author
        item['国家'] = country
        item['省份'] = province
        item['城市'] = city
        item['地区'] = district
        item['职业'] = custom_verify
        gender = dict_text['user']['gender']
        if gender == 1:
            gender_str = '男'
        elif gender == 2:
            gender_str = '女'
        else:
            gender_str = ''
        item['性别'] = gender_str
        item['作者编号'] = uid
        item['婚姻'] = ''
        item['年龄'] = ''
        item['生日'] = birthday
        item['抖音号'] = short_id
        item['抖音平台认证时间号'] = ''
        item['平台等级'] = ''
        item['平台积分'] = ''
        item['动态数'] = dongtai_count
        item['作品数'] = aweme_count
        item['喜欢数'] = favoriting_count
        item['头像'] = avatar
        item['获赞数'] = total_favorited
        item['关注数'] = following_count
        item['打榜数'] = mplatform_followers_count
        item['粉丝数'] = sprint
        # item['爱DOU榜'] =
        item['榜单名'] = ''
        item['榜单名次'] = star_billboard_rank
        item['签名'] = signature
        item['url'] = share_url

        write_daren_info(item)








