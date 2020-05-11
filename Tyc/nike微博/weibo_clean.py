# -*- coding: utf-8 -*-
# import pickle,os,arrow
import pandas as pd
import numpy as np
from dateutil import parser
from collections import defaultdict
import datetime as dt
import traceback
import time


def time_parse(x):
    x = str(x)
    result = {'date': time.strftime('YYYY-MM-DD'), 'time': time.strftime('hh:mm:ss')}
    try:
        temp_date = parser.parse(x)
        result['date'] = dt.datetime.strftime(temp_date, '%Y-%m-%d')
        result['time'] = dt.datetime.strftime(temp_date, '%H:%M:%S')
        #
        # temp_date = arrow.get(parser.parse(x))
        # result['date'] = temp_date.format('YYYY-MM-DD')
        # result['time'] = temp_date.format('hh:mm:ss')
    finally:
        return result


def user_parse(user):
    user_dict = defaultdict(str)
    result = defaultdict(str)
    try:
        user_dict.update(user)
        adress = user_dict['location'].split(' ')
        result['province_name'] = adress[0]
        if len(adress) > 1:
            result['address'] = adress[1]
        else:
            result['address'] = ''
        result['screen_name'] = user_dict['screen_name']
        result['favourites_count'] = user_dict['favourites_count'] if 'favourites_count' in user_dict else 0
        result['name'] = user_dict['name']
        result['province'] = user_dict['province']
        result['city'] = user_dict['city']
        result['location'] = user_dict['location']
        result['gender'] = user_dict['gender']
        result['allow_all_act_msg'] = user_dict['allow_all_act_msg']
        result['geo_enabled'] = user_dict['geo_enabled']
        result['verified'] = user_dict['verified']
        result['verified_reason'] = user_dict['verified_reason']
        result['followers_count'] = user_dict['followers_count']
        result['uid'] = user_dict['id']
        result['friends_count'] = user_dict['friends_count'] #if 'friends_count' in user_dict else 0
        result['statuses_count'] = user_dict['statuses_count'] #if 'statuses_count' in user_dict else 0
        result['bi_followers_count'] = user_dict['bi_followers_count'] #if 'bi_followers_count' in  user_dict else 0
        result['avatar_large'] = user_dict['avatar_large']
        result['avatar_hd'] = user_dict['avatar_hd']
        result['profile_url'] = user_dict['profile_url'] if 'profile_url' in user_dict else ''
        result['domain'] = user_dict['domain'] if 'domain' in user_dict else ''
        result['user_url'] = user_dict['domain'] if 'domain' in user_dict else ''
    except:
        print(traceback.format_exc())
        print(result)
    finally:
        return dict(result)


def clean(df, kw):
    columns = ['platform', 'keyword', 'date', 'time', 'weibo_id', 'mid', 'idstr', 'content', 'source', 'favorited',
               'truncated', 'province_name', 'address', 'pinyin', 'uid', 'screen_name', 'name', 'province', 'city',
               'location', 'gender', 'allow_all_act_msg', 'geo_enabled', 'verified', 'verified_reason', 'likes', 'views',
               'retweeted_status', 'reposts_count', 'comments_count', 'attitudes_count', 'visible', 'pic_ids', 'ad',
               'isLongText', 'url', 'followers_count', 'favourites_count', 'friends_count', 'statuses_count',
               'bi_followers_count', 'avatar_large', 'avatar_hd', 'retweeted_time', 'retweeted_post_id', 'retweeted_author',
               'retweeted_author_id', 'profile_url', 'domain', 'user_url', 'author_url', 'imageurl', 'audiourl']

    date_df = pd.DataFrame(list(map(lambda x: time_parse(x), df['created_at'])), columns=['date', 'time'])
    user_df = pd.DataFrame(list(map(lambda x: user_parse(x), df['user'])))
    try:
        col_dict = {
            'weibo_id': 'id',
            'mid': 'mid',
            'content': 'text',
            'source': 'source',
            'favorited': 'favorited',
            'truncated': 'truncated',
            'idstr': 'idstr',
            'retweeted_status': 'retweeted_status',
            'reposts_count': 'reposts_count',
            'comments_count': 'comments_count',
            'attitudes_count': 'attitudes_count',
            'likes': 'attitudes_count',
            'visible': 'visible',
            'pic_ids': 'pic_ids',
            'ad': 'ad',
            'pinyin': 'pinyin',
            # 'uid': 'uid',
            'isLongText': 'isLongText',
            'imageurl': 'original_pic',
            'audiourl': ''
        }
    except:
        col_dict = {
            'weibo_id': 'id',
            'mid': 'mid',
            'content': 'text',
            'source': 'source',
            'favorited': 'favorited',
            'truncated': 'truncated',
            'idstr': 'idstr',
            'retweeted_status': 'retweeted_status',
            'reposts_count': 'reposts_count',
            'comments_count': 'comments_count',
            'attitudes_count': 'attitudes_count',
            'likes': 'attitudes_count',
            'visible': 'visible',
            'pic_ids': 'pic_ids',
            'ad': 'ad',
            'pinyin': 'pinyin',
            # 'uid': 'uid',
            'isLongText': 'isLongText',
            'imageurl': '',
            'audiourl': ''
        }
    for key, value in col_dict.items():
        try:
            df[key] = df[value]
        except:
            df[key] = np.nan

    df['keyword'] = kw
    df['platform'] = '微博'
    # try:
    #     df['likes'] = df['url_objects'].map(lambda x: x[0]['like_count'] if len(x) > 0 else 0)
    # except:
    #     # print(traceback.format_exc())
    #     df['likes'] = np.nan
    df['views'] = np.nan
    df = pd.concat([df, date_df, user_df], axis=1, sort=False)
    df['url'] = list(map(lambda x, y: 'http://m.weibo.cn/' + str(x) + '/' + str(y), df['uid'], df['idstr']))
    df['author_url'] = list(map(lambda x: 'http://m.weibo.cn/' + str(x), df['uid']))
    # 转发状态
    df['retweeted_status'].fillna('', inplace=True)
    df['retweeted_time'] = df['retweeted_status'].map(lambda x: x['created_at'] if 'created_at' in x else '')
    df['retweeted_post_id'] = df['retweeted_status'].map(lambda x: x['idstr'] if 'idstr' in x else '')
    df['retweeted_author'] = df['retweeted_status'].map(lambda x: x['in_reply_to_screen_name'] if 'in_reply_to_screen_name' in x else '')
    df['retweeted_author_id'] = df['retweeted_status'].map(lambda x: x['in_reply_to_status_id'] if 'in_reply_to_status_id' in x else '')

    #  对布尔值列进行处理
    for col in ['verified', 'isLongText', 'geo_enabled', 'allow_all_act_msg', 'truncated', 'favorited']:
        label = df[col].map(lambda x: not (isinstance(x, bool)))
        df.loc[label, col] = False

    #  对非散列对象进行处理
    for col in ['retweeted_status', 'visible', 'pic_ids']:
        df[col] = df[col].map(lambda x: str(x))

    df = df[columns]
    return df
