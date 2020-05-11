# coding=UTF-8
import requests
import pandas as pd
import json
import threading
import hdfs
import math
import os
import traceback
import sys
import time
import argparse

CurrentPath = os.path.abspath(__file__)
SuperiorPath = os.path.dirname(CurrentPath)
FatherPath = os.path.dirname(SuperiorPath)
sys.path.insert(0, FatherPath)
from weibo_api import weibo_clean

code = '2.00o4_w1HrAaeYBedf38e38b8SnITmD'
APP_KEY = "1428199813"
APP_SECRET = "c25e5caf32f3b72319867990e59c2b1f"
keyword_file = os.path.join(SuperiorPath, 'nike_weibo_keywords.xlsx')
data = {'access_token': code,  # 访问许可
        'q': '',  # 搜索关键词
        'sort': 'time',
        'endtime': 1212656000,  # 采集终止点'2018-11-26 00:00:00'
        'dup': 0,  # 是否去除相似内容，1为去除
        'antispam': 0,  # 是否反垃圾（不显示低质量数据），0：否、1：是，默认为1。
        'count': 30
        }  # 每页返回条数 最大50条


def changetime(timestr):
    fmt2 = '%a %b %d  %H:%M:%S %z %Y'
    timestrp = time.strptime(timestr, fmt2)
    temp_time = time.strftime("%Y-%m-%d %H:%M:%S", timestrp)
    # print(f"last time {temp_time}, continue request")
    timestampstr = time.mktime(timestrp)
    return int(timestampstr)


def getdata(st, kw):
    url = 'https://c.api.weibo.com/2/search/statuses/limited.json'  # 接口链接
    data['q'] = kw    # 搜索关键词
    data['endtime'] = st  # 采集终止点
    res = {}
    for _ in range(4):
        try:
            d = requests.get(url, data, timeout=30)
            content = d.content.decode('utf-8')
            try:
                res = json.loads(content, strict=False)
            except:
                print(traceback.format_exc())
            return res
        except:
            print("request error")
            time.sleep(5)
    else:
        return res


def request_loop(st, kw):
    res = getdata(st, str(kw))
    temp_df = pd.DataFrame()
    if 'error' in res:
        lasttime = 0
    else:
        try:
            if isinstance(res, list):
                result = []
            elif isinstance(res, dict):
                result = res['statuses']

            if len(result) == 0:
                lasttime = 0
            else:
                temp_df = pd.DataFrame(result)
                lasttime = changetime(result[-1]['created_at'])
                temp_df = weibo_clean.clean(temp_df, kw)
            if lasttime == st:
                lasttime = lasttime - 1
        except:
            lasttime = st - 1000
            print(traceback.format_exc())
    return lasttime, temp_df


#  上次结束时间即为起始时间，直到到达采集终止时间
def mk_request(keyword_list, df, date, lock):
    while True:
        with lock:
            if len(keyword_list) == 0:
                break
            else:
                kw = keyword_list.pop()
        st = int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))  # 自定义起始时间 '2018-11-27 00:00:00'
        et = st - 9000  # 自定义终止时间 回溯2.5h
        while st > et:
            lasttime, temp_df = request_loop(st, kw)
            with lock:
                df.append(temp_df)
            st = lasttime
            time.sleep(1)
        print(f'{kw} download {len(temp_df)} pages from {date}')


# 请求age
def birth(uid=6604271961):
    url = 'https://c.api.weibo.com/2/users/birthday/other.json'
    data = {}
    data['access_token'] = code
    data['uid'] = uid
    for _ in range(3):
        try:
            d = requests.get(url, data)
            j = d.json()
            break
        except:
            print('try again')
            time.sleep(2)
    else:
        print('request error')
        j = {}

    if 'birthday' in j:
        age = j['birthday']
        try:
            if int(age) >= 1900:
                return age
            else:
                return ''
        except:
            return ''
    else:
        # print(j)
        return ''


# 多线程请求年龄
def multi_birth(lock):
    global uid_list, age_dict
    while True:
        with lock:
            if len(uid_list) == 0:
                break
            uid = uid_list.pop()
        age = birth(uid)
        with lock:
            # print(uid, age)
            age_dict[uid] = age


def add_age(df):
    global uid_list, age_dict
    df['uid'].astype('int', inplace=True)
    uid_list = list(df['uid'].unique())
    age_dict = {}

    lock = threading.Lock()
    ts = []
    # 多线程请求年龄
    for _ in range(15):
        t = threading.Thread(target=multi_birth, args=(lock,))
        ts.append(t)
        t.start()

    for t in ts:
        t.join()

    birth_df = pd.Series(age_dict).reset_index()
    birth_df.columns = ['uid', 'age']
    if 'age' in df.columns:
        df.drop('age', axis=1, inplace=True)
    df = df.merge(birth_df, on='uid', how='left')

    return df


tag_data = {'access_token': code,  # 访问许可
            'uids': ''
            }

def resquest_tag(lock):
    global tag_uid_list, tag_list
    while True:
        with lock:
            if len(tag_uid_list) == 0:
                break
            uids = tag_uid_list.pop()
        uids = [str(i) for i in uids]
        url = 'https://c.api.weibo.com/2/tags/tags_batch/other.json'  # 接口链接
        tag_data['uids'] = ','.join(uids)
        res = []
        try:
            d = requests.get(url, tag_data, timeout=60)
            content = d.content.decode('utf-8')
            content = json.loads(content, strict=False)
            for ll in content:
                if isinstance(ll, dict):
                    if 'tags' in ll:
                        ll['tags'] = json.dumps(ll['tags'], ensure_ascii=False)
                        res.append(ll)
        except Exception as e:
            print(e)
        with lock:
            tag_list.extend(res)


def get_tag(df):
    global tag_uid_list, tag_list
    tag_list = []
    df['uid'].astype('int', inplace=True)
    tag_uid_list = list(df['uid'].unique())
    # uid 按20个一组进行拆分
    nums = int(math.ceil(len(tag_uid_list) / 20.0))
    tag_uid_list = [tag_uid_list[i*20: (i+1)*20] for i in range(nums)]

    lock = threading.Lock()
    ts = []
    # 多线程请求年龄
    for _ in range(8):
        t = threading.Thread(target=resquest_tag, args=(lock,))
        ts.append(t)
        t.start()

    for t in ts:
        t.join()

    tags_df = pd.DataFrame(tag_list, index=range(len(tag_list)))
    if len(tags_df) == 0:
        df['tags'] = ''
        return df
    tags_df = tags_df.rename(columns={'id': 'uid'}).drop_duplicates('uid')
    df = df.merge(tags_df, on='uid', how='left')
    df['tags'].fillna('', inplace=True)
    return df


def main(date_2h, h_range):
    import datetime as dt
    from dateutil import parser
    # global raw_df
    # raw_df = []
    keyword_list = pd.read_excel(keyword_file)['词表'].tolist()
    st_time = parser.parse(f"{date_2h} {h_range.split('_')[0]}:00:00") + dt.timedelta(seconds=3600 * 2 - 1)
    st_time = st_time.strftime('%Y-%m-%d %H:%M:%S')

    # 多线程采集
    df = []
    lock = threading.Lock()
    pool = []
    for _ in range(15):
        t = threading.Thread(target=mk_request, args=(keyword_list, df, st_time, lock))
        t.start()
        pool.append(t)
        time.sleep(1)
    for t in pool:
        t.join()

    # 合并结果数据
    df = pd.concat(df, sort=False)
    df.drop_duplicates('idstr', inplace=True)

    # 添加年龄
    print('添加年龄')
    df = add_age(df)
    print(df['age'].unique())
    # df['age'] = ''
    # 添加tags用户标签
    print('添加用户标签')
    df = get_tag(df)

    #  输出json文件
    df.fillna('', inplace=True)
    df.drop_duplicates('idstr', inplace=True)
    df = df.to_dict(orient='records')
    df = [json.dumps(line, ensure_ascii=False) for line in df]


    # 写出到本地
    local_dir = f'/data/nlpData/weibo_files_24h/{date_2h}/{h_range}'
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    local_filepath = f'/data/nlpData/weibo_files_24h/{date_2h}/{h_range}/104_{date_2h}_{h_range}_nike_weibo.json'
    with open(local_filepath, 'w', encoding='utf-8') as f:
        for line in df:
            f.write(line+'\n')


    # 输出json到hdfs
    client = hdfs.InsecureClient(url='http://192.168.1.209:14000', user='dpp-executor')
    # 输出json文件到hdfs
    output_dir = f'/user/cspider_daily/nike_2h/weibo/{date_2h}/{h_range}'
    hdfs_path = f'/user/cspider_daily/nike_2h/weibo/{date_2h}/{h_range}/104_{date_2h}_{h_range}_nike_weibo.json'

    for _ in range(6):
        try:
            # 确认是否有对应日期路径，没有则创建
            if not client.status(output_dir, strict=False):
                client.makedirs(output_dir)
            else:
                # 如果hdfs文件已存在删除对应文件
                if client.status(hdfs_path, strict=False):
                    print(f'hdfs文件已存在，删除该文件')
                    client.delete(hdfs_path)

            client.upload(output_dir, local_filepath)
            break
        except:
            print('上传失败，重新尝试')
            continue
    else:
        raise RuntimeError(f'weibo 24h 上传失败: {output_dir}')





def parse_args():
    parser = argparse.ArgumentParser()
    # 当前时间回溯4个小时，得到2小时任务的开始时间
    date_2h = time.strftime('%Y%m%d', time.localtime(time.time() - 7200))
    hour0 = int(int(time.strftime('%H', time.localtime(time.time() - 7200))) / 2)
    h_range = str(hour0*2).zfill(2) + '_' + str((hour0 + 1)*2).zfill(2)

    parser.add_argument('-t', '--date_2h', default=date_2h, help='set date for 2h misson')
    parser.add_argument('-r', '--range', default=h_range, help='set time range for 2h misson')
    parser.add_argument('-i', '--isdaily', action='store_false', default=True, help='whether run daily')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    t0 = time.time()
    args = parse_args()
    print(args)
    date_2h = args.date_2h
    h_range = args.range
    print('开始下载微博数据')
    main(date_2h, h_range)
    print(time.time() - t0)