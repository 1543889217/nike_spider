#coding=UTF-8
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
from multiprocessing import Queue, Process
import weibo_clean

CurrentPath = os.path.abspath(__file__)
SuperiorPath = os.path.dirname(CurrentPath)
FatherPath = os.path.dirname(SuperiorPath)
sys.path.insert(0, FatherPath)

code = '2.00o4_w1HrAaeYBedf38e38b8SnITmD'
APP_KEY = "1428199813"
APP_SECRET = "c25e5caf32f3b72319867990e59c2b1f"
keyword_file = os.path.join(SuperiorPath, '微博补采关键词_daily.xlsx')


data = {'access_token': code,  # 访问许可
        'q': '',  # 搜索关键词
        'sort': 'time',
        'endtime': 1212656000,  # 采集终止点'2018-11-26 00:00:00'
        'dup': 0,  # 是否去除相似内容，1为去除
        'antispam': 0,  # 是否反垃圾（不显示低质量数据），0：否、1：是，默认为1。
        'count': 50
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
                # with open('./error.log', 'a', encoding='utf-8') as f:
                #     f.write(content)
                print(traceback.format_exc())
            return res
        except:
            print("request error")
            time.sleep(1)
    else:
        return res


def request_loop(st, kw):
    res = getdata(st, str(kw))
    temp_df = pd.DataFrame()
    if 'error' in res:
        lasttime = 0
    elif isinstance(res, list):
        lasttime = st - 3600
    elif isinstance(res, dict) and len(res) == 0:
        lasttime = st - 3600
    else:
        try:
            result = res['statuses']

            if len(result) == 0:
                lasttime = 0
            else:
                temp_df = pd.DataFrame(result)
                lasttime = changetime(result[-1]['created_at'])
                temp_df = weibo_clean.clean(temp_df, kw)
            if lasttime == st:
                lasttime = lasttime - 1
        except KeyError:
            lasttime = st - 3600
            print(res.keys())
            print(res)
        except:
            lasttime = st - 3600
            print(traceback.format_exc())
    return lasttime, temp_df


#  上次结束时间即为起始时间，直到到达采集终止时间
def mk_request(keyword_list, date, lock, inque):
    while True:
        with lock:
            if len(keyword_list) == 0:
                break
            else:
                kw = keyword_list.pop()
        st = int(time.mktime(time.strptime(date, '%Y%m%d')))  # 自定义起始时间 '2018-11-27 00:00:00'
        et = st - 86400  # 自定义终止时间 '2018-11-26 00:00:00'
        tmp_combine_df = pd.DataFrame()
        while st > et:
            lasttime, temp_df = request_loop(st, kw)
            tmp_combine_df = tmp_combine_df.append(temp_df)
            st = lasttime
        inque.put((kw, tmp_combine_df))


# 请求age
age_api_useful = True


def birth(uid=6604271961):
    global age_api_useful
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
            time.sleep(1)
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
    elif 'error' in j:
        if j['error'] == 'exceed call times limited.':
            age_api_useful = False
            return ''
    else:
        print(j)
        return ''


# 多线程请求年龄
def multi_birth(lock):
    global uid_list, age_dict
    while True:
        with lock:
            if len(uid_list) == 0:
                break
            uid = uid_list.pop()
        if age_api_useful:
            age = birth(uid)
        else:
            age = ''
        with lock:
            # print(uid, age)
            age_dict[uid] = age


def add_age(df):
    global uid_list, age_dict
    df['uid'].astype('int', inplace=True)
    uid_list = list(df['uid'].unique())
    uid_list=uid_list[:40000]
    age_dict = {}

    lock = threading.Lock()
    ts = []
    # 多线程请求年龄
    for _ in range(25):
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


# def upload2hdfs(local_filepath):
#     # 输出json到hdfs
#     client = hdfs.InsecureClient(url='http://192.168.1.205:14000', user='dpp-executor')
#     # 输出json文件到hdfs
#     output_dir = f'/user/cspider_daily/nike_daily/weibo/{date}'
#     hdfs_path = f'/user/cspider_daily/nike_daily/weibo/{date}/104_{date}_nike_weibo.json'
#
#     for _ in range(3):
#         try:
#             # 确认是否有对应日期路径，没有则创建
#             if not client.status(output_dir, strict=False):
#                 client.makedirs(output_dir)
#             else:
#                 # 如果hdfs文件已存在删除对应文件
#                 if client.status(hdfs_path, strict=False):
#                     print(f'hdfs文件已存在，删除该文件')
#                     client.delete(hdfs_path)
#             client.upload(output_dir, local_filepath)
#             break
#         except:
#             print('weibo 24h 上传失败，重新尝试')
#             continue
#     else:
#         raise RuntimeError('weibo 24h 上传失败')


def write(inque, local_filepath):
    while True:
        kw, df = inque.get()
        if df is None:
            break
        # 去重
        print(f'{kw} 下载 {len(df)} 条微博')
        if len(df) == 0:
            continue
        df.drop_duplicates('idstr', inplace=True)

        # 添加年龄
        df = add_age(df)
        # 添加tags用户标签
        df = get_tag(df)

        #  输出json文件
        df.fillna('', inplace=True)
        df = df.to_dict(orient='records')
        df = [json.dumps(line, ensure_ascii=False) for line in df]

        with open(local_filepath, 'a', encoding='utf-8') as f:
            for line in df:
                f.write(line + '\n')


def main(date):
    keyword_list = pd.read_excel(keyword_file)['关键词'].tolist()
    inque = Queue()

    # 写出到本地
    local_dir = f'Z:/田玉川/微博'
    # local_dir = f'./json_data'
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    timeArray = time.strptime(date, "%Y%m%d")
    # 转换为时间戳: int(time.mktime(timeArray)) - 1800
    timeArray1 = time.localtime(int(time.mktime(timeArray)) - 86400)
    file_date = time.strftime("%Y%m%d ", timeArray1)  # 抓取开始时间
    local_filepath = os.path.join(local_dir, f'104_{file_date}_微博数据_想出去.json')
    # 情况原来的数据
    with open(local_filepath, 'w', encoding='utf-8') as f:
        pass

    # 开启写出进程
    p = Process(target=write, args=(inque, local_filepath))
    p.start()

    # 多线程采集
    lock = threading.Lock()
    pool = []
    for _ in range(15):
        t = threading.Thread(target=mk_request, args=(keyword_list, date, lock, inque,))
        t.start()
        pool.append(t)
        time.sleep(1)
    for t in pool:
        t.join()

    inque.put((None, None))
    while not inque.empty():
        time.sleep(2)
    print('下载完成')
    # 上传到hdfs
    # upload2hdfs(local_filepath)


# def parse_args():
#     parser = argparse.ArgumentParser()
#     # 24小时任务获取对应日期
#     date = time.strftime('%Y%m%d', time.localtime(time.time()))
#     parser.add_argument('-d', '--date', default=date, help='set date for 24h misson')
#     args = parser.parse_args()
#     return args


if __name__ == '__main__':
    t0 = time.time()
    # date_list = ['20200407', '20200406', '20200405', '20200404', '20200403', '20200402', '20200401', '20200331', '20200330', '20200329', '20200328', '20200327', '20200326', '20200325', '20200324', '20200323', '20200322', '20200321', '20200320', '20200319', '20200318', '20200317', '20200316', '20200315', '20200314', '20200313', '20200312', '20200311', '20200310', '20200309']
    # print(len(date_list))
    # for date in date_list:
    date = '{}'.format(time.strftime('%Y%m%d'))
    print(f'{date} 开始下载微博数据')
    main(date)
    print(time.time() - t0)
