
import datetime
import time, json,re
import logging
from datetime import timedelta
now_table = str(datetime.datetime.now()).split(' ')[0]
from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[0].replace('-', '')
import requests, json
import hashlib, time
import sys
canss = sys.argv[1]

st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1





import requests
from lxml import etree
import datetime
import time, json, re
import logging
from datetime import timedelta

from with_hdfs import HdfsClient
from lxml import etree


# 通过系统时间自动计算时间间隔 ---------------------------------------------------------
date = datetime.datetime.now() - timedelta(days=1)
news_start_time = str(date).split(' ')[0]
now_time = str(datetime.datetime.now()).split(' ')[0]
logging.info('爬取时间段：{}到{}'.format(news_start_time, now_time))

# --------------------------------------------------------------------------------------
# 抓取前两天的数据的时间
tmp_date_list = news_start_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
news_start_time = time.mktime(dateC.timetuple())

tmp_date_list = now_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
nows_time = time.mktime(dateC.timetuple())


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'cookie': 'cna=PLUNF6Tv8EICATohFCKcca7Y;',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
}
headers1 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'upgrade-insecure-requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36',

}


def main(word):
    # 爬取总页
    # 获取aaid
    times = str(time.time()).split('.')[0]
    srcs = 'GET:/i/s:' + times + ':631l1i1x3fv5vs2dxlj5v8x81jqfs2om'
    m2 = hashlib.md5()
    m2.update(srcs.encode('utf-8'))
    sign = m2.hexdigest()

    url = 'http://api.appsdk.soku.com/i/s?_t_=%s&e=md5&_s_=%s&keyword=%s' % (times, sign, word)
    for i in range(10):
        urls = url + '&ob=1'+'&pg=%s'%str(int(i)+1)
        print(urls)
        res = requests.get(urls)
        data = json.loads(res.text)
        # print(data)

        if data['results'] == []:
            return
        for o in data['results']:
            title = o['title']
            times = o['publish_date']



            tmp_date_list = times.strip().split('-')
            dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
            showtime = time.mktime(dateC.timetuple())
            if showtime < news_start_time:
                return



            userid = o['userid']
            videoid = o['videoid']
            try:
                autor = o['publish_time'].split(' ')[0]
            except:
                autor = ''
            video_url = 'https://v.youku.com/v_show/id_'+videoid+'.html'
            car = {"platform": "优酷视频", "date": times, "time": '', "title": "".join(title), "description": '',
                   "source_author": "".join(autor), "followers_count": '', "clicks": 0, "play": 0,
                   'comments_count': 0, "likes": 0, "keyword": word, "url": video_url, "categroy": '',
                   "topic_id": videoid, "author_id": userid, "file_code": '3', "audiourl": '',
                   "imageurl": ''}  # response2 = requests.get(urls, timeout=10, headers=headers1)
            data = json.dumps(car, ensure_ascii=False) + '\n'
            hdfs.new_write(
                '/user/cspider_daily/nike_daily/video/%s/3_%s_%s_youku.json' % (daily_date, daily_date, sjc),
                data,
                encoding='utf-8')




def match_time(aaids, word,):
    cc = list(set(aaids))
    href_list = sorted(cc, key=aaids.index)
    for j in href_list:
        if len(j) > 50:
            continue
        urls = 'https://' + j.strip()
        response2 = requests.get(urls, timeout=10, headers=headers1)
        html = etree.HTML(response2.text)
        try:
            title = "".join(html.xpath('//*/h1/span//text()'))
        except:
            title = ''
        autor = html.xpath('//*[@class="title-wrap"]/span/text()')
        autor_href = html.xpath('//*[@class="title-wrap"]/@href')
        times = "".join(html.xpath('//*[@class="desc"]/span[1]//text()')).replace('上传于','').strip()
        times123 = clean_date(times)
        tmp_date_list = times123.strip().split('-')
        dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
        showtime = time.mktime(dateC.timetuple())
        if '-' not in times:
            continue
        if showtime < news_start_time:
            return 1

        try:
            autor_id = autor_href.split('/')[-1]

        except:
            autor_id = ''

        topic_id = urls.split('/')[-1].replace('.html','')
        # 请求粉丝
        # 截取作者id
        # 截取视频id
        response2 = requests.get(urls, timeout=10, headers=headers1)

        car = {"platform": "优酷视频", "date": times, "time": '', "title": "".join(title), "description": '',
               "source_author": "".join(autor), "followers_count": '', "clicks": 0, "play": 0,
               'comments_count': 0, "likes": 0, "keyword": word, "url": urls, "categroy": '',
               "topic_id": topic_id, "author_id": autor_id, "file_code": '3',"audiourl": '', "imageurl": ""}    #         response2 = requests.get(urls, timeout=10, headers=headers1)
        data = json.dumps(car, ensure_ascii=False) + '\n'

        hdfs.new_write(
            '/user/cspider_daily/nike_2h/video/%s/%s/3_%s_%s_youku.json' % (daily_date, st_3,st_3, sjc),
            data,
            encoding='utf-8')



def resss_ip():
    while True:
        try:
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Host': 'webapi.http.zhimacangku.com',
                # 'Host': 'http.tiqu.qingjuhe.cn',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
            }
            # 5-25分 500个ip
            url = 'http://http.tiqu.alicdns.com/getip3?num=1&type=2&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
            # url = 'http://http.tiqu.qingjuhe.cn/getip?num=1&type=2&pro=&city=0&yys=0&port=1&pack=29515&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=0&regions='
            ip_pro = requests.get(url, headers=headers)
            if json.loads(ip_pro.text)['code'] == 0:
                break
        except:
            pass
    return json.loads(ip_pro.text)


def clean_date(x):
    now = datetime.datetime.now()
    if str(x).find('昨天') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-1), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('前天') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-2), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('天前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-int(str(x).replace('天前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('小时前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(hours=-int(str(x).replace('小时前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('分钟前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(minutes=-int(str(x).replace('分钟前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('今天') != -1:
        x = str(x).replace('今天', now.strftime('%Y-%m-%d'))
    elif str(x).find('刚刚') != -1:
        x = now.strftime('%Y-%m-%d %H:%M:%S')
    elif str(x).find('月前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-4 * int(str(x).replace('月前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('周前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(weeks=-int(str(x).replace('周前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    return x

def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'

import traceback

if __name__ == '__main__':

    clean_deily()
    hdfs.makedirs('/user/cspider_daily/nike_2h/video/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹
    h1 = time.time()

    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    for i in lines:
        h2 = time.time()
        h3 = h2 - h1
        if int(h3) > int(canss) * 3600:
            break
        try:
            main(i.strip())
        except:
            print(traceback.format_exc())
        # except:
        #     pass

