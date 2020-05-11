from lxml import etree
import datetime
import time, json
import requests,re
import logging
from datetime import timedelta
from lxml import etree

now_table = str(datetime.datetime.now()).split(' ')[0]

from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()- timedelta(hours=2)).split(' ')[0].replace('-', '')

import sys
canss = sys.argv[1]

ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)
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

proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
# proxyUser = "HQ60F7PAQBO68GWD"
# proxyUser = "H7307T4706B25G4D"
proxyUser = "HW032H3ON4V96V1D"
# proxyPass = "FBD9D819229DBB1B"
# proxyPass = "05B4877CC39192C0"`
proxyPass = "718746FE4237B37E"

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



headers = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
}


st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1


def main(word,):

    nun = 20
    for j in range(10000):
        url = 'https://www.douban.com/j/search?q=%s&start=%s&cat=1015'%(word,nun)
        print(url)
        print(nun)
        try:
            res = requests.get(url,proxies=proxies,headers=headers,timeout=20)
        except:
            continue
        data = json.loads(res.text)['items']
        if len(data)<=4:
            return
        if nun == 100:
            return
        for i in data:
            sid = re.findall(r'sid: (.*?)qcat', i, re.S | re.M)[0]
            href = 'https://www.douban.com/note/'+sid.split(',')[0]+'/'
            try:
                response = requests.get(href,proxies=proxies,headers=headers,timeout=20)
            except:
                continue
            html1 = etree.HTML(response.text)
            # print(href)
            content = html1.xpath('//*[@class="note"]//text()')
            title = html1.xpath('//h1/text()')
            autor = html1.xpath('//*[@class="note-author"]/text()')
            date = html1.xpath('//*[@class="pub-date"]/text()')
            like = html1.xpath('//*[@class="rec-num"]/text()')
            # print(content)
            # print(title)
            sids = sid.split(',')[0]

            try:
                riqi = "".join(date).split(' ')[0]
                shijian = "".join(date).split(' ')[1]
            except:
                riqi = "".join(date)
                shijian = ''
            caree = {"platform": "豆瓣日记", "date": riqi, "time": shijian, "article_author": "".join(autor),
                     "title": "".join(title), 'content': "".join(content), "content_id": sids,
                     "article_url": href, "clicks": '', "views": '', "comments_count": '',
                     "keyword": word,"topic_date":riqi,"topic_time":shijian,
                     "likes": '', "dislikes": '', "series_url": '', "list_url": '',
                     "article_type_1st": '日记', 'article_type_2nd': '',"reposts_count":"".join(like),
                     "article_source": '', 'insert_time': str(datetime.datetime.now()).split('.')[0],
                     'update_time': str(datetime.datetime.now()).split('.')[0], "author_id": '', "topic_id": sids,"file_code":'174',
                     "audiourl": '', "imageurl": ""
                     }
            data = json.dumps(caree, ensure_ascii=False) + '\n'
            # TODO
            hdfs.new_write('/user/cspider_daily/nike_2h/article/%s/%s/174_%s_%s_douban.json' % (
                daily_date, st_3,st_3, sjc), data,
                           encoding='utf-8')
            try:
                comment(title,response,href,sids,date,riqi,shijian)
            except:
                pass
        nun += 20

        # 评论



        # data = base.split('}]')[0]+'}]'
        # data.replace(':false,','"123"')
        # print(data)
        # print(eval(data))



def comment(title,response,href,topic_id,date,topic_date,topic_time):
    base = re.findall(r" 'comments':(.*?)'start'", response.text, re.S | re.M)[0]
    text = re.findall(r'text":"(.*?)","total', base, re.S | re.M)
    name = re.findall(r'name":"(.*?)","url', base, re.S | re.M)
    create_time = re.findall(r'create_time":"(.*?)","replies', base, re.S | re.M)
    num = 0
    try:
        dates = create_time[num].split(' ')[0]
        times = create_time[num].split(' ')[1]
    except:
        dates = create_time[num]
        times = ''

    for i in text:
        caree = {"platform": "豆瓣评论", "source_date": "".join(date).split(' ')[0],
                 "source_time": "".join(date).split(' ')[1],
                 "date": dates,
                 'time': times,"topic_date":topic_date,"topic_time":topic_time,
                 "title": "".join(title), "comments_count": '', "author": "".join(name[num]),
                 "content": i, "floor": '', "keyword": '',
                 "content_id": '', "topic_id": topic_id,
                 "source_url": href, "comment_url": href, "views": '', "likes": '',
                 "author_id": '', "dislikes": '',
                 'insert_time': str(datetime.datetime.now()).split('.')[0],
                 'update_time': str(datetime.datetime.now()).split('.')[0],"file_code":'181',
                 "reposts_count":'',"audiourl": '', "imageurl": ""}
        data = json.dumps(caree, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_2h/articlecomments/%s/%s/181_%s_%s_douban_comment.json' % (
            daily_date, st_3, st_3,sjc), data,
                       encoding='utf-8')
        num += 1



def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'




if __name__ == '__main__':
    clean_deily()

    hdfs.makedirs('/user/cspider_daily/nike_2h/article/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹
    hdfs.makedirs('/user/cspider_daily/nike_2h/articlecomments/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹
    h1 = time.time()
    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    for word in lines:
        h2 = time.time()
        h3 = h2 - h1
        if int(h3) > int(canss) * 3600:
            break
        try:
            main(word.strip(),)
        except:
            pass

