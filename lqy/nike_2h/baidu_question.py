from lxml import etree
import datetime
import time, json
import chardet,re
import requests
import logging
from datetime import timedelta
now_table = str(datetime.datetime.now()).split(' ')[0]

ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)

import sys
canss = sys.argv[1]
from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()- timedelta(hours=2)).split(' ')[0].replace('-', '')

s_dailt_date =str(datetime.datetime.now()).split(' ')[0]

st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1

# 通过系统时间自动计算时间间隔 ---------------------------------------------------------
date = datetime.datetime.now() - timedelta(days=2)
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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'zhidao.baidu.com',
    'Cookie': 'BAIDUID=65631D05AF7950A120C3CE8F4DAFF4CC:FG=1',

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    'Upgrade-Insecure-Requests': '1'
}

from urllib import parse

def run(url,pn,word):
    print(word)
    word = parse.quote(word)
    url = url%(word,pn)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Host': 'zhidao.baidu.com',
        'Referer': url,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    }

    response = requests.get(url, timeout=10,headers=headers,proxies=proxies)
    response.encoding='gbk'
    html1 = etree.HTML(response.text)
    time_is = html1.xpath('//*[@class="dd explain f-light"]/span[1]/text()')
    urls = html1.xpath('//*[@class="list"]/dl/dt/a/@href')

    num = 0
    for j in urls:
        if len(urls)<10:
            return 'gun'
        # try:
        # print(j)
        headers1 = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Host': 'zhidao.baidu.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        }
        # headers1 = {
        #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        #     'Accept-Encoding': 'gzip, deflate, br',
        #     'Accept-Language': 'zh-CN,zh;q=0.9',
        #     'Cache-Control': 'max-age=0',
        #     'Connection': 'keep-alive',
        #     'Host': 'zhidao.baidu.com',
        #     'Referer': '%s' % j,
        #     'Cookie': 'BAIDUID=65631D05AF7950A120C3CE8F4DAFF4CC:FG=1',
        #
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        #     'X-ik-ssl': '1',
        #     'X-Requested-With': 'XMLHttpRequest'
        # }
        # 采集浏览数
        qu_id = j.split('.html')[0].split('question/')[1]
        # response123 = requests.get('https://zhidao.baidu.com/api/qbpv?q=%s'%qu_id,timeout=10,headers=headers1)
        comments_view = ''
        #  详情页
        response = requests.get(j, timeout=10, headers=headers1,proxies=proxies)
        response.encoding = 'gbk'
        html = etree.HTML(response.text)
        # 标题
        title = re.findall(r'<title>(.*?)_百度知道</title>', response.text, re.S | re.M)[0]
        # 内容
        try:
            con_text = html.xpath('//*[@class="con conReal"]/text()')
            content = clean(con_text[0])
        except:
            content = ''
        #回复数
        comments_count = re.findall(r'<span class="question-all-answers-title"(.*?)</span>', response.text, re.S | re.M)[0]
        comments_coun = int(comments_count.split('>')[1].replace('个回答',''))
        # 下一页
        topic_id = j.split('.html')[0].split('/')[-1]
        que_dates = []

        get_xia(qu_id,comments_coun,word,que_dates)
        ping(qu_id,j,word,que_dates)
        complete_date = list(set(que_dates))
        complete_date.sort()
        if ':' in complete_date[0]:
            complete_date[0] = s_dailt_date

        caree = {"platform": "百度知道", "date": complete_date[0], "time": '', "author": '', "title": ''.join(title), "content": content,
                 "url": j, "is_topics": '是', "floor": '',"topic_date":complete_date[0],"topic_time":'',
                 "keyword": word, 'comments_count': comments_coun, 'views': comments_view, "likes": '',
                 "topic_id":qu_id,"author_id":topic_id,"dislikes":'',"content_id":qu_id,"file_code":'48',"reposts_count":''}
        data = json.dumps(caree, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_2h/qa/%s/%s/48_%s_%s_baidu_question.json' % (
            daily_date, st_3,st_3, sjc), data,
                       encoding='utf-8')
        num += 1


def get_xia(qu_id,comments_coun,word,que_dates):
    pages = int(comments_coun) // 5
    for ss in range(pages):
        xia_page = 'https://zhidao.baidu.com/question/%s?sort=11&rn=5&pn=%s' % (qu_id, str((ss + 1) * 5))

        ping(qu_id,xia_page,word,que_dates)


def ping(qu_id,j,word,que_dates):
    headers1 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Host': 'zhidao.baidu.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    }
    response = requests.get(j, timeout=10, headers=headers1,proxies=proxies)
    response.encoding = 'gbk'
    html = etree.HTML(response.text)
    title = re.findall(r'<title>(.*?)_百度知道</title>', response.text, re.S | re.M)[0]

    ping_con_text = html.xpath('//*[@class="line content"]/div[1]')
    ping_autor = html.xpath(
        '//*[@class="line content"]/../../div[2]/span[2]/text() | //*[@class="line content"]/../../div[1]/span[2]/text()')
    ping_time = html.xpath(
        '//*[@class="line content"]/../../div[2]/span[3]//text() | //*[@class="line content"]/../../div[1]/span[3]//text() ')
    ping_timess = re.findall(r'\d\S+', ''.join(ping_time))
    sj_pingtime = []

    for dste in ping_timess:
        if '-' in dste or ':' in dste:
            sj_pingtime.append(dste)
    nums = 0
    likes = html.xpath('//*[@class="wgt-eva "]/span/@data-evaluate | //*[@class="wgt-eva"]/span/@data-evaluate')
    # comments_count = html.xpath('//*[@class="wgt-best-operator clearfix f-aid "]/span[1]//text() | //*[@class="wgt-best-operator clearfix f-aid"]/span[1]//text() | //*[@class="wgt-answers-operator f-aid "]/span[1]//text() | //*[@class="wgt-answers-operator f-aid"]/span[1]')
    like_num = 0
    disk_num =  1
    #topic_id = html.xpath('//*[@class="line content"]/../../div[2]/@id | //*[@class="line content"]/../../div[1]/@id')
    topic_id = html.xpath('//*[@class="line content"]/../../div[2]/span[2]/@data-href | //*[@class="line content"]/../../div[1]/span[2]/@data-href')


    for o in ping_autor:
        #try:
            #comments_countsss = comments_count[nums].xpath('./em/text()')
        # except:
        #     comments_countsss = ''
        try:
            imgurl = ping_con_text[nums].xpath('.//img/@src')
        except:
            imgurl = ''
        contents = etree.tostring(ping_con_text[nums], method='html', encoding='utf-8')
        ccccc = contents.decode()
        ccccc = clean(ccccc)
        try:
            if o == "热心网友":
                author_id = ' '
            else:
                if 'uid' in topic_id[nums]:
                    author_id = topic_id[nums].split('uid=')[-1].split('&')[0]
                else:
                    author_id = topic_id[nums].split('=')[-1]
        except:
            author_id = ''
        que_dates.append(sj_pingtime[nums])


        care = {"platform": "百度知道", "date": sj_pingtime[nums], "time": '', "author": o, "title": ''.join(title),
                "content": ccccc,"topic_date":'',"topic_time":'',
                "url": j, "is_topics": '否', "floor": '',
                "keyword": word, 'comments_count': '', 'views': '', "likes": likes[like_num],
                "topic_id":qu_id,"author_id":author_id,"dislikes":likes[disk_num],"content_id":'',"file_code":'48',"reposts_count":'',
                "audiourl": '', "imageurl": imgurl}
        data = json.dumps(care, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_2h/qa/%s/%s/48_%s_%s_baidu_question.json' % (
            daily_date, st_3,st_3, sjc), data,
                       encoding='utf-8')
        nums += 1
        like_num += 2
        disk_num += 2


def clean(x):
    s3 = re.compile((r"<br>"), re.I | re.S)
    s6 = re.compile((r"<p>"), re.I | re.S)
    s0 = re.compile((r"</div></div>"), re.I | re.S)
    s4 = re.compile((r'<div class=\\"app-phone-read\\">.*?<div class=\\"post_width\\">'), re.I | re.S)
    s5 = re.compile((r'<span role=\\"postTime\\">.*?<div class=\\"post_width\\">'), re.I | re.S)
    s1 = re.compile((r"<[0-9a-zA-Z\w@\\/].*?>"), re.I | re.S)
    s2 = re.compile((r"& ?nbsp;"), re.I | re.S)
    x = re.sub(s3, '\n', str(x))
    x = re.sub(s6, '\n', str(x))
    x = re.sub(s0, '\n', str(x))
    x = re.sub(s4, '', str(x))
    x = re.sub(s5, '', str(x))
    x = re.sub(s1, '', str(x))
    x = re.sub(s2, ' ', str(x))
    return x

def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'


def main():
    clean_deily()

    hdfs.makedirs('/user/cspider_daily/nike_2h/qa/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹
    h1 = time.time()
    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    for words in lines:

        h2 = time.time()
        h3 = h2 - h1
        if int(h3) > int(canss) * 3600:
            break

        pn = 0
        for oo in range(4):
            i = 'https://zhidao.baidu.com/search?word=%s&date=2&pn=%s'
            try:
                word = run(i,str(pn),words.strip())
                if word == 'gun':
                    break
            except:
                pass
            pn += 10
main()