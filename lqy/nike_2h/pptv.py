import requests
from lxml import etree
import datetime,time
import json, re
from datetime import timedelta

from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()- timedelta(hours=2)).split(' ')[0].replace('-', '')


ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)
import sys
canss = sys.argv[1]

st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1


header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Host': 'sou.pptv.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
}


def main(word):
    page = 1
    while True:
        url = 'http://sou.pptv.com/s_video?kw=%s&sortType=1&pn=%s' % (word, page)
        print(url)
        res = requests.get(url, headers=header)
        html = etree.HTML(res.text)
        hrefs = html.xpath('//*[@class="news-list"]/ul/li/a/@href')
        title = html.xpath('//*[@class="news-list"]/ul/li/div/h5/@title')
        sj = html.xpath('//*[@class="news-list"]/ul/li/div/p[1]')
        autors = html.xpath('//*[@class="news-list"]/ul/li/div/p[1]/span[last()]/text()')
        views = html.xpath('//*[@class="news-list"]/ul/li/div/p[2]/text()')

        if hrefs == []:
            return
        num = 0
        for i in hrefs:
            # 时间
            sjs = etree.tostring(sj[num], method='html', encoding='utf-8')
            sjs1 = re.findall(r'</span>(.*?)</p>', str(sjs), re.S | re.M)[0]
            if len(sjs1) > 4:
                sjs1 = re.findall(r'<span class="shortview-time">(.*?)</span>', str(sjs), re.S | re.M)[0]

            topic_id = i.split('show/')[-1].replace('.html','')
            # 名字
            if '间：' in autors[num]:
                autor = ''
            else:
                autor = autors[num]
            # 播放量
            try:
                if isinstance(int(views[num][:1]), int) == True:
                    view = views[num]
                else:
                    view = ''
            except:
                view = ''

            car = {"platform": "pptv", "date": sjs1, "time": '', "title": title[num], "description": '',
                   "source_author": autor, "followers_count": '', "clicks": view, "play": view,
                   'comments_count': 0, "likes": 0, "keyword": word, "url": i, "categroy": '',
                   "topic_id": topic_id, "author_id": '',"content_id":'',"file_code":'169',"reposts_count":'',"audiourl": '', "imageurl": ""}
            data = json.dumps(car, ensure_ascii=False) + '\n'
            # TODO
            hdfs.new_write('/user/cspider_daily/nike_2h/video/%s/%s/169_%s_%s_pptv.json' % (
                daily_date, st_3,st_3, sjc), data,
                           encoding='utf-8')
            num += 1
        page += 1

def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'


if __name__ == '__main__':

    h1 = time.time()
    clean_deily()
    hdfs.makedirs('/user/cspider_daily/nike_2h/video/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹

    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    for word in lines:
        h2 = time.time()
        h3 = h2 - h1
        if int(h3) > int(canss) * 3600:
            break
        try:
            main(word.strip())
        except:
            pass
