import requests
from lxml import etree
import datetime
import time, json
now_table = str(datetime.datetime.now()).split(' ')[0]

from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')

ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Host': 'v.qq.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    'If-Modified-Since': 'Thu, 06 Sep 2018 02:10:38 GMT',
    'Upgrade-Insecure-Requests': '1'
}

def main(url,word):
    for i in range(20):
        time.sleep(1)
        urls = url+'&stag=3&cur='+str(i+1)+'&cxt=tabid%3D0%26sort%3D0%26pubfilter%3D2%26duration%3D0'
        print(urls)
        response = requests.get(urls, timeout=10, headers=headers)
        html = etree.HTML(response.text)
        href = html.xpath('//*[@class="result_title"]/a/@href')
        times = html.xpath('//*[@class="result_info"]/div[1]/div[1]/span[2]/text()')
        title = html.xpath('//*[@class="result_title"]')

        wirte_json(title, href, times,word)
        if href == []:
            break
def wirte_json(title,href,time,word):

    num = 0
    for j in href:
        # print(j)
        ti = title[num].xpath('string(.)')
        response1 = requests.get(j, timeout=10, headers=headers)
        response1.encoding = 'utf-8'
        html = etree.HTML(response1.text)

        try:
            des = html.xpath('//*[@class="summary _video_summary"]/text()')[0]
        except:
            des = ''
        try:
            autor = html.xpath('//*[@class="user_name"]/text()')[0]
        except:
            autor = ''
        try:
            fans = html.xpath('//*[@class="video_user _video_user"]/a[2]/span/text()')[1]
        except:
            fans = 0
        try:
            autor_id = html.xpath('//*[@class="user_name"]/../@href')[0].split('/')[-1]
        except:
            autor_id = ''
        try:
            view = html.xpath('//*[@id="mod_cover_playnum"]/text()')[0]
        except:
            view = 0
        try:
            timeaa = html.xpath('//*[@class="date _date"]/text()')[0]
            if '年' in timeaa:
                date = timeaa.replace('年','-').replace('月','-').replace('日','').replace('发布','')
            else:
                date = ''
        except:
            date = ''
        topic_id = j.split('.html')[0].split('/')[-1]
        if autor == 'undefined':
            autor = ''
        car = {"platform": "腾讯视频", "date": date, "time": '',"title": ti,"description":des,
               "source_author": autor,"followers_count":fans,"clicks": 0, "play": view,"categroy":'',
               'comments_count': 0, "likes": 0, "keyword": word, "url": j,"topic_id":topic_id,"author_id":autor_id,"content_id":''
               ,"file_code":'4',"reposts_count":"","audiourl":'',"imageurl":''}
        data = json.dumps(car, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_daily/video/%s/4_%s_%s_tecent.json' % (
            daily_date, daily_date, sjc), data,
                       encoding='utf-8')
        num+=1
def clean_date(x):
    now = datetime.datetime.now()
    if str(x).find('昨天')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(days=-1), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('前天')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(days=-2), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('天前')!=-1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-int(str(x).replace('天前',''))), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('小时前')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(hours=-int(str(x).replace('小时前',''))), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('分钟前')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(minutes=-int(str(x).replace('分钟前',''))), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('今天')!=-1:
        x = str(x).replace('今天',now.strftime('%Y-%m-%d'))
    elif str(x).find('刚刚')!=-1:
        x = now.strftime('%Y-%m-%d %H:%M:%S')
    elif str(x).find('月前')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(weeks=-4*int(str(x).replace('月前',''))), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('周前')!=-1:
        x = datetime.datetime.strftime(now+datetime.timedelta(weeks=-int(str(x).replace('周前',''))), '%Y-%m-%d %H:%M:%S')
    return x

if __name__ == '__main__':

    hdfs.makedirs('/user/cspider_daily/nike_daily/video/{}'.format(daily_date)) # 创建每日文件夹

    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    for i in lines:
        url = 'http://v.qq.com/x/search/?q=' +i.strip()+ '&filter=sort%3D1%26pubfilter%3D1%26duration%3D0%26tabid%3D0%26resolution%3D0'
        try:
            main(url,i.strip())
        except:
            try:
                url = 'http://v.qq.com/x/search/?q=' + i.strip() + '&filter=sort%3D1%26pubfilter%3D1%26duration%3D0%26tabid%3D0%26resolution%3D0'

                main(url, i.strip())
            except:
                pass