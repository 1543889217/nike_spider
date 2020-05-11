from lxml import etree
import datetime
import time, json
import requests
import logging
from datetime import timedelta
import traceback
now_table = str(datetime.datetime.now()).split(' ')[0]

ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)



from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')


# 通过系统时间自动计算时间间隔 ---------------------------------------------------------
date = datetime.datetime.now() - timedelta(days=7)
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


def main(word):
    page = 0
    while True:
        data = resss_ip()
        ips = data['data'][0]['ip']
        port = data['data'][0]['port']
        proxies = {
            'http': 'http://' + str(ips) + ':' + str(port),
            'https': 'https://' + str(ips) + ':' + str(port)
        }
        url = 'https://wenwen.sogou.com/cate/tag?q=%s&pno=%s&ch=ww.fly.search' % (word, str(page))
        print(url)
        # res = requests.get(url,proxies=proxies)
        res = requests.get(url)
        html = etree.HTML(res.text)
        hrefs = html.xpath('//*[@class="sort-lst"]/li/a/@href')
        if hrefs == []:
            return
        times = html.xpath('//*[@class="sort-rgt"]/span[2]/text()')
        replays = html.xpath('//*[@class="sort-rgt"]/span[1]/text()')

        # print(times)
        # print(replays)
        # print(hrefs)
        nn = 0
        for i in hrefs:
            urls = 'https://wenwen.sogou.com' + i
            print(urls)
            # res = requests.get(urls,proxies=proxies)
            res = requests.get(urls)
            html = etree.HTML(res.text)
            type = html.xpath('//*[@class="section"]/div/a/text()')
            title = html.xpath('//*[@class="detail-tit"]/text()')
            autor = html.xpath('//*[@class="ft-info-box"]/div[2]//text()')
            autor_id = html.xpath('//*[@class="ft-info-box"]/div[2]/a/@href')
            view_count = html.xpath('//*[@class="ft-info-box"]/div[3]/span[1]/text()')
            timeaa = html.xpath('//*[@class="ft-info-box"]/div[3]/span[2]/text()')[0]
            dates = timeaa.split('提问')[0].strip()
            date = clean_date(dates)
            print(date)
            try:
                tmp_date_list = date.split(' ')[0].strip().split('.')
                dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
                coo_now_time = time.mktime(dateC.timetuple())
            except:
                tmp_date_list = date.split(' ')[0].strip().split('-')
                dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
                coo_now_time = time.mktime(dateC.timetuple())

            if coo_now_time <= news_start_time:
                return
            #  判断时间
            try:
                autor_ids = autor_id.split('&ch')[0].split('uid=')[-1]
            except:
                autor_ids = ''

            try:
                riqi = "".join(date).split(' ')[0]
                shijian = "".join(date).split(' ')[1]
            except:
                riqi = "".join(date)
                shijian = ''

            try:
                topic_id = i.split('.')[0].split('/')[-1]
            except:
                topic_id = i
            caree = {"platform": "搜狗问问", "date": riqi, "time": shijian, "author": ''.join(autor),
                     "title": ''.join(title), "content": ''.join(title),"topic_date":riqi,"topic_time":shijian,
                     "url": urls, "is_topics": '是', "floor": '',
                     "keyword": word, 'comments_count': replays[nn], 'views': ''.join(view_count), "likes": '',
                     "topic_id": topic_id, "author_id": ''.join(autor_ids), "type": ''.join(type), "dislikes": '', "content_id":topic_id,
                     "reposts_count":"","audiourl": '', "imageurl": ""}
            data = json.dumps(caree, ensure_ascii=False) + '\n'
            # TODO
            hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/160_%s_%s_sougou.json' % (
                daily_date, daily_date, sjc), data,
                           encoding='utf-8')
            ask1(html, urls, view_count, title, i,riqi,shijian)
            ask2(html, urls, view_count, title, i,riqi,shijian)

            nn += 1
        page += 1


def ask1(html, urls, view_count, title, i,topic_date,topic_time):
    # 专业回答
    print(1111)

    for j in range(5):
        try:
            autors1 = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/div[1]/a/text()')
        except:
            autors1 = ''
        print(autors1)
        if autors1 == '' or autors1 == []:
            return
        autors_id = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/div[1]/a/@href') % str(j + 1)
        content = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/pre//text())') % str(j + 1)
        date = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/div[2]//text()') % str(j + 1)
        like = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/div[last()-1]/div[last()]/a[1]//text()') % str(j + 1)
        dislikes = html.xpath('//*[@id="bestAnswers"]/div[%s]/div[2]/div[last()-1]/div[last()]/a[2]//text()') % str(
            j + 1)
        dates1 = clean_date(''.join(date).split('回答')[0].strip())
        try:
            riqi = "".join(dates1).split(' ')[0]
            shijian = "".join(dates1).split(' ')[1]
        except:
            riqi = "".join(dates1)
            shijian = ''
        try:
            topic_id = i.split('.')[0].split('/')[-1]
        except:
            topic_id = i
        care = {"platform": "搜狗问问回答", "date": riqi, "time": shijian, "author": ''.join(autors1),
                "title": ''.join(title),"topic_date":topic_date,"topic_time":topic_date,
                "content": ''.join(content),
                "url": urls, "is_topics": '否', "floor": '',
                "keyword": word, 'comments_count': '', 'views': ''.join(view_count), "likes": ''.join(like),
                "topic_id": topic_id, "author_id": ''.join(autors_id), "dislikes": ''.join(dislikes),"reposts_count":"" }
        data = json.dumps(care, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/160_%s_%s_sougou.json' % (
            daily_date, daily_date, sjc), data,
                       encoding='utf-8')


def ask2(html, urls, view_count, title, i,topic_date,topic_time):
    # 其他回答
    # try:
    print(2222)
    for j in range(5):
        try:
            autors2 = html.xpath('//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/div[1]//text()' % str(j + 1))
        except:
            autors2 = ''
        print(autors2)
        if autors2 == '' or autors2 == []:
            return
        autors_id = html.xpath('//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/div[1]/a/@href' % str(j + 1))
        date = html.xpath('//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/div[2]/text()' % str(j + 1))
        content1 = html.xpath('//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/pre[1]//text()' % str(j + 1))
        like = html.xpath(
            '//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/div[last()-1]/div[last()]/a[1]//text()' % str(
                j + 1))
        dislikes = html.xpath(
            '//*[@class="replay-wrap common_answers"]/div[%s]/div[2]/div[last()-1]/div[last()]/a[2]//text()' % str(
                j + 1))
        print(date)
        dates1 = clean_date(''.join(date).split('回答')[0].strip())
        try:
            riqi = "".join(dates1).split(' ')[0]
            shijian = "".join(dates1).split(' ')[1]
        except:
            riqi = "".join(dates1)
            shijian = ''
        try:
            topic_id = i.split('.')[0].split('/')[-1]
        except:
            topic_id = i
        cares = {"platform": "搜狗问问回答", "date": riqi, "time": shijian, "author": ''.join(autors2),
                 "title": ''.join(title),"topic_date":topic_date,"topic_time":topic_time,
                 "content": ''.join(content1),
                 "url": urls, "is_topics": '否', "floor": '',
                 "keyword": word, 'comments_count': '', 'views': ''.join(view_count), "likes": ''.join(like),
                 "topic_id": topic_id, "author_id": ''.join(autors_id), "dislikes": ''.join(dislikes),"reposts_count":"" }
        data = json.dumps(cares, ensure_ascii=False) + '\n'
        # TODO
        print(data)
        hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/160_%s_%s_sougou.json' % (
            daily_date, daily_date, sjc), data,
                       encoding='utf-8')
    # except:
    #     pass


def clean_date(x):
    now = datetime.datetime.now()
    if str(x).find('昨天') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-1), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('前天') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-2), '%Y-%m-%d %H:%M:%S')
    elif str(x).find('天前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(days=-int(str(x).replace('天前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('小时之前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(hours=-int(str(x).replace('小时之前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('小时前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(hours=-int(str(x).replace('小时前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('分钟前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(minutes=-int(str(x).replace('分钟前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('分钟之前') != -1:
        x = datetime.datetime.strftime(now + datetime.timedelta(minutes=-int(str(x).replace('分钟之前', ''))),
                                       '%Y-%m-%d %H:%M:%S')
    elif str(x).find('今天') != -1:
        x = str(x).replace('今天', now.strftime('%Y-%m-%d'))
    elif str(x).find('刚刚') != -1:
        x = now.strftime('%Y-%m-%d %H:%M:%S')
    return x


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
            url = 'http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=1&time=2&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
            ip_pro = requests.get(url, headers=headers)
            if json.loads(ip_pro.text)['code'] == 0:
                break
        except:
            pass
    return json.loads(ip_pro.text)


if __name__ == '__main__':

    hdfs.makedirs('/user/cspider_daily/nike_daily/qa/{}'.format(daily_date)) # 创建每日文件夹

    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    for word in lines:
        try:
            main(word.strip())
        except:
            lines.append(word)

            print(traceback.format_exc())