import datetime
import time
import requests
import logging
from lxml import etree
import json
from datetime import timedelta
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


header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.9',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Host': 'iask.sina.com.cn',
'If-Modified-Since':'Mon, 12 Aug 2019 06:23:38 GMT',
'Referer': 'https://iask.sina.com.cn/search?searchWord=%E5%AE%9D%E9%A9%AC&page=1',
'Sec-Fetch-Mode': 'navigate',
'Sec-Fetch-Site': 'none',
'Sec-Fetch-User': '?1',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
}

def main(word):
    for i in range(30):
        url = 'https://iask.sina.com.cn/search?searchWord=%s&page=%s' % (word, str(i+1))
        print(url)
        res = requests.get(url,headers=header)
        html = etree.HTML(res.text)

        hrefs = html.xpath('//*[@class="iask-search-list"]/li/p[1]/a/@href')
        type = html.xpath('//*[@class="iask-search-list"]/li/div/a/text()')
        reply = html.xpath('//*[@class="iask-search-list"]/li/div/span/em/text()')
        num = 0
        for j in hrefs:


            try:
                urls = 'https://iask.sina.com.cn/' + j
                # print(urls)
                response = requests.get(urls)
                html1 = etree.HTML(response.text)
                name = html1.xpath('//*[@class="iask-user-bar"]/div[1]/a/span/text() | //*[@class="other-bar cf"]/div[1]/a/span/text()')[0]
                author_id = html1.xpath('//*[@class="iask-user-bar"]/div[1]/a/@href | //*[@class="other-bar cf"]/div[1]/a/@href')[0]
                content = html1.xpath('//*[@class="problem-title-text"]/text() | //*[@class="question-title "]/text()')[0]

                que_dates = []

                good_rel(html1,type[num],reply[num],f,urls,j,content,que_dates)
                rel(html1,type[num],reply[num],f,urls,j,content,que_dates)
                complete_date = list(set(que_dates))
                complete_date.sort()

                caree = {"platform": "爱问问答", "date": complete_date[0], "time": '', "author": name,
                         "title": content,"topic_date":complete_date[0],"topic_time":'',
                         "content": content,
                         "url": urls, "is_topics": '是', "floor": '',
                         "keyword": word, 'comments_count': reply[num], 'views': '', "likes": '',
                         "topic_id": clean_id(j), "author_id": author_id, "type": type[num],"dislikes":'',
                         "content_id":clean_id(j),"file_code":'159',"reposts_count":"","audiourl": '', "imageurl": ""}
                data = json.dumps(caree, ensure_ascii=False) + '\n'
                # TODO
                hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/159_%s_%s_aiwen.json' % (
                    daily_date, daily_date, sjc), data,
                               encoding='utf-8')


            except:
                pass
                num += 1

def rel(html,type,reply,f,urls,topic_id,title,que_dates):
    num = 0
    name = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[2]/div[1]/p[1]/a/text()')
    for i in name:
        sj = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[2]/div[1]/p[2]/text() | //*[@class="new-other-answer answer_list"]/ul/li/div[2]/div[1]/p[2]/text()')
        like = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[2]/div[2]/a[1]/span/text()')
        dislikes = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[2]/div[2]/a[2]/span/text()')

        try:
            riqi = sj[num].split(' ')[0]
            shijian = sj[num].split(' ')[1]
        except:
            riqi = sj[num]
            shijian = ''
        que_dates.append(riqi)

        try:
            content = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[1]/pre')[num]
        except:
            return
        contents1 = etree.tostring(content, method='html', encoding='utf-8')
        author_id = html.xpath('//*[@class="detail-answer-item detail-all-answer mt10"]/ul/li/div[2]/div[1]/p[1]/a/@href')[0]

        # print(contents1.decode())


        care = {"platform": "爱问问答","topic_date":'',"topic_time":'', "date": riqi, "time": shijian, "author": i, "title": title,
                "content": contents1.decode(),
                "url": urls, "is_topics": '否', "floor": '',
                "keyword": word, 'comments_count': reply, 'views': '', "likes": "".join(like[num]),"reposts_count":"",
                "topic_id": clean_id(topic_id), "author_id": clean_id(author_id),"type":type,"dislikes":"".join(dislikes[num]),"file_code":'159',
                "audiourl": '', "imageurl": ""}
        data = json.dumps(care, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/159_%s_%s_aiwen.json' % (
            daily_date, daily_date, sjc), data,
                       encoding='utf-8')
        num +=1


def good_rel(html,type,reply,f,urls,topic_id,title,que_dates):
    # 好评
    name = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[2]/div[1]/p[1]/a/text()')
    sj = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[2]/div[1]/p[2]/text() | //*[@class="time"]/text()')
    like = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[2]/div[2]/a[1]/span/text()')
    dislikes = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[2]/div[2]/a[2]/span/text()')


    try:
        riqi = sj[0].split(' ')[0]
        shijian = sj[0].split(' ')[1]
    except:
        riqi = sj[0]
        shijian = ''
    que_dates.append(riqi)



    try:
        content = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[1]/pre')[0]
    except:
        return
    contents1 = etree.tostring(content, method='html', encoding='utf-8')
    author_id = html.xpath('//*[@class="detail-answer-item detail-goods-item mt10"]/ul/li/div[2]/div[1]/p[1]/a/@href')

    # print(contents1.decode())


    care = {"platform": "爱问问答", "date": riqi, "time": shijian, "author": name[0], "title": title,
        "content": contents1.decode(),"topic_date":'',"topic_time":'',
        "url": urls, "is_topics": '否', "floor": '',
        "keyword": word, 'comments_count': reply, 'views': '', "likes": "".join(like),"reposts_count":"",
        "topic_id": clean_id(topic_id), "author_id": clean_id(author_id),"type":type,"dislikes":"".join(dislikes),"file_code":'159',
            "audiourl": '', "imageurl": ""}
    data = json.dumps(care, ensure_ascii=False) + '\n'
    # TODO
    hdfs.new_write('/user/cspider_daily/nike_daily/qa/%s/159_%s_%s_aiwen.json' % (
        daily_date, daily_date, sjc), data,
                   encoding='utf-8')


def clean_id(data):
    if '.' in str(data):
        data = data.split('.')[0]
    if '/' in data:
        data = data.split('/')[-1]

    return data
import traceback
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

            #     lines.append(word)
