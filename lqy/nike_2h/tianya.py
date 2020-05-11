import requests
from lxml import etree
import datetime
import time, json
import logging
from datetime import timedelta
import uuid

now_time = str(datetime.datetime.now()).split(' ')[0]

import sys
canss = sys.argv[1]

ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)


from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()- timedelta(hours=2)).split(' ')[0].replace('-', '')




st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1


# 通过系统时间自动计算时间间隔 ---------------------------------------------------------
date = datetime.datetime.now() - timedelta(days=3)
news_start_time = str(date).split(' ')[0]
logging.info('爬取时间段：{}到{}'.format(news_start_time, now_time))
print(news_start_time,now_time)
# --------------------------------------------------------------------------------------
# 抓取前两天的数据的时间
tmp_date_list = news_start_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
news_start_time = time.mktime(dateC.timetuple())


tmp_date_list = now_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
nows_time = time.mktime(dateC.timetuple())


headers = {
'Host': 'bbs.tianya.cn',
'Referer': 'http://bbs.tianya.cn/list-cars-1.shtml',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
}
def match_time(times,href,click,replay_no,autor,carseries):
    time_num = 0
    for j in times:
        j_time = j.split(' ')[0]
        tmp_date_list = j_time.split('-')
        dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
        showtime = time.mktime(dateC.timetuple())
        if news_start_time <= showtime <= nows_time:
            try:
                write_json(href[time_num],click[time_num],replay_no[time_num],autor[time_num],carseries)
            except:
                pass
        elif news_start_time > showtime:
            return 1
        else:
            pass
        time_num += 1
    return time_num


def run():
    href_list = ['http://bbs.tianya.cn/list-fans-1.shtml','http://bbs.tianya.cn/list-basketball-1.shtml','http://bbs.tianya.cn/list-sport-1.shtml',]
    # response = requests.get(url, timeout=10, headers=headers)
    # html1 = etree.HTML(response.text)
    # href = html1.xpath('//*[@class="nav_child"][2]/li/a/@href')
    # name_list2 = html1.xpath('//*[@class="nav_child"][2]/li/a/text()')
    name_list1 = ['球迷一家','篮球公园','体育聚焦']
    # # 拼接车友会url
    # href_list = []
    # href_list.append('http://bbs.tianya.cn/list.jsp?item=cars&order=1')
    # href_list.append('http://bbs.tianya.cn/list.jsp?item=944&order=1')
    # for i in href:
    #     url_id = i.split('-')[1]
    #     href_list.append('http://bbs.tianya.cn/list.jsp?item=%s&order=1'%url_id)
    #
    # name_list3 = html1.xpath('//*[@class="nav_child"][3]/li/a/text()')
    # href = html1.xpath('//*[@class="nav_child"][3]/li/a/@href')
    # for i in href:
    #     url_id = i.split('-')[1]
    #     href_list.append('http://bbs.tianya.cn/list.jsp?item=%s&order=1'%url_id)
    # name_list1 = name_list1 + name_list2 + name_list3
    num = 0
    for i in href_list:
        try:
            response = requests.get(i, timeout=10, headers=headers)
            html1 = etree.HTML(response.text)
            href = html1.xpath('//*[@class="bg"]/../tr/td[1]/a/@href')
            times = html1.xpath('//*[@class="bg"]/../tr/td[5]/@title')
            click = html1.xpath('//*[@class="bg"]/../tr/td[3]/text()')
            autor = html1.xpath('//*[@class="bg"]/../tr/td[2]/a/text()')
            replay_no = html1.xpath('//*[@class="bg"]/../tr/td[4]/text()')
            carseries = name_list1[num]

            status = match_time(times,href,click,replay_no,autor,carseries)



            if status >= 78:

                xia = html1.xpath('//*[@class="short-pages-2 clearfix"]/div/a[last()]/@href')[0]
                href_list.append('http://bbs.tianya.cn'+xia)
                name_list1.append(name_list1[num])
            num +=1
        except:
            num += 1
            pass



def write_json(href,click,replay_no,autor,carseries):

    href = 'http://bbs.tianya.cn' + href
    print(href)
    try:
        id_list = href.split('-')
        del id_list[0]
        topic_id = "-".join(id_list).split('.shtml')[0]
    except:
        topic_id = ''
    if 'me' in href:
        return
    response = requests.get(href, timeout=10, headers=headers)
    html1 = etree.HTML(response.text)
    try:
        biaoqian = html1.xpath('//*[@class="crumbs"]/a[3]/text()')[0]
    except:
        biaoqian = ''

    if biaoqian == '问答':
        pass
    else:
        try:
            is_elites = html1.xpath('//*[@class="info_icon_box"]/span/@class')
            is_elite = "".join(is_elites)
            if 'faceblue' in is_elite:
                is_elite = '否'
            else:
                is_elite = '是'
        except:
            is_elite = ''

        title = html1.xpath('//*[@class="s_title"]/span//text() | //*[@class="q-title"]/h1//text()')[0]
        ####
        try:
            content = html1.xpath('//*[@class="bbs-content clearfix"]/text() | //*[@class="q-content atl-item"]//text()')
            content = "".join(content)
        except:
            content = ""
        author_id = html1.xpath('//*[@id="post_head"]/div[2]/div[2]/span/a/@uid | //*[@class="wd-question"]/div/@_host')[0]
        try:
            like = html1.xpath('//*[@class="shang_zan"]/text()')[0]
        except:
            like = 0
        try:
            j_time = html1.xpath('//*[@class="info_icon_box"]/../div[2]/span[2]/text()')[0]
            zhu_j_date = j_time.split('时间：')[1].split(' ')[0]
            zhu_j_time = j_time.split('时间：')[1].split(' ')[1]
        except:
            zhu_j_date = ''
            zhu_j_time = ''

        try:
            img_url = html1.xpath('//*[@class="bbs-content clearfix"]/img/@src| //*[@class="q-content atl-item"]//img/@src')
        except:
            img_url = ""


        cars = {"platform": "天涯论坛","carseries": carseries, "author": autor, "date":zhu_j_date,
                'time': zhu_j_time,"title": title, "content": content, "url": href, "floor": '0',
                'reply_no': replay_no,  "is_topics": '是', "views": click,
                "likes": like,'signin_time': ''," identification": '',"from": '',"brand": '',
                "is_elite":is_elite,"keyword":'',"author_id":author_id,"content_id":str(uuid.uuid4()),
                "post_client": '',"series_url": '',"topic_date":zhu_j_date,"topic_time":zhu_j_time,
                "topic_count": '', "reply_count": '', "pick_count": '', "topic_categroy": '',
                "topic_type": '',"topic_id":topic_id[:-2],
                'insert_time': str(datetime.datetime.now()).split('.')[0],
                'update_time': str(datetime.datetime.now()).split('.')[0],
                'favorite':'',"identification":'',"follows":'',"reply_floor":'',"file_code":'43',"reposts_count":'',
                "audiourl": '', "imageurl": img_url

                }

        data = json.dumps(cars, ensure_ascii=False) + '\n'
        # TODO
        hdfs.new_write('/user/cspider_daily/nike_2h/forum/%s/%s/43_%s_%s_tianya_forum1.json' % (
            daily_date, st_3,st_3, sjc), data,
                       encoding='utf-8')
    # 评论
    #         floor = html1.xpath('//*[@class="atl-reply"]/span[1]/text()')
    #         reply_no = html1.xpath('//*[@class="atl-item"]//a[@_stat="/stat/bbs/post/评论"]/text()')
    #         for oo in range(len(floor)):
    #             oo = oo+1
    #             try:
    #                 content = html1.xpath('//*[@class="atl-item"][%s]//div[@class="bbs-content"]/text()'%str(oo))
    #                 content = "".join(content)
    #             except:
    #                 content = ""
    #             autor = html1.xpath('//*[@class="atl-item"][%s]/@js_username'%str(oo))[0]
    #             autor_id = html1.xpath('//*[@class="atl-item"][%s]/@_hostid'%str(oo))[0]
    #             date = html1.xpath('//*[@class="atl-item"][%s]/@js_restime'%str(oo))[0]
    #             # post_client = html1.xpath('//*[@class="atl-item"][%s]//a[@rel="nofollow"]/text()'%str(oo))[0]
    #             like = html1.xpath('//*[@class="atl-item"][%s]//a[@class="zan"]/@_count'%str(oo))[0]
    #
    #             # cars = {"platform": "天涯论坛", "carseries": carseries, "author": autor, "date": date.split(' ')[0],
    #             #         "author_id": autor_id, "post_client": '',
    #             #         'time': date.split(' ')[1], "title": title, "content": content, "url": href, "floor": floor[oo-1],
    #             #         'reply_no': reply_no[oo-1], "is_topics": '是', "views": click,
    #             #         "likes": like, 'signin_time': '', " identification": '', "from": '', "series_url": '', "brand": '',
    #             #         "is_elite": '', "topic_count": '', "reply_count": '', "pick_count": '', "topic_categroy": '',
    #             #         "topic_type": '',
    #             #         'insert_time': str(datetime.datetime.now()).split('.')[0],
    #             #         'update_time': str(datetime.datetime.now()).split('.')[0]
    #             #         }
    #             cars = {"platform": "天涯论坛", "carseries": carseries, "author": autor, "date": date.split(' ')[0],
    #                     'time': date.split(' ')[1], "title": title, "content": content, "url": href, "floor": floor[oo-1],
    #                     'reply_no': "", "is_topics": '否', "views": "","is_elite":is_elite,"identification": '',
    #                     "likes": like, 'signin_time': '',  "from": '',  "brand": '',"keyword":'',"content_id":'',"author_id":''
    #                     }
    #             data = json.dumps(cars, ensure_ascii=False)
    #             try:
    #                 f.write(data + '\n')
    #             except:
    #                 pass


def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'


def main():
    clean_deily()
    hdfs.makedirs('/user/cspider_daily/nike_2h/forum/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹

    run()
main()