import requests
from lxml import etree
import datetime
import time, json
import logging,urllib
from datetime import timedelta
now_table = str(datetime.datetime.now()).split(' ')[0]
import uuid

from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')

zhu_date_st = 'a'
zhu_time_st = 'b'

# 通过系统时间自动计算时间间隔 ---------------------------------------------------------
date = datetime.datetime.now() - timedelta(days=30)
news_start_time = str(date).split(' ')[0]
now_time = str(datetime.datetime.now()).split(' ')[0]
logging.info('爬取时间段：{}到{}'.format(news_start_time, now_time))
print(news_start_time,now_time)

# --------------------------------------------------------------------------------------
# 抓取前两天的数据的时间
tmp_date_list = news_start_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
news_start_times = time.mktime(dateC.timetuple())


tmp_date_list = now_time.split('-')
dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
nows_time = time.mktime(dateC.timetuple())




headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection':'close',

    'Host': 'tieba.baidu.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    'Upgrade-Insecure-Requests': '1'
}

import re
def run(urls,ww):
    pn = 0
    href_list = []
    for page in range(10):
        url = urls%ww+str(pn)
        print(url)
        response = requests.get(url, timeout=10, headers=headers)

        href = re.findall(r'<div class="threadlist_title pull_left j_th_tit .*?</div>', response.text, re.S | re.M)
        for i in href:
            if '置顶' not in i:
                href_list.append('http://tieba.baidu.com' + i.split('href="')[1].split('" title')[0])
        pn +=50
    href_list = list(set(href_list))
    detail(href_list,ww)

def detail(href_list,ww):
    for i in href_list:
        print(i)
        try:
            # 访问帖子
            response = requests.get(i, timeout=10, headers=headers)
            # print(i)
            html = etree.HTML(response.text)
            # sj = html.xpath('//*[@class="post-tail-wrap"]/span[last()]/text() | //*[@class="p_tail"]/li[last()]/span/text()')
            sj = html.xpath('//*[@class="p_postlist"]/div/@data-field')
            content = html.xpath(
                '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2] | //*[@class="d_post_content_main"]/div/cc/div[2] | //*[@class="d_post_content_main "]/div/cc/div[2] ')
            # print(content)
            #时间判断
            sj_num = 0
            for o in range(len(content)):
            # # print(data['author']['user_id'])
                data = json.loads(str(sj[o]))
                try:
                    sjs = data['content']['date']
                except:
                    sjs = html.xpath('//*[@class="post-tail-wrap"]/span[last()]//text()')[sj_num]

                floor = data['content']['post_no']
                j_time = sjs.split(' ')[0]
                tmp_date_list = j_time.split('-')
                dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
                showtime = time.mktime(dateC.timetuple())

                if news_start_times <= showtime <= nows_time or str(floor) == '1':
                    try:
                        name = data['author']['name_u']
                    except:
                        name = data['author']['user_name']

                    user_id = data['author']['user_id']

                    # for j in name:
                #     if " " in j.xpath('string(.)'):1`
                #         name.remove(j)
                # 获取进去页面内容
                # floor = html.xpath('//*[@class="post-tail-wrap"]/span[last()-1]/text() | //*[@class="p_tail"]/li[last()-1]/span/text()')
                    try:
                        title = html.xpath('//*[@class="core_title core_title_theme_bright"]/h1//text()')[0]
                    except:
                        title = html.xpath('//*[@class="core_title_wrap_bright clearfix"]/h3//text()')[0]

                    end_page = html.xpath('//*[@class="l_pager pager_theme_5 pb_list_pager"]/a[last()-2]/@href')
                    reply_no = html.xpath('//*[@class="l_reply_num"]/span/text()')

                    c = content[o].xpath('string(.)')


                    try:
                        imm_url = html.xpath(
                            '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]/img/@src | //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2]/img/@src')
                    except:
                        imm_url = ''
                    try:
                        video_url = html.xpath(
                            '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]//video/@src| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2]//video/@src')
                    except:
                        video_url = ''



                    names = urllib.parse.unquote(name.split('&ie')[0])


                    wirte_excel(names,c,floor,sjs,title,i,ww,reply_no,user_id,imm_url,video_url)
                    # 倒序爬取
                    if end_page:
                        try:
                            rev_request(end_page,title,ww)
                        except:
                            pass
                else:
                    pass
                sj_num +=1

        except:
            pass


# 倒序爬取
def rev_request(end_page,title,ww):
    # 拼接url
    for ni in range(int(end_page[0].split('pn=')[-1]), -1, -1):
        if ni == 0:
            break
        url = 'https://tieba.baidu.com' + end_page[0].split('?')[0] + '?pn=%s' % str(ni)
        response = requests.get(url, timeout=10, headers=headers)
        html = etree.HTML(response.text)
        sj = html.xpath('//*[@class="p_postlist"]/div/@data-field')
        content = html.xpath('//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2] | //*[@class="d_post_content_main"]/div/cc/div[2] | //*[@class="d_post_content_main "]/div/cc/div[2] ')
        sj_num = 0
        for o in range(len(content)-1,-1,-1):
            # # print(data['author']['user_id'])
            data = json.loads(str(sj[o]))
            try:
                sjs = data['content']['date']
            except:
                sjs = html.xpath('//*[@class="post-tail-wrap"]/span[last()]//text()')[sj_num]
            try:
                name = data['author']['name_u']
            except:
                name = data['author']['user_name']

            floor = data['content']['post_no']
            user_id = data['author']['user_id']
            reply_no = html.xpath('//*[@class="l_reply_num"]/span/text()')
            names = urllib.parse.unquote(name.split('&ie')[0])

            showtimes = sjs.split(' ')[0]
            tmp_date_list = showtimes.split('-')
            dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
            showtime = time.mktime(dateC.timetuple())
            if showtime <= news_start_time:
                return


            try:
                imm_url = html.xpath(
                    '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]/img/@src | //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2]/img/@src')
            except:
                imm_url = ''
            try:
                video_url = html.xpath(
                    '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]//video/@src| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2]//video/@src')
            except:
                video_url = ''
            c = content[o].xpath('string(.)')
            wirte_excel(names, c, floor, sjs, title,url,ww,reply_no,user_id,imm_url,video_url)
            sj_num +=1

# 追加内容进excel
def wirte_excel(name, content, floor, sj, title,url,ww,reply_no,user_id,imageurl,video_url):
    global zhu_date_st
    global zhu_time_st
    if '?' in url:
        topic_id = url.split('?')[0].split('/')[-1]
    else:
        topic_id = url.split('p/')[-1]
    if str(floor) == '1':
        is_topics = '是'
        reply_no = reply_no[0]

        zhu_date_st = sj.split(' ')[0]
        zhu_time_st = sj.split(' ')[1]
        content_id = topic_id
        imageurl = imageurl
        video_url = video_url
    else:
        is_topics = '否'
        reply_no = 0

        zhu_date_st = ''
        zhu_time_st = ''
        content_id = str(uuid.uuid4())
        imageurl = ''
        video_url = ''
    content_id1 = url.split("?")[0].split('/')[-1].zfill(15)+ str(str(floor).replace('楼', '').strip().zfill(7))

    cars = {"platform": "贴吧论坛", "brand": '', "carseries": ww, "author": name, "date": sj.split(' ')[0], 'time': sj.split(' ')[1],
         "title": title, "content": content, "url": url, "floor": floor, "from": '',
         "identification": '', 'reply_no': reply_no, 'signin_time': '', "is_topics": is_topics, "views": '',
         "likes": '', 	'post_client':'',  'series_url':'',"topic_date":zhu_date_st,"topic_time":zhu_time_st,
        'is_elite': '', 'topic_count': '',	'reply_count':'','pick_count':'', 'topic_categroy':'',
        'topic_type':'',"favorite":'','follows':'',
        'insert_time': str(datetime.datetime.now()).split('.')[0],
        'update_time': str(datetime.datetime.now()).split('.')[0],
        'topic_id':topic_id, 'author_id': user_id,"content_id":content_id1,"reply_floor":'',"file_code":'62',"reposts_count":"",
            "audiourl":video_url,"imageurl":imageurl}
    # print(cars)
    data = json.dumps(cars, ensure_ascii=False) + '\n'
    # TODO
    hdfs.new_write('/user/cspider_daily/nike_daily/forum/%s/62_%s_%s_baidu1_forum2.json' % (
        daily_date, daily_date, sjc), data,
                   encoding='utf-8')
def main():

    hdfs.makedirs('/user/cspider_daily/nike_daily/forum/{}'.format(daily_date)) # 创建每日文件夹

    urls = 'https://tieba.baidu.com/f?kw=%s&ie=utf-8&pn='
    with open("tieba_word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    for word in lines:
        try:
            run(urls, word.strip())
        except:
            lines.append(word)
main()