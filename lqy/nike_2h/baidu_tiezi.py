import requests
from lxml import etree
import datetime
import time, json
import logging
from datetime import timedelta
now_table = str(datetime.datetime.now()).split(' ')[0]
import uuid
import sys
canss = sys.argv[1]


ss_name = str(datetime.datetime.now()).split('.')[0]
ss_name = ss_name.replace(':', '-').split('-')
del ss_name[-1]
ss_names = "-".join(ss_name)

zhu_date_st = 'a'
zhu_time_st = 'b'


from with_hdfs import HdfsClient
hdfs = HdfsClient(url='http://192.168.1.209:14000', user='dpp-executor')
sjc = str(time.time()).split('.')[0]
daily_date = str(datetime.datetime.now()- timedelta(hours=2)).split(' ')[0].replace('-', '')




st_1 = str(datetime.datetime.now()).split(' ')[1].split(':')[0]
st_2 = str(datetime.datetime.now() - timedelta(hours=2)).split(' ')[1].split(':')[0]
st_3 = st_2 + '_' + st_1


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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection':'close',

    'Host': 'tieba.baidu.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    'Upgrade-Insecure-Requests': '1'
}


def run(url,ww):
    url = url%ww
    for cishu in range(10):
        status = detail(url+str(cishu+1),ww)
        if status == 1:
            return

def detail(url,ww):
    print(url)
    response = requests.get(url, timeout=10, headers=headers)
    response.encoding ='gbk'
    html1 = etree.HTML(response.text)
    href = html1.xpath('//*[@class="p_title"]/a/@href')
    zhu_time = html1.xpath('//*[@class="p_green p_date"]/text()')
    if len(zhu_time) != len(href):
        del href[0]
    num = 0
    for i in href:
        showtimes = zhu_time[num].split(' ')[0]
        tmp_date_list = showtimes.split('-')
        dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
        showtime = time.mktime(dateC.timetuple())
        if showtime <= news_start_time:
            # print(showtimes)
            return 1
        try:
            urls = 'https://tieba.baidu.com'+i
            # 访问帖子
            response = requests.get(urls, timeout=10, headers=headers)
            # print(urls)
            html = etree.HTML(response.text)
            name = html.xpath('//*[@class="d_name"]/a')
            for j in name:
                if " " in j.xpath('string(.)'):
                    name.remove(j)
            # 获取进去页面内容
            content = html.xpath('//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2] | //*[@class="d_post_content_main"]/div/cc/div[2] | //*[@class="d_post_content_main "]/div/cc/div[2] ')
            floor = html.xpath('//*[@class="post-tail-wrap"]/span[last()-1]/text() | //*[@class="p_tail"]/li[last()-1]/span/text()')
            sj = html.xpath('//*[@class="post-tail-wrap"]/span[last()]/text() | //*[@class="p_tail"]/li[last()]/span/text()')
            title = html.xpath('//*/h3[1]/text()')
            end_page = html.xpath('//*[@class="l_pager pager_theme_5 pb_list_pager"]/a[last()-2]/@href')
            reply_no = html.xpath('//*[@class="l_reply_num"]/span/text()')
            huifu_reply_no = html.xpath('//*[@class="p_reply"]/li/a/text() | //*[@class="j_lzl_r p_reply"]/a/text()')

            if len(floor) != 0:
                for o in range(len(name)):

                    au = html.xpath('//*[@class="p_postlist"]/div/@data-field')
                    au_data = json.loads(str(au[o]))
                    user_id = au_data['author']['user_id']
                    n = name[o].xpath('string(.)')
                    c = content[o].xpath('string(.)')
                    try:
                        reply_huifu = huifu_reply_no[o]
                    except:
                        reply_huifu = 0

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
                    wirte_excel(n,c,floor[o],sj[o],title,urls,ww,reply_no,user_id,reply_huifu,imm_url,video_url)
                # 倒序爬取
                if end_page:
                    try:
                        rev_request(end_page,title,ww)
                    except:
                        pass
            else:
                pass
            num +=1
        except:
            try:
                urls = 'https://tieba.baidu.com' + i
                # 访问帖子
                response = requests.get(urls, timeout=10, headers=headers)
                # print(urls)
                html = etree.HTML(response.text)
                name = html.xpath('//*[@class="d_name"]/a')
                for j in name:
                    if " " in j.xpath('string(.)'):
                        name.remove(j)
                # 获取进去页面内容
                content = html.xpath(
                    '//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2] | //*[@class="d_post_content_main"]/div/cc/div[2] | //*[@class="d_post_content_main "]/div/cc/div[2] ')
                floor = html.xpath(
                    '//*[@class="post-tail-wrap"]/span[last()-1]/text() | //*[@class="p_tail"]/li[last()-1]/span/text()')
                sj = html.xpath(
                    '//*[@class="post-tail-wrap"]/span[last()]/text() | //*[@class="p_tail"]/li[last()]/span/text()')
                end_page = html.xpath('//*[@class="l_pager pager_theme_5 pb_list_pager"]/a[last()-2]/@href')
                title = html.xpath('//*/h3[1]/text()')
                reply_no = html.xpath('//*[@class="l_reply_num"]/span/text()')

                huifu_reply_no = html.xpath('//*[@class="p_reply"]/li/a/text() | //*[@class="j_lzl_r p_reply"]/a/text()')


                if len(floor) != 0:
                    for o in range(len(name)):

                        au = html.xpath('//*[@class="p_postlist"]/div/@data-field')
                        au_data = json.loads(str(au[o]))
                        user_id = au_data['author']['user_id']


                        n = name[o].xpath('string(.)')
                        c = content[o].xpath('string(.)')

                        try:
                            reply_huifu = huifu_reply_no[o]
                        except:
                            reply_huifu = 0

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

                        wirte_excel(n, c, floor[o], sj[o], title, urls, ww,reply_no,user_id,reply_huifu,imm_url,video_url)
                    # 倒序爬取
                    if end_page:
                        try:
                            rev_request(end_page, title, ww)
                        except:
                            pass
                else:
                    pass
                num += 1
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
        name = html.xpath('//*[@class="d_name"]/a')
        for j in name:
            if " " in j.xpath('string(.)'):
                name.remove(j)
        content = html.xpath('//*[@class="d_post_content_main d_post_content_firstfloor"]/div/cc/div[2]| //*[@class="d_post_content_main  d_post_content_firstfloor"]/div/cc/div[2] | //*[@class="d_post_content_main"]/div/cc/div[2] | //*[@class="d_post_content_main "]/div/cc/div[2] ')
        floor = html.xpath('//*[@class="post-tail-wrap"]/span[last()-1]/text() | //*[@class="p_tail"]/li[last()-1]/span/text()')
        sj = html.xpath('//*[@class="post-tail-wrap"]/span[last()]/text() | //*[@class="p_tail"]/li[last()]/span/text()')
        reply_no = html.xpath('//*[@class="l_reply_num"]/span/text()')
        try:
            huifu_reply_no = html.xpath('//*[@class="p_reply"]/li/a/text() | //*[@class="j_lzl_r p_reply"]/a/text()')
        except:
            huifu_reply_no = ''
        num = len(sj)-1

        for i in reversed(sj):
            au = html.xpath('//*[@class="p_postlist"]/div/@data-field')
            au_data = json.loads(str(au[num]))
            user_id = au_data['author']['user_id']
            # 不在时间范围 break
            showtimes = i.split(' ')[0]
            tmp_date_list = showtimes.split('-')
            dateC = datetime.datetime(int(tmp_date_list[0]), int(tmp_date_list[1]), int(tmp_date_list[2]))
            showtime = time.mktime(dateC.timetuple())
            if showtime <= news_start_time:
                return
            n = name[num].xpath('string(.)')
            c = content[num].xpath('string(.)')
            try:
                reply_huifu = huifu_reply_no[num]
            except:
                reply_huifu = 0
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
            wirte_excel(n, c, floor[num], sj[num], title,url,ww,reply_no,user_id,reply_huifu,imm_url,video_url)
            num -= 1


# 追加内容进excel
def wirte_excel(name, content, floor, sj, title,url,ww,reply_no,user_id,huifu_reply_no,imageurl,video_url):
    global zhu_date_st
    global zhu_time_st
    if '?' in url:
        topic_id = url.split('?')[0].split('/')[-1]
    else:
        topic_id = url.split('p/')[-1]
    if floor == '1楼':
        is_topics = '是'
        reply_no = reply_no[0]

        zhu_date_st = sj.split(' ')[0]
        zhu_time_st = sj.split(' ')[1]
        content_id = topic_id
        imageurl = imageurl
        video_url = video_url

    else:
        is_topics = '否'
        reply_no = huifu_reply_no

        zhu_date_st = ''
        zhu_time_st = ''
        content_id = str(uuid.uuid4())
        imageurl = ''
        video_url = ''
    content_id1 = url.split("?")[0].split('/')[-1].zfill(15)+ str(str(floor).replace('楼', '').strip().zfill(7))

    cars = {"platform": "贴吧论坛", "keyword":ww,"brand": '', "carseries": '', "author": name, "date": sj.split(' ')[0], 'time': sj.split(' ')[1],
         "title": title[0], "content": content, "url": url, "floor": floor, "from": '',
         "identification": '', 'reply_no': reply_no, 'signin_time': '', "is_topics": is_topics, "views": '',
            "likes": '', 'post_client': '', 'series_url': '',"topic_date":zhu_date_st,"topic_time":zhu_time_st,
            'is_elite': '', 'topic_count': '', 'reply_count': '', 'pick_count': '', 'topic_categroy': '',
            'topic_type': '', "favorite": '', 'follows': '',
            'insert_time': str(datetime.datetime.now()).split('.')[0],
            'update_time': str(datetime.datetime.now()).split('.')[0],
            'topic_id': topic_id, 'author_id': user_id, "content_id": content_id1,"reply_floor":'',"file_code":'62',
            "reposts_count":'',"audiourl":video_url,"imageurl":imageurl}
    data = json.dumps(cars, ensure_ascii=False) + '\n'
    # TODO
    hdfs.new_write('/user/cspider_daily/nike_2h/forum/%s/%s/62_%s_%s_baidu_forum.json' % (
        daily_date, st_3,st_3, sjc), data,
                   encoding='utf-8')


def clean_deily():
    global st_3
    if st_3 == '22_00':
        st_3 = '22_24'


def main():
    clean_deily()

    hdfs.makedirs('/user/cspider_daily/nike_2h/forum/{}/{}'.format(daily_date,st_3)) # 创建每日文件夹
    h1 = time.time()
    url = 'http://tieba.baidu.com/f/search/res?qw=%s&rn=20&pn='
    with open("word.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()
    for word in lines:
        h2 = time.time()
        h3 = h2 - h1
        if int(h3) > int(canss) * 3600:
            break
        try:
            run(url, word.strip())
        except:
            lines.append(word)
main()