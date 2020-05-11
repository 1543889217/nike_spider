import requests
from lxml import etree
import json
import time
from datetime import datetime
from datetime import timedelta
import logging
import traceback
import random
import multiprocessing
import os, sys
from with_hdfs import HdfsClient



# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
# file_name = r"./../zhihu/zhihu-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.WARNING,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    # filename=file_name,   # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)
now_time = str(datetime.now()).split(' ')[0].replace('-', '_')


class ZhiHuSpider(object):
    """
    知乎爬虫，根据关键字进行搜索，爬取一周内的信息
    """
    def __init__(self):

        self.headers_one = {

        }

        self.start_url = ''
        # 评论接口模板
        self.commnet_port_url = ''
        # # 打开json文件
        # self.news_jsonfile = open('./sina_newsfile.json', 'wb')
        # self.comment_jsonfile = open('./sina_commentfile.json', 'wb')

        # 时间判断部分
        date = datetime.now() - timedelta(days=7)
        news_start_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
        yesterday = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        logging.info('爬取时间段：{}到{}'.format(news_start_time, yesterday))

        # 定义开始时间 y-m-d  离现在时间远  news_start_time
        self.start_time = news_start_time
        # self.start_time = '2019-08-01'
        # 定义结束时间 y-m-d  离现在时间近  yesterday
        self.end_time = yesterday
        # self.end_time = '2019-08-18'
        # 标记爬虫工作
        self.is_work = True
        self.is_stop = False
        # 翻页计数
        self.page_count = 0
        # 楼层计数
        self.floor_num = 1

        # 去重列表
        self.set_list = []

        self.user_agent = [
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/536.6',
            'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.1',
        ]
        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        # proxyUser = "HQ60F7PAQBO68GWD"
        # proxyUser = "H7307T4706B25G4D"
        proxyUser = "HEW657EL99F83S9D"
        # proxyPass = "FBD9D819229DBB1B"
        # proxyPass = "05B4877CC39192C0"
        proxyPass = "8916B1F3F10B1979"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            # "http": proxyMeta,
            "https": proxyMeta
        }

        self.queue = ''

    # 获取知乎列表页
    def get_questions_list_page(self, url, keyword, params=None):
        """
        知乎搜索出来的列表页，其中包含问答类信息和文章类信息，所以在函数中页做出了适当的判断
        :param url:
        :param params: 参数
        :return:
        """

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'x-ab-param': 'ug_zero_follow_0=0;zr_video_rank=new_rank;se_dnn_cpyramid=0;top_recall_deep_user=1;zr_km_xgb_model=new_xgb;zr_test_aa1=0;top_v_album=1;top_universalebook=1;li_price_test=1;se_topicdirect=2;ug_zero_follow=0;tsp_newchild=1;pf_creator_card=1;li_se_kv=0;zr_infinity_small=256;se_whitelist=0;se_waterfall=0;se_mobileweb=1;se_subtext=0;tp_header_style=1;li_album_liutongab=0;li_hot_score_ab=0;li_search_answer=0;tp_qa_metacard=1;se_ri=0;se_dnn_muli_task=0;se_college=default;se_agency= 0;top_quality=0;top_new_feed=5;ug_follow_answerer_0=0;li_tjys_ec_ab=0;qa_answerlist_ad=0;se_mclick=0;se_zu_recommend=0;pf_fuceng=1;li_qa_cover=old;zr_rec_answer_cp=close;se_dnn_slabel=0;tsp_lastread=0;zr_km_style=base;se_site_onebox=0;top_ebook=0;se_p_slideshow=0;se_ad_index=10;se_col_boost=0;se_backsearch=0;se_ios_spb309=0;se_topiclabel=1;se_websearch=3;tp_sft=a;ug_follow_topic_1=2;zr_km_answer=open_cvr;se_rr=0;se_ltr_dnn_cp=0;top_recall_exp_v1=1;zr_intervene=0;se_famous=1;ug_follow_answerer=0;zr_slot_cold_start=default;zr_rel_search=base;se_webtimebox=0;soc_bignew=1;zr_km_slot_style=event_card;zr_man_intervene=0;top_recall_exp_v2=1;soc_bigone=0;pf_noti_entry_num=0;zr_video_rank_nn=new_rank;se_webrs=1;se_mclick1=0;soc_special=0;ls_videoad=2;li_se_paid_answer=0;se_hotsearch=0;top_native_answer=1;tsp_vote=1;top_vipconsume=1;top_ydyq=X;se_dnn_mt=0;se_new_topic=0;tp_qa_metacard_top=top;top_test_4_liguangyi=1;ls_new_upload=0;li_se_album_card=0;zr_answer_rec_cp=open;se_zu_onebox=0;li_back=0;tp_club_qa=0;se_payconsult=0;top_root=0;li_se_xgb=0;li_ts_sample=old;qa_test=0;se_limit=0;ug_goodcomment_0=1;zr_km_item_cf=close;se_wannasearch=0;se_amovietab=1;top_rank=0;li_pay_banner_type=0;zr_ans_rec=gbrank;ls_fmp4=0;zr_video_recall=current_recall;pf_foltopic_usernum=50;li_qa_new_cover=0;se_movietab=1;se_featured=1;tsp_hotctr=1;se_lottery=0;top_gr_ab=0;zr_art_rec=base;se_search_feed=N;se_billboardsearch=0;se_auto_syn=0;se_webmajorob=0;tp_sticky_android=0;tp_sft_v2=d;tsp_childbillboard=1;ug_fw_answ_aut_1=0;se_preset_tech=0;se_time_threshold=0;tp_meta_card=0;tp_m_intro_re_topic=1;top_hotcommerce=1;soc_update=1;pf_newguide_vertical=0;zw_sameq_sorce=999;se_expired_ob=0;se_likebutton=0;tp_qa_toast=1;ug_newtag=0;li_android_vip=0;se_spb309=0;se_colorfultab=1;soc_notification=0;zr_search_xgb=0;se_go_ztext=0;se_college_cm=0;tp_topic_head=1',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Cache-Control': 'max-age=0',
            # 'Upgrade-Insecure-Requests': '1',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
            # 'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        # print({'https': self.ip.strip()})
        try:
            if params:
                response = requests.get(url, headers=headers, params=params, proxies=self.proxies, timeout=120)
            else:
                response = requests.get(url, headers=headers, proxies=self.proxies, timeout=120)
        except requests.exceptions.ProxyError:
            self.get_questions_list_page(url, keyword, params)
            return

        logger.log(31, '正在抓取主链接:'+ response.url)
        if response.text != None:
            # data = response.content.decode()
            data = json.loads(response.text)
            # print(data)
            if data['data']:  # 判断获取的json数据中的data['data']的value列表是否为空，可以间接判断是否还有下一页数据
                # if len(data['data']) > 1:
                #     data_list = data['data'][1:]
                # else:
                data_list = data['data']
                for news in data_list:
                    try:

                        news_type = news['target']['type']
                        # 时间判断

                        if news_type == 'answer':  # 问答类信息
                            question_title = news['target']['question']['title'].replace('<em>', '').replace('</em>','')
                            answers_url = news['target']['url']
                            question_url = news['target']['question']['url']
                            try:
                                topic_time_all = self.get_topic_time(question_url)
                                # topic_time_all = '2019-03-04 01:03:30'
                            except:
                                topic_time_all = self.get_topic_time(question_url)
                                # topic_time_all = '2019-03-04 01:03:30'
                            question_id = question_url.split('/')[-1]
                            view_url = 'https://www.zhihu.com/question/' + question_id
                            views = self.get_view(view_url)  # 获取浏览
                            url = 'https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&sort_by=created'.format(question_id, '0')
                            print(url)
                            # 传入页面的url
                            source_url = 'https://www.zhihu.com/question/{}/answers/created'.format(str(question_id))
                            if source_url not in self.set_list:  # 对url进行简单的去重，避免重复的工作量

                                self.get_answers_page(url, question_title, source_url, keyword, views, topic_time_all)

                                self.set_list.append(source_url)
                        elif news_type == 'article':  # 文章类信息
                            question_title = news['target']['title'].replace('<em>', '').replace('</em>',
                                                                                                             '')
                            item = {}
                            content = news['target']['content']
                            # item['type'] = '文章'
                            item['platform'] = '知乎'
                            crt_time = news['target']['created']
                            # #转换成localtime
                            time_local = time.localtime(float(crt_time))
                            # 转换成新的时间格式(2016-05-05 20:28:54)
                            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                            date = dt.split(' ')[0]
                            news_time = dt.split(' ')[1]
                            item['date'] = date
                            item['time'] = news_time
                            author = news['target']['author']['name']
                            item['author'] = author.replace('<em>', '')
                            item['title'] = question_title
                            # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
                            content = etree.HTML(content)
                            content = content.xpath('.//p//text()')
                            content = ''.join(content)
                            item['content'] = content
                            articles_url = news['target']['url'].split('/')[-1]
                            item['url'] = 'https://zhuanlan.zhihu.com/p/{}'.format(str(articles_url))
                            item['is_topics'] = '是'
                            item['floor'] = 0
                            item['keyword'] = keyword
                            comments_count = news['target']['comment_count']
                            item['comments_count'] = comments_count
                            item['views'] = ''
                            likes = news['target']['voteup_count']
                            item['likes'] = str(likes)
                            topic_id = articles_url
                            item['topic_id'] = topic_id
                            item['author_id'] = news['target']['author']['id']
                            item['topic_date'] = date
                            item['topic_time'] = news_time
                            item['dislikes'] = ''
                            item['content_id'] = topic_id
                            item['reposts_count'] = ''
                            item['file_code'] = '47'


                            # 做时间判断部分---------------  这个部分区分于另外一个部分
                            get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
                            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
                            if self.start_time != '':
                                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
                            else:
                                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
                            if float(get_news_time) < float(start_time):
                                pass
                            if float(start_time) <= float(get_news_time) <= float(end_time):
                                # print('爬取正文数据中.....')
                                # print(item)
                                # self.write_news_jsonfile(item)
                                self.queue.put(item)
                                # print(self.queue.get())
                                if int(comments_count) > 0:
                                    comment_id = news['target']['id']
                                    comment_url = 'https://www.zhihu.com/api/v4/articles/{}/root_comments?include=data%5B*%5D.author%2Ccollapsed%2Creply_to_author%2Cdisliked%2Ccontent%2Cvoting%2Cvote_count%2Cis_parent_author%2Cis_author&order=normal&limit=20&offset=0&status=open'.format(str(comment_id))
                                    comment_source_url = 'https://zhuanlan.zhihu.com/p/{}'.format(str(comment_id))
                                    self.floor_num = 1
                                    self.get_comment_info(comment_url, question_title, comment_source_url, keyword, topic_id, dt)
                            else:
                                pass
                    except:
                        # print(traceback.format_exc())
                        logger.error(traceback.format_exc())

                is_end = data['paging']['is_end']
                if not is_end:
                    next_url = data['paging']['next']
                    try:
                        self.get_questions_list_page(next_url, keyword)
                    except:
                        try:
                            time.sleep(5)
                            self.get_questions_list_page(next_url, keyword)
                        except:
                            time.sleep(5)
                            self.get_questions_list_page(next_url, keyword)

    def get_topic_time(self, url):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        # print(url)
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies)
        except requests.exceptions.ProxyError:
            self.get_topic_time(url)
            return
        dict_text = json.loads(response.text)
        # print(3333333, dict_text)
        created_time = dict_text['created']
        time_local = time.localtime(float(created_time))
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
        #
        # print(4444444444444, dt)
        return dt

    def get_view(self, url):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            # 'referer': 'https://www.zhihu.com/search?q=%E5%AE%9D%E9%A9%AC&range=1w&type=content',
            'User-Agent': '{}'.format(random.choice(self.user_agent))
        }
        # print(url)
        response = requests.get(url, headers=headers, proxies=self.proxies, timeout=120)
        data = etree.HTML(response.content.decode())
        views = data.xpath('.//div[@class="QuestionFollowStatus"]/div/div[2]/div/strong/text()')[0]
        # print('浏览量', views)
        return views

    # 获取回答信息
    def get_answers_page(self, url, question_title, source_url, keyword, views, topic_time_all):
        """
        获取问答类的回答列表，其中包含一条条的回答，这些回答可能还有评论，
        :param url:
        :param question_title: 问答的标题
        :param question_id: 问答的id
        :return:
        """
        logger.log(31, '获取回答信息:    ' + url)
        item = {}
        self.is_stop = False
        # accept-encoding': 'gzip, deflate, br' 在开发中携带了这个头信息，出现乱码情况，去掉这个头信息，问题解决
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9',
            'Connection': 'close',
            'cookie': 'tgw_l7_route=e0a07617c1a38385364125951b19eef8; _xsrf=PhxZhhuALHVLP9dntJMOL27yQZx34zUG',
            'upgrade-insecure-requests': '1',
            'user-agent': '{}'.format(random.choice(self.user_agent))
        }
        # url = 'https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cbadge%5B%2A%5D.topics&limit=20&offset={}&sort_by=created'.format(question_id, offset)
        # print(url)
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies, timeout=120)  # , proxies={'http':'49.79.67.253:7671'}
        except requests.exceptions.ProxyError:
            self.get_answers_page(url, question_title, source_url, keyword, views, topic_time_all)
            return

        data = json.loads(response.content)
        data_list = data['data']
        for news in data_list:
            # print(1111111111111, news)
            # item['type'] = '回答'
            item['platform'] = '知乎'
            crt_time = news['created_time']
            # #转换成localtime
            time_local = time.localtime(float(crt_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
            date = dt.split(' ')[0]
            news_time = dt.split(' ')[1]
            item['date'] = date
            item['time'] = news_time
            author = news['author']['name']
            item['author'] = author.replace('<em>', '').replace('</em>', '')
            item['title'] = question_title
            # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
            content = news['content']
            content = etree.HTML(content)
            content = content.xpath('.//p//text()')
            content = ''.join(content)
            item['content'] = content
            topic_id = str(news['id'])  # 主贴id
            source_id = url.split('/')[-2]
            answer_url = 'https://www.zhihu.com/question/{}/answer/{}'.format(source_id, topic_id)
            item['url'] = answer_url
            item['is_topics'] = '是'
            item['floor'] = 0
            item['keyword'] = keyword
            comments_count = news['comment_count']
            item['comments_count'] = comments_count
            item['views'] = views
            likes = news['voteup_count']
            item['likes'] = str(likes)
            # topic_id = str(news['id'])  # 主贴id
            item['topic_id'] = topic_id
            item['author_id'] = news['author']['id']
            item['topic_date'] = topic_time_all.split(' ')[0]
            item['topic_time'] = topic_time_all.split(' ')[1]
            item['dislikes'] = ''
            item['content_id'] = news['id']
            item['reposts_count'] = ''
            item['file_code'] = '47'

            # 做时间判断部分---------------
            get_news_time = time.mktime(time.strptime(date, "%Y-%m-%d"))
            end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
            if self.start_time != '':
                start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
            else:
                start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
            if float(get_news_time) < float(start_time):
                self.is_stop = True  # 返回的回答消息是按时间进行排序的，所以当时间小于指定时间时，就停止爬取，
                break

            if float(start_time) <= float(get_news_time) <= float(end_time):
                # print('爬取正文数据中.....')
                # print(item)
                self.queue.put(item)
                # self.write_news_jsonfile(item)
                comment_id = news['id']
                if int(comments_count) > 0:  # 获取评论信息
                    comment_url = 'https://www.zhihu.com/api/v4/answers/{}/root_comments?include=data%5B*%5D.author%2Ccollapsed%2Creply_to_author%2Cdisliked%2Ccontent%2Cvoting%2Cvote_count%2Cis_parent_author%2Cis_author&order=normal&limit=20&offset=0&status=open'.format(str(comment_id))
                    self.floor_num = 1
                    logger.info('写入评论中')
                    self.get_comment_info(comment_url, question_title, answer_url, keyword, topic_id, topic_time_all)
            else:
                pass

        if not self.is_stop:  # 当此次爬取标记为stop时，就不再执行翻页操作
            is_end = data['paging']['is_end']
            if not is_end:  # 判断是否有下一页数据
                next_page_url = data['paging']['next']
                self.get_answers_page(next_page_url, question_title, source_url, keyword, views, topic_time_all)

    def get_comment_info(self, url, question_title, source_url, keyword, topic_id, topic_time_all):
        """
        获取评论信息
        :url:
        :return:
        """
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9',
            'Connection': 'close',
            'upgrade-insecure-requests': '1',
            'user-agent': '{}'.format(random.choice(self.user_agent))
        }
        comment_item = {}
        # print(url)
        logger.log(31, '爬取评论数据中......')
        try:
            response = requests.get(url, headers=headers, proxies=self.proxies, timeout=30)  # , proxies={'http':'49.79.67.253:7671'}
        except requests.exceptions.ProxyError:
            self.get_comment_info(url, question_title, source_url, keyword, topic_id, topic_time_all)
            return

        status_code = response.status_code
        if str(status_code) == '200':
            data = json.loads(response.content)
            comment_data = data['data']
            for comments in comment_data:
                print(222222222222, comments)
                # comment_item['type'] = '评论'
                comment_item['platform'] = '知乎'
                crt_time = comments['created_time']
                # #转换成localtime
                time_local = time.localtime(float(crt_time))
                # 转换成新的时间格式(2016-05-05 20:28:54)
                dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)  # "%Y-%m-%d %H:%M:%S"
                date = dt.split(' ')[0]
                news_time = dt.split(' ')[1]
                comment_item['date'] = date
                comment_item['time'] = news_time
                author = comments['author']['member']['name']
                comment_item['author'] = author.replace('<em>', '')
                comment_item['title'] = question_title
                # content = news['content'].replace('<p>', '').replace('</p>', '').replace('<br>', '')
                content = comments['content']
                content = etree.HTML(content)
                content = content.xpath('.//p//text()')
                content = ''.join(content)
                comment_item['content'] = content
                comment_item['url'] = source_url
                comment_item['is_topics'] = '否'
                comment_item['floor'] = self.floor_num
                self.floor_num += 1
                comment_item['keyword'] = keyword
                comment_item['comments_count'] = 0
                comment_item['views'] = ''
                likes = comments['vote_count']
                comment_item['likes'] = str(likes)
                comment_item['topic_id'] = topic_id
                comment_item['author_id'] = comments['author']['member']['id']
                comment_item['topic_date'] = topic_time_all.split(' ')[0]
                comment_item['topic_time'] = topic_time_all.split(' ')[1]
                comment_item['dislikes'] = ''
                comment_item['content_id'] = comments['id']
                comment_item['reposts_count'] = ''
                comment_item['file_code'] = '47'
                # self.write_news_jsonfile(comment_item)
                self.queue.put(comment_item)
            is_end = data['paging']['is_end']
            if not is_end:
                next_url = data['paging']['next']
                self.get_comment_info(next_url, question_title, source_url, keyword, topic_id, topic_time_all)

    # 写入json文件
    def write_news_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./../zhihu/47_{}_zhihu_讨论组.json'.format(str(now_time)), 'ab') as f:
            f.write(item.encode("utf-8"))
        # self.news_jsonfile.write(item.encode("utf-8"))

    def write_comment_jsonfile(self, item):
        item = json.dumps(dict(item), ensure_ascii=False) + '\n'
        with open('./../zhihu/47_{}_zhihu_commnet.json'.format(str(now_time)), 'ab') as f:
            f.write(item.encode("utf-8"))


    def run_two(self, topic_url, queue):
        self.queue = queue
        try:
            # topic_id_list = ['https://www.zhihu.com/topic/19560462/hot', 'https://www.zhihu.com/topic/19560464/hot',  'https://www.zhihu.com/topic/19651948/hot']
            # for topic_url in topic_id_list:
            topic_id = topic_url.split('/')[-2]
            url = 'https://www.zhihu.com/api/v4/topics/{}/feeds/top_activity'.format(topic_id)
            params = {
                'include': 'data[?(target.type=topic_sticky_module)].target.data[?(target.type=answer)].target.content,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp;data[?(target.type=topic_sticky_module)].target.data[?(target.type=answer)].target.is_normal,comment_count,voteup_count,content,relevant_info,excerpt.author.badge[?(type=best_answerer)].topics;data[?(target.type=topic_sticky_module)].target.data[?(target.type=article)].target.content,voteup_count,comment_count,voting,author.badge[?(type=best_answerer)].topics;data[?(target.type=topic_sticky_module)].target.data[?(target.type=people)].target.answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics;data[?(target.type=answer)].target.annotation_detail,content,hermes_label,is_labeled,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp;data[?(target.type=answer)].target.author.badge[?(type=best_answerer)].topics;data[?(target.type=article)].target.annotation_detail,content,hermes_label,is_labeled,author.badge[?(type=best_answerer)].topics;data[?(target.type=question)].target.annotation_detail,comment_count;',
                'limit': '5',
                'after_id': '0.00000',

            }
            logger.log(31, '开始抓取模块：'+ topic_url)
            try:
                self.get_questions_list_page(url, topic_url, params)
            except:
                logger.error(traceback.format_exc())
        except:
            logger.error(traceback.format_exc())
        logger.log(31, '爬取完毕......')


    def run(self):

        try:
            topic_id_list = ['https://www.zhihu.com/topic/19560462/hot', 'https://www.zhihu.com/topic/19560464/hot',  'https://www.zhihu.com/topic/19651948/hot']
            for topic_url in topic_id_list:
                topic_id = topic_url.split('/')[-2]
                url = 'https://www.zhihu.com/api/v4/topics/{}/feeds/top_activity'.format(topic_id)
                params = {
                    'include': 'data[?(target.type=topic_sticky_module)].target.data[?(target.type=answer)].target.content,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp;data[?(target.type=topic_sticky_module)].target.data[?(target.type=answer)].target.is_normal,comment_count,voteup_count,content,relevant_info,excerpt.author.badge[?(type=best_answerer)].topics;data[?(target.type=topic_sticky_module)].target.data[?(target.type=article)].target.content,voteup_count,comment_count,voting,author.badge[?(type=best_answerer)].topics;data[?(target.type=topic_sticky_module)].target.data[?(target.type=people)].target.answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics;data[?(target.type=answer)].target.annotation_detail,content,hermes_label,is_labeled,relationship.is_authorized,is_author,voting,is_thanked,is_nothelp;data[?(target.type=answer)].target.author.badge[?(type=best_answerer)].topics;data[?(target.type=article)].target.annotation_detail,content,hermes_label,is_labeled,author.badge[?(type=best_answerer)].topics;data[?(target.type=question)].target.annotation_detail,comment_count;',
                    'limit': '5',
                    'after_id': '0.00000',

                }
                print('开始抓取模块：', topic_url)
                try:
                    self.get_questions_list_page(url, topic_url, params)
                except Exception as e:
                    logger.error(traceback.format_exc())
        except:
            logger.error(traceback.format_exc())
        logger.info('爬取完毕......')

def app_run(topic_url, queue):
    spider = ZhiHuSpider()
    try:
        spider.run_two(topic_url, queue)
    except:
        logger.critical(traceback.format_exc())

def write_news(queue):
    # print(3)
    # for i in range(10000000):
    #     item = queue.get(timeout=1800)
    #     print('写入数据中：', item)
    #     item = json.dumps(dict(item), ensure_ascii=False) + '\n'
    #     with open('./../zhihu/47_{}_zhihu_nike.json'.format(str(now_time)), 'ab') as f:
    #         f.write(item.encode("utf-8"))
    zero_num = 0
    while True:
        q_size = queue.qsize()
        total_item = ''
        if q_size > 0:
            for i in range(q_size):
                item = queue.get(timeout=600)
                # print('写入数据中......')
                item = json.dumps(dict(item), ensure_ascii=False) + '\n'
                total_item += item
            # print('写入数据中......')
            # item = queue.get(timeout=600)
            # item = json.dumps(dict(item), ensure_ascii=False) + '\n'
            # with open('./47_{}_zhihu.json'.format(str(now_time)), 'ab') as f:
            #     f.write(total_item.encode("utf-8"))
            try:
                hdfsclient.new_write('{}/{}/47_{}_{}_zhihu_talk.json'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', ''), str(datetime.now()).split(' ')[0], time_time).replace('-', '_'), total_item,encoding='utf-8')
            except:
                logging.error(traceback.format_exc())
            zero_num = 0
        else:
            zero_num += 1
        time.sleep(5)
        if zero_num > 120:
            logger.log(31, '队列中数据写入完毕......')
            break

if __name__ == "__main__":
    logger.log(31, sys.argv)
    file_path = sys.argv[1]
    # file_path = './'
    hdfsclient = HdfsClient(url='http://jq-chance-05:9870', user='dpp-executor')
    hdfsclient.makedirs('{}/{}'.format(file_path, str(datetime.now()).split(' ')[0].replace('-', '')))  # 创建每日文件夹
    time_time = str(time.time()).split('.')[0]

    topic_id_list = ['https://www.zhihu.com/topic/19560462/hot', 'https://www.zhihu.com/topic/19560464/hot',
                     'https://www.zhihu.com/topic/19651948/hot']

    queue = multiprocessing.Manager().Queue()
    pool = multiprocessing.Pool(processes=3)
    # for i in range(10):
    pool.apply_async(write_news, args=(queue,))
    for topic_url in topic_id_list:
        # print(1)
        if topic_url:
            pool.apply_async(app_run, args=(topic_url, queue))
    # print(2)
    pool.close()
    pool.join()
    logger.log(31, '程序结束.....')

