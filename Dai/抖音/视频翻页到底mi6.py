# -*- encoding=utf-8 -*-

__author__ = "Administrator"

from airtest.core.api import *
from poco.drivers.android.uiautomation import AndroidUiautomationPoco
import traceback
from airtest.core.android import Android
from datetime import datetime
from datetime import timedelta
import re
import redis
from config import device_name_list
import multiprocessing

# # 通过系统时间自动计算时间间隔
# date = datetime.now() - timedelta(days=3)  # 七天前的时间，不包括今天
# str_time = str(date).split(' ')[0]
#
# yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
#
# now_time = str(yesterday).split(' ')[0]
# print('爬取时间段：{}到{}'.format(str_time, now_time))
# start_time = str_time
# # start_time = '2019-02-23'
# # 定义结束时间 y-m-d  离现在时间近
# end_time = now_time
# # end_time = '2019-03-22'


class DouYin():
    '''
    操作模拟器，操控软件
    '''
    def __init__(self):
        # 通过系统时间自动计算时间间隔
        a = str(datetime.now())
        hour = a.split(' ')[-1].split(':')[0]
        num = int(hour) / 3
        num = int(num) * 3
        if num == 0:  # 对于凌晨 0 点的判断
            # 时间判断部分
            date = datetime.now() - timedelta(days=1)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        else:
            # 时间判断部分
            date = datetime.now() - timedelta(days=0)
            news_start_time = str(date).split(' ')[0]
            yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
            yesterday = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(news_start_time, yesterday))
        self.start_time = news_start_time
        # self.start_time = '2019-09-09'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = yesterday
        # end_time = '2019-03-22'
        self.phone_name = device_name_list[0]['device_id']
        self.device_num = device_name_list[0]['device_num']
        print(self.device_num)
        self.device_1 = Android(self.device_num)
        self.poco = AndroidUiautomationPoco(self.device_1, deuse_airtest_input=True, screenshot_each_action=False)
        self.keyword_list = ['奔驰 C级', '奔驰 GLA', '奔驰 S级']
        # '宝马 X1', '宝马 X2','宝马 3系','宝马 7系', '宝马 X5', '宝马 X7',, '奔驰 GLE'
        self.size = self.poco.get_screen_size()
        self.is_break = False
        self.same_num = 0  # 表示评论数相同的计数，用来判断视频翻页是否到底
        self.up_title = ''

        pool = redis.ConnectionPool(host='127.0.0.1')  # 实现一个Redis连接池

        self.redis_example = redis.Redis(connection_pool=pool)

        self.zhu_num = 0  # 视频的翻页数，主要用来断点续爬，多少个视频之后开始采集评论

        self.is_first = True   # 是否是第一次启动的第一个关键词，主要用来断点续爬

        self.keyword = ''


    def hua(self):
        '''
        评论滑动
        :return:
        '''
        # print(size)
        x = self.size[0]
        # 获取屏幕宽
        y = self.size[1]
        self.poco.swipe([(1 / 2)*x / x, (4 / 5)*y / y], [(1 / 2)*x / x, (1 / 4)*y / y], duration=0.2)


    def first_hua(self):
        '''
        防止评论输入模式滑动
        :return:
        '''
        x = self.size[0]
        # 获取屏幕宽
        y = self.size[1]
        # # 向上滑
        self.poco.swipe([(1 / 2)*x / x, (3 / 4)*y / y], [(1 / 2)*x / x, (1 / 6)*y / y], duration=0.1)


    def hua_zhuping(self):
        '''
        视频列表页滑动
        :return:
        '''
        # 获取屏幕的高
        x = self.size[0]
        # 获取屏幕宽
        y = self.size[1]
        # 向上滑动
        # self.poco.swipe([(1 / 2) * x / x, (4 / 7) * y / y], [(1 / 2) * x / x, (3 / 7) * y / y], duration=0.1)
        self.poco.swipe([(1 / 2)*x / x, (5 / 6)*y / y], [(1 / 2)*x / x, (1 / 6)*y / y], duration=0.35)
        time.sleep(0.1)

    def hua_up(self):
        '''
        视频列表页滑动
        :return:
        '''

        # # 获取屏幕的高
        # x = self.size[0]
        # # 获取屏幕宽
        # y = self.size[1]
        # # 向上滑动
        # self.poco.swipe([(1 / 2) * x / x, (4 / 7) * y / y], [(1 / 2) * x / x, (3 / 7) * y / y], duration=0.1)
        self.poco.swipe([0.5, 0.6], [0.5, 0.8], duration=0.2)
        time.sleep(0.1)


    def is_chinese(self , uchar):
        """判断一串字符中是否有汉字"""
        for child in uchar:
            if child >= u'\u4e00' and child <= u'\u9fa5':
                return True
            else:
                continue

    def main_work(self):
        """
        这里进行滑动的逻辑操作
        :return:
        """
        for i in range(300):
            print(i)
            souye = self.poco(text='首页')
            if souye:
                return
            print('进行一次主页滑动')
            self.hua_zhuping()
            time.sleep(0.5)
            if self.poco(text='暂时没有更多了'):
                break
        # time.sleep(5)
        # self.device_1.keyevent('KEYCODE_BACK')
        # time.sleep(5)
        # self.device_1.keyevent('KEYCODE_BACK')
        # time.sleep(5)
        # self.device_1.keyevent('KEYCODE_BACK')
        # time.sleep(5)
        # self.device_1.keyevent('KEYCODE_BACK')
        # time.sleep(5)
        # self.device_1.keyevent('KEYCODE_BACK')
        print('目前返回到抖音首页.....')
        time.sleep(5)

    def input_keyword(self, keyword, num):
        """
        这里进行点击输入框，输入文本操作，并搜索
        """

        # try:
        #     print('点击输入文本框-1')
        #     search_btn = self.poco(name='com.ss.android.ugc.aweme:id/aex')[0]
        #     search_btn.click()
        #     time.sleep(1)
        # except:
        #     print('点击输入文本框-2')
        #     self.poco(name='com.ss.android.ugc.aweme:id/aex').click()
        #     time.sleep(1)
        if num > 0:
            self.poco(name='com.ss.android.ugc.aweme:id/a5j').click()
        else:
            self.poco(type='android.widget.EditText').click()
        print('输入搜索关键词')
        self.poco(type='android.widget.EditText').set_text(keyword)
        time.sleep(1)
        print('点击搜索按钮')
        self.poco(text='搜索').click()
        time.sleep(2)
        # self.poco(text='视频').click()
        # time.sleep(1)
        # self.poco.swipe([0.3, 0.3], [0.3, 0.3], duration=0.2)
        time.sleep(15)

    def time_change(self, str_time):
        """
        时间可是转换， 将‘分钟前’，‘小时前’，‘昨天’，‘前天’, '天前'，转换成标准时间格式Y-m-d h:m:s
        :param str_time:
        :return:
        """
        if '秒' in str_time:
            get_time = str(datetime.now()).split('.')[0]
            return get_time

        elif '分钟' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60
            print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '小时' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60
            print(get_time_num)
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '昨天' in str_time:
            try:
                part_time = str_time.split(' ')[1]
                yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
            except:
                yesterday = datetime.now() - timedelta(days=1)  # 昨天时间
                yesterday = str(yesterday).split(' ')[0]
            return yesterday

        elif '前天' in str_time:
            part_time = str_time.split(' ')[1]
            two_days_ago = datetime.now() - timedelta(days=2)  # 昨天时间
            two_days_ago = str(two_days_ago).split(' ')[0] + ' ' + part_time.replace('点', ':').replace('分', '')
            return two_days_ago

        elif '天前' in str_time:
            part_time = str_time.split('天前')[0]
            two_days_ago = datetime.now() - timedelta(days=int(part_time))  # 昨天时间
            two_days_ago = str(two_days_ago).split(' ')[0]
            return two_days_ago

        elif '年' in str_time:
            str_time = str_time.replace('年', '-').replace('月', '-').replace('日', '')
            return str_time
        else:
            # str_time = '2019-' + str_time.replace('月', '-').replace('日', '')
            return str_time

    def run(self):

        self.homing()

        # if not self.poco(text='首页'):
        #     self.device_1.shell('input keyevent 26')
        #     time.sleep(1)
        #     self.poco.swipe([0.5, 0.9], [0.5, 0.4], duration=0.3)
        #     time.sleep(5)
        # try:
        #     self.poco(name='com.ss.android.ugc.aweme:id/ap1').click()
        #     print('点击搜索按钮-1')
        #     time.sleep(1)
        # except:
        #     print(traceback.format_exc())
        #     print('点击搜索按钮-2')
        #     self.poco(name='com.ss.android.ugc.aweme:id/agt').click()
        #     time.sleep(1)
        keyword_list = eval(self.redis_example.get('keyword_list'))
        print(type(keyword_list))
        # for keyword in self.keyword_list:
        for i in range(1000):   # 通过将关键词存入rides中，然后各个爬虫实例来取
            # self.homing()
            time.sleep(5)
            keyword_index = int(self.redis_example.get('keyword_index'))
            if keyword_index > len(keyword_list):  # 判断关键词是否爬取完毕
                print('关键词爬取完毕......,最后一个爬取关键词为：', self.keyword)
                break
            self.keyword = keyword_list[keyword_index]
            print('获得关键词：', self.keyword)
            keyword_index += 1
            self.redis_example.set('keyword_index', keyword_index)
            self.redis_example.set(self.phone_name, self.keyword)
            # time.sleep(30)
            print(111111, self.redis_example.get(self.phone_name).decode('utf8'))
            # try:
            if i == 0:

                self.poco(desc='搜索').click()
                # except:
                #     time.sleep(10)
                #     self.device_1.keyevent('KEYCODE_BACK')
                #     time.sleep(10)
                #     self.poco(desc='搜索').click()
                time.sleep(10)
                self.input_keyword(self.keyword, i)  # 输入关键词
                self.main_work()  # 活动操作
            else:
                self.input_keyword(self.keyword, i)  # 输入关键词
                self.main_work()  # 活动操作

    def homing(self):

        for i in range(6):
            souye = self.poco(text='关注')
            if not souye:
                self.device_1.keyevent('KEYCODE_BACK')
                time.sleep(5)
            else:
                print('返回到首页.....')
                time.sleep(20)
                break


def run():
    spider = DouYin()
    try:
        spider.run()
    except:
        print(traceback.format_exc())


if __name__ == "__main__":

    pool = multiprocessing.Pool(1)
    pool.apply_async(run)
    pool.close()
    # pool.join()
    start_time = time.time()
    while True:
        if (float(time.time()) - float(start_time)) > 28800:
            print(u'爬取时间已经达到八小时，结束进程任务')
            pool.terminate()
            time.sleep(5)
            spider = DouYin()
            spider.homing()
            break
        time.sleep(1)
    print('程序结束......')
