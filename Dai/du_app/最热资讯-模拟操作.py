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
# from config import device_name_list

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
        date = datetime.now() - timedelta(days=7)  # 七天前的时间，不包括今天
        str_time = str(date).split(' ')[0]
        yesterday = datetime.now() - timedelta(days=0)  # 昨天时间
        now_time = str(yesterday).split(' ')[0]
        print('爬取时间段：{}到{}'.format(str_time, now_time))
        self.start_time = str_time
        # self.start_time = '2019-07-25'
        # 定义结束时间 y-m-d  离现在时间近
        self.end_time = now_time
        # end_time = '2019-03-22'
        # self.phone_name = device_name_list[0]['name']
        # self.device_num = device_name_list[0]['device_num']
        # print(self.device_num)
        # self.device_1 = Android(self.device_num)
        self.device_1 = Android('DWH9X17812G01661')
        self.poco = AndroidUiautomationPoco(self.device_1, deuse_airtest_input=True, screenshot_each_action=False)

        pool = redis.ConnectionPool(host='127.0.0.1')  # 实现一个Redis连接池

        self.redis_example = redis.Redis(connection_pool=pool)

        self.nick_name = ''
        self.nick_name_num = 0
        self.is_work = True

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
        self.poco.swipe([0.5, 0.55], [0.5, 0.15], duration=0.3)
        time.sleep(0.1)

    def shop_info_hua_up(self):
        """
        商品详情页滑动
        :return:
        """
        self.poco.swipe([0.5, 0.7], [0.5, 0.4], duration=0.4)
        time.sleep(0.1)

    def comment_list_hua_up(self):
        """
        评论列表页滑动
        :return:
        """
        self.poco.swipe([0.5, 0.8], [0.5, 0.2], duration=0.2)
        time.sleep(0.1)



    def is_chinese(self , uchar):
        """判断一串字符中是否有汉字"""
        for child in uchar:
            if child >= u'\u4e00' and child <= u'\u9fa5':
                return True
            else:
                continue

    def shop_info_page(self, strat, end):


        time.sleep(2)
        self.device_1.keyevent('KEYCODE_BACK')
        time.sleep(2)
        self.device_1.keyevent('KEYCODE_BACK')


    def main_work(self):
        """
        这里进行滑动的逻辑操作
        :return:
        """
        for i in range(100):

            time_bn = self.poco(name='com.shizhuang.duapp:id/tv_last_time')
            if not time_bn:
                continue
            for btn in time_bn:
                try:
                    get_time = btn.get_text().replace('月', '-').replace('日', '')
                    print(get_time)
                    get_time = self.time_change(get_time)
                    print(get_time)
                    # 做时间判断部分---------------
                    get_news_time = time.mktime(time.strptime(get_time, "%Y-%m-%d"))
                    end_time = time.mktime(time.strptime(self.end_time, "%Y-%m-%d"))
                    if self.start_time != '':
                        start_time = time.mktime(time.strptime(self.start_time, "%Y-%m-%d"))
                    else:
                        start_time = time.mktime(time.strptime('2010-1-1', "%Y-%m-%d"))
                    if float(get_news_time) < float(start_time):
                        self.is_work = False
                        return

                    if '天' in get_time:
                        if '1' not in get_time and '2' not in get_time and '3' not in get_time and '4' not in get_time and '5' not in get_time and '6' not in get_time:
                            self.is_work = False
                            return
                    # if float(start_time) <= float(get_news_time) <= float(end_time):
                except:
                    continue

            if not self.is_work:
                break
            else:
                self.hua_up()

    def time_change(self, str_time):
        """
        时间可是转换， 将‘分钟前’，‘小时前’，‘昨天’，‘前天’, '天前'，转换成标准时间格式Y-m-d h:m:s
        :param str_time:
        :return:
        """
        if '秒' in str_time or '刚刚' in str_time:
            get_time = str(datetime.now()).split('.')[0]
            return get_time

        elif '分钟' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60
            int_time = int(str(time.time()).split('.')[0]) - get_time_num
            # #转换成localtime
            time_local = time.localtime(float(int_time))
            # 转换成新的时间格式(2016-05-05 20:28:54)
            dt = time.strftime("%Y-%m-%d", time_local)  # "%Y-%m-%d %H:%M:%S"
            return dt

        elif '小时' in str_time:
            get_time_num = re.search('\d{1,2}', str_time).group(0)
            get_time_num = int(get_time_num) * 60 * 60
            # print(get_time_num)
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
        elif 3 <= len(str_time) < 7:
            str_time = str(datetime.now()).split('-')[0] + '-' + str_time
            return str_time

        else:
            # str_time = '2019-' + str_time.replace('月', '-').replace('日', '')
            return str_time

    def run(self):
        if not self.poco(text='最热资讯'):
            self.device_1.shell('input keyevent 26')
            time.sleep(1)
            self.poco.swipe([0.5, 0.9], [0.5, 0.4], duration=0.3)
            time.sleep(5)
        self.poco(text='最热资讯').click()

        self.main_work()
        time.sleep(2)
        self.device_1.keyevent('KEYCODE_BACK')


if __name__ == "__main__":
    spider = DouYin()
    spider.run()


