import time
from with_hdfs import HdfsClient
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.gevent import GeventScheduler
from datetime import datetime
from datetime import timedelta
import logging
import traceback
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

# 设置日志记录
LOG_FORMAT = "%(asctime)s %(filename)s %(levelname)s %(lineno)d %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S '   # 配置输出时间的格式，注意月份和天数不要搞乱了
yesterday = datetime.now() - timedelta(days=9)  # 昨天时间
yesterday = str(yesterday).split(' ')[0]
# file_name = r"./timed_task-{}.log".format(str(datetime.now()).split(' ')[0])
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )
# headle = logging.FileHandler(filename=file_name, encoding='utf-8')
logger = logging.getLogger()
# logger.addHandler(headle)

import os
executors = {
    # 'default': ThreadPoolExecutor(20),  # 线程
    'processpool': ProcessPoolExecutor(30)  # 进程   两者可同时启用也可以单独启用
}
sched = BlockingScheduler(executors=executors)
sched_two = BackgroundScheduler(executors=executors)

time_time = str(time.time()).split('.')[0]
cli = HdfsClient(url='http://192.168.1.205:14000', user='dpp-executor')


def upload_du_article():
    """
    上传 毒 文章
    :return:
    """
    logger.info('开始上传 毒 文章数据')
    try:
        cli.upload('/user/cspider_daily/nike_daily/article/{}'.format(str(datetime.now()).split(' ')[0].replace('-', '')),'./../du_app/json_file/{}/156_{}_zuixing_article.json'.format(str(datetime.now()).split(' ')[0],str(datetime.now()).split(' ')[0].replace('-', '_')), overwrite=True)
        logger.info('毒 文章数据 上传完成!')
    except:
        print('./../du_app/json_file/{}/158_{}_zuixing_article.json'.format(str(datetime.now()).split(' ')[0],str(datetime.now()).split(' ')[0].replace('-', '_')), '文件不存在.....')


def upload_du_daren():
    """
    上传 毒 达人
    :return:
    """
    logger.info('开始上传 毒 达人数据')
    try:
        cli.upload('/user/cspider_daily/nike_daily/qa/{}'.format(str(datetime.now()).split(' ')[0].replace('-', '')),'./../du_app/json_file/{}/158_{}_du_app_da_ren.json'.format(str(datetime.now()).split(' ')[0],str(datetime.now()).split(' ')[0].replace('-', '_')), overwrite=True)
        logger.info('毒 达人数据 上传完成!')
    except:
        print('./../du_app/json_file/{}/158_{}_du_app_da_ren.json'.format(str(datetime.now()).split(' ')[0],str(datetime.now()).split(' ')[0].replace('-', '_')), '文件不存在.....')


def upload_douyin_file():
    """
    上传 抖音数据
    :return:
    """

    logger.info('开始上传 抖音 视频数据')
    try:
        cli.upload('/user/cspider_daily/nike_daily/video/{}'.format(str(datetime.now()).split(' ')[0].replace('-', '')),'./../抖音/131_douyin_video_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_')), overwrite=True)
        logger.info('抖音 视频数据 上传完成!')
    except:
        logger.error('./../抖音/131_douyin_video_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_'))+'文件不存在.....')
    logger.info('开始上传 抖音 评论数据')
    try:
        cli.upload('/user/cspider_daily/nike_daily/videocomments/{}'.format(str(datetime.now()).split(' ')[0].replace('-', '')),'./../抖音/132_douyin_video_comment_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_')), overwrite=True)
        logger.info('抖音 评论数据 上传完成!')
    except:
        logger.error('./../抖音/132_douyin_video_comment_{}.json'.format(str(datetime.now()).split(' ')[0].replace('-', '_')) + '文件不存在.....')


# sched.add_job(func=du_app, trigger='interval', hours=3, id='du_app', max_instances=2, misfire_grace_time=600, start_date='2019-08-08 12:01:00')
# sched.add_job(func=du_app_da_ren, trigger='interval', hours=3, id='du_app_da_ren', max_instances=2, misfire_grace_time=600, start_date='2019-08-08 12:01:00')
sched.add_job(func=upload_du_article, trigger='cron', hour=7, minute=50, second=31, id='upload_du_article', max_instances=2, misfire_grace_time=600)
sched.add_job(func=upload_du_daren, trigger='cron', hour=7, minute=50, second=31, id='upload_du_daren', max_instances=2, misfire_grace_time=600)
sched.add_job(func=upload_douyin_file, trigger='cron', hour=7, minute=50, second=31, id='upload_douyin_file', max_instances=2, misfire_grace_time=600)

def err_listener(event):
    if event.exception:
        logging.error(traceback.format_exc())
    else:
        logging.info('{} miss'.format(str(event.job)))


sched_two.add_listener(err_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

print('定时任务开启')
logging.info('定时任务开启')
sched.start()