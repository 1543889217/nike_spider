import json
from collections import OrderedDict
import redis
import time

pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
# pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)

# 读取京东url链接以便去重
f_anazon = open('./goods_url/20200407_amazon_img_url.json'.format('pk'), 'r', encoding='utf-8')

goods_anazon_list = []
for line in f_anazon.readlines():
    dic_data = json.loads(line)
    goods_anazon_list.append(dic_data)
print('去重前亚马逊商品链接数：', len(goods_anazon_list))

a_anazon = goods_anazon_list
goods_anazon_url = OrderedDict()

# 对url链接去重
for item in a_anazon:
    goods_anazon_url.setdefault(item['url'], {**item, })
anazon_nike_url = list(goods_anazon_url.values())
print('去重之后亚马逊商品链接数：', len(anazon_nike_url))

# 将去重的链接存入Redis数据库待抓取
# for line in anazon_nike_url:
#     # print(line)
#     redis_example.lpush('anazon_day_url', json.dumps(line))

# 京东耐克
num = redis_example.llen('anazon_day_url')  # 回复时间队列
print('亚马逊每日-----Reids中等待爬取的主贴数为：', num)




