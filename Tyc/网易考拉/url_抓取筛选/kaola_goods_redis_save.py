import json
from collections import OrderedDict
import redis
import time

# pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)

# 读取京东url链接以便去重
f_kaola = open('./goods_url/20200407_kaola_img_url.json'.format('pk'), 'r', encoding='utf-8')

goods_kaola_list = []
for line in f_kaola.readlines():
    dic_data = json.loads(line)
    goods_kaola_list.append(dic_data)
print('去重前考拉商品链接数：', len(goods_kaola_list))

a_kaola = goods_kaola_list
goods_kaola_url = OrderedDict()

# 对url链接去重
for item in a_kaola:
    goods_kaola_url.setdefault(item['url'], {**item, })
kaola_day_url = list(goods_kaola_url.values())
print('去重之后考拉商品链接数：', len(kaola_day_url))

# for data in kaola_day_url:
#     items = json.dumps(dict(data), ensure_ascii=False) + '\n'
#     with open('./goods_url/{}_kaola_url.json'.format('test'), 'ab') as f:
#         f.write(items.encode("utf-8"))

# 将去重的链接存入Redis数据库待抓取
# for line in kaola_day_url:
#     # print(line)
#     redis_example.lpush('kaola_day_url', json.dumps(line))

# 京东耐克
num = redis_example.llen('kaola_day_url')  # 回复时间队列
print('考拉每日-----Reids中等待爬取的主贴数为：', num)


# for i in range(1550000):
#
#     item = redis_example.lpop('kaola_day_url')
#
#     item = json.loads(item.decode())
#     print(item)

