import json
from collections import OrderedDict
import redis
import time

pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
# pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)

# 读取京东url链接以便去重
f_suning = open('./goods_url/20200407_suning_img_url.json'.format('pk'), 'r', encoding='utf-8')

goods_suning_list = []
for line in f_suning.readlines():
    dic_data = json.loads(line)
    goods_suning_list.append(dic_data)
print('去重前苏宁商品链接数：', len(goods_suning_list))

a_suning = goods_suning_list
goods_suning_url = OrderedDict()

# 对url链接去重
for item in a_suning:
    goods_suning_url.setdefault(item['URL'], {**item, })
suning_day_url = list(goods_suning_url.values())
print('去重之后苏宁商品链接数：', len(suning_day_url))

# for data in suning_day_url:
#     items = json.dumps(dict(data), ensure_ascii=False) + '\n'
#     with open('./goods_url/{}_Suning_url.json'.format('test'), 'ab') as f:
#         f.write(items.encode("utf-8"))

# 将去重的链接存入Redis数据库待抓取
# for line in suning_day_url:
#     # print(line)
#     redis_example.lpush('suning_day_url', json.dumps(line))

# 京东耐克
num = redis_example.llen('suning_day_url')  # 回复时间队列
print('苏宁每日-----Reids中等待爬取的主贴数为：', num)


# for i in range(1550000):
#
#     item = redis_example.lpop('suning_day_url')
#
#     item = json.loads(item.decode())
#     print(item)



