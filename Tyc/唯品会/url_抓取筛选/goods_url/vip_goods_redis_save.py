import json
from collections import OrderedDict
import redis
import time

# pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
pool = redis.ConnectionPool(host='192.168.1.11')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)

# 读取京东url链接以便去重
f_vip = open('./new_weipinhui_img_url_20200402.json'.format('pk'), 'r', encoding='utf-8')

goods_VIP_list = []
for line in f_vip.readlines():
    dic_data = json.loads(line)
    goods_VIP_list.append(dic_data)
print('去重前唯品会商品链接数：', len(goods_VIP_list))

a_VIP = goods_VIP_list
goods_VIP_url = OrderedDict()

# 对url链接去重
for item in a_VIP:
    goods_VIP_url.setdefault(item['url'], {**item, })
WPH_day_url = list(goods_VIP_url.values())
print('去重之后唯品会商品链接数：', len(WPH_day_url))

# for data in WPH_day_url:
#     items = json.dumps(dict(data), ensure_ascii=False) + '\n'
#     with open('./goods_url/{}_VIP_url.json'.format('test'), 'ab') as f:
#         f.write(items.encode("utf-8"))

# # 将去重的链接存入Redis数据库待抓取
# for line in WPH_day_url:
#     # print(line)
#     redis_example.lpush('WPH_day_url', json.dumps(line))

# 京东耐克
num = redis_example.llen('WPH_day_url')  # 回复时间队列
print('唯品会每日-----Reids中等待爬取的主贴数为：', num)

# for i in range(5555000):
#
#     item = redis_example.lpop('WPH_day_url')
#
#     item = json.loads(item.decode())
#     print(item)




