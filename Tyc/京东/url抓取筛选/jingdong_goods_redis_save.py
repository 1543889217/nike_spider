import json
from collections import OrderedDict
import redis
import time

pool = redis.ConnectionPool(host='192.168.1.208', port=6379, password='chance@123')  # 实现一个Redis连接池
# pool = redis.ConnectionPool(host='192.168.1.20')  # 实现一个Redis连接池
redis_example = redis.Redis(connection_pool=pool)

# 读取京东url链接以便去重
f_jing_dong = open('./goods_url/20200407_jingdong_img_url.json'.format('pk'), 'r', encoding='utf-8')

goods_jingdong_list = []
for line in f_jing_dong.readlines():
    dic_data = json.loads(line)
    goods_jingdong_list.append(dic_data)
print('去重前京东商品链接数：', len(goods_jingdong_list))

a_jingdong = goods_jingdong_list
goods_jingdong_url = OrderedDict()

# 对url链接去重
for item in a_jingdong:
    # if item['商品名'] == '':
    #     pass
    # else:
    goods_jingdong_url.setdefault(item['goods_id'], {**item, })
jing_dong_url = list(goods_jingdong_url.values())
print('去重之后京东商品链接数：', len(jing_dong_url))

# for data in jing_dong_url:
#     items = json.dumps(dict(data), ensure_ascii=False) + '\n'
#     with open('./goods_url/{}_Jingdong_url.json'.format('test'), 'ab') as f:
#         f.write(items.encode("utf-8"))

# 将去重的链接存入Redis数据库待抓取
# for line in jing_dong_url:
#     # print(line)
#     redis_example.lpush('JingDong_day_url', json.dumps(line))

# 京东耐克
num = redis_example.llen('JingDong_day_url')  # 回复时间队列
print('京东每日-----Reids中等待爬取的主贴数为：', num)




