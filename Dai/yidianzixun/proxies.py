import requests
import json
import random


def res_ip():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'webapi.http.zhimacangku.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
    }
    # 5-25分 500个ip
    import time
    url = 'http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
    ip_pro = requests.get(url, headers=headers)
    # print(ip_pro.text)
    # ip_data = json.loads(ip_pro.text)
    if '请1秒后再试' in ip_pro.text:
        time.sleep(random.randint(4, 20))
        return res_ip()
    ip = ip_pro.text.strip()
    return ip


if __name__ == '__main__':
    res_ip()