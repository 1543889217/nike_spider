import json
import random
import time
import uuid
from urllib import parse

import requests


class XhsApi:

    USER_AGENT = 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; Redmi 5A MIUI/V9.6.2.0.NCKCNFD) Resolution/720*1280 Version/5.45.0 Build/5450095 Device/(Xiaomi;Redmi 5A)'

    COMMON_DEVICE_PARAMS = {
        'platform': 'android',
        'versionName': '5.45.0',
        'channel': 'xiaohongshu',
        'lang': 'zh-Hans',
    }

    HOST = 'http://47.105.95.219:8080/'

    URL_BASE = HOST + 'xhsapi/'

    def __init__(self, cid):
        self.__cid = cid
        self.__device_id = 'abbd5bf5-3a82-3fcd-b8b8-4e4c48f68950'
        self.__imei = ''
        self.__sid = ''
        self.__smid = ''
        self.__adid = ''
        self.__cookie = {}

        # 代理服务器
        proxyHost = "http-cla.abuyun.com"
        proxyPort = "9030"

        # 代理隧道验证信息
        proxyUser = "H3487178Q0I1HVPC"
        proxyPass = "ACE81171D81169CA"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            "http": proxyMeta,
            "https": proxyMeta
        }



    def get_api_access_info(self):
        """获取接口使用情况
        :return:
        """
        querys = {
            'cid': self.__cid,
            'api': 'xiaohongshu'
        }

        response = requests.get(XhsApi.HOST + "info/getApiAccessInfo?" + parse.urlencode(querys))
        return response.text

    def register_smid(self):
        """注册设备
        :return:
        """
        self.__imei = '86866' + self.__get_random(10)
        self.__adid = '7664' + str(uuid.uuid4())[-12:]
        bssid = self.__get_random_mac()
        serial = '8e' + str(uuid.uuid4())[-10:]
        imsi = '4600' + self.__get_random(11)
        iccid = '898601168' + self.__get_random(11)
        imei2 = '86866' + self.__get_random(10)
        ts = int(round(time.time() * 1000))

        api_url = self.URL_BASE + "getDeviceRegisterInfo?cid=" + self.__cid
        form_params = {
            'bssid': bssid,
            'imei': self.__imei,
            'imei2': imei2,
            'serialno': serial,
            'imsi': imsi,
            'ts': str(ts),
            'adid': self.__adid,
            'iccid': iccid
        }

        register_info = requests.post(api_url, data=form_params).text
        print(register_info)
        reg_url = 'http://fp-it.fengkongcloud.com/v3/profile/android'
        headers = {
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; Redmi 5A MIUI/V9.6.2.0.NCKCNFD)'
        }
        return requests.post(reg_url, data=register_info, headers=headers).text

    def register_smid_proxy(self, proxy):
        """注册设备
        :return:
        """
        self.__imei = '86866' + self.__get_random(10)
        self.__adid = '7664' + str(uuid.uuid4())[-12:]
        bssid = self.__get_random_mac()
        serial = '8e' + str(uuid.uuid4())[-10:]
        imsi = '4600' + self.__get_random(11)
        iccid = '898601168' + self.__get_random(11)
        imei2 = '86866' + self.__get_random(10)
        ts = int(round(time.time() * 1000))

        api_url = self.URL_BASE + "getDeviceRegisterInfo?cid=" + self.__cid
        form_params = {
            'bssid': bssid,
            'imei': self.__imei,
            'imei2': imei2,
            'serialno': serial,
            'imsi': imsi,
            'ts': str(ts),
            'adid': self.__adid,
            'iccid': iccid
        }

        register_info = requests.post(api_url, data=form_params).text
        print(register_info)
        reg_url = 'http://fp-it.fengkongcloud.com/v3/profile/android'
        headers = {
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; Redmi 5A MIUI/V9.6.2.0.NCKCNFD)'
        }
        return requests.post(reg_url, data=register_info, headers=headers, proxies={'http': proxy}).text

    def set_smid(self, smid):
        """初始化设备参数
        :return:
        """
        self.__device_id = str(uuid.uuid4())
        self.__smid = smid
        # print('cloudconf:' + self.__init_cloudconf())
        # print('app_launch:' + self.__app_launch())

    def get_home_feed(self):
        """获取首页推荐
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v6/homefeed?oid=homefeed_recommend'
        query_params = {
            'cursor_score': '',
            'trace_id': str(uuid.uuid4()),
            'note_index': '0',
            'refresh_type': '2'
        }
        return self.__http_get(xhs_url, query_params)

    def get_user_info(self, user_id):
        """获取用户信息
        :param user_id: user id
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v3/user/' + user_id + '/info?'
        return self.__http_get(xhs_url)

    def get_user_note(self, user_id, page, page_size):
        """获取用户笔记
        :param user_id: user id
        :param page: 页码
        :param page_size: 每次返回的条数
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v3/note/user/' + user_id
        query_params = {
            'page': str(page),
            'page_size': str(page_size),
            'sub_tag_id': ''
        }
        return self.__http_get(xhs_url, query_params)

    def get_note(self, note_id):
        url = 'https://www.xiaohongshu.com/api/sns/v1/note/{}/feed'.format(note_id)
        query_params = {
            'page': 1,
            'num': 5,
            'fetch_mode': 1,
            'ads_track_id': '',
            'fid': '',
        }
        return self.__http_get(url, query_params)

    def get_note_comments(self, note_id, num, start=''):
        """获取note评论
        :param note_id: note id
        :param num: 每次返回的条数
        :param start: 评论id，首页不传，下一页传上一页最后一个评论id
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v5/note/' + note_id + '/comment/list?'
        query_params = {
            'start': start,
            'num': str(num),
            'show_priority_sub_comments': '0',
            'fid': ''
        }
        return self.__http_get(xhs_url, query_params)

    def get_user_videos(self, user_id, page, page_size):
        """获取用户视频
        :param user_id: user id
        :param page: 页码
        :param page_size: 每次返回的条数
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v3/note/user/' + user_id
        query_params = {
            'page': str(page),
            'page_size': str(page_size),
            'sub_tag_id': 'special.video'
        }
        return self.__http_get(xhs_url, query_params)

    def get_user_followings(self, user_id, start=''):
        """获取用户关注的用户列表
        :param user_id: user id
        :param start: 关注列表中的用户id，首页不传，下一页传上一页最后一个用户id
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/user/' + user_id + '/followings?'
        query_params = {
            'start': start
        }
        return self.__http_get(xhs_url, query_params)

    def get_user_followers(self, user_id, start=''):
        """获取用户粉丝
        :param user_id: user id
        :param start: 粉丝列表中的用户id，首页不传，下一页传上一页最后一个用户id
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/user/' + user_id + '/followers?'
        query_params = {
            'start': start
        }
        return self.__http_get(xhs_url, query_params)

    def search(self, keyword, page, page_size):
        """搜索
        :param keyword: 关键词
        :param page: 页码，最小1
        :param page_size: 每次返回的条数
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v9/search/notes?'
        query_params = {
            'keyword': keyword,
            'filters': '',
            'sort': 'time_descending',
            'page': str(page),
            'page_size': str(page_size),
            'source': 'explore_feed',
            'search_id': '',
            'api_extra': '',
            'page_pos': str(page_size * (page - 1)),
            'allow_rewrite': '1',
            'fid': ''
        }
        return self.__http_get(xhs_url, query_params)

    def get_topic_notes(self, topic_id, page, page_size, sort='time'):
        """获取话题相关的日记
        :param topic_id: 话题id
        :param page: 页码
        :param page_size: 每次返回的条数
        :return:
        """
        # xhs_url = 'https://www.xiaohongshu.com/fe_api/burdock/v1/page/' + topic_id + "/notes?"
        xhs_url = 'https://www.xiaohongshu.com/fe_api/burdock/v2/page/' + topic_id + "/notes?"
        query_params = {
            'page': str(page),
            'pageSize': str(page_size),
            'sid': self.__sid,
            # 'sid': 'session.1570584984409448341951',
            'sort': sort,
        }
        return self.__http_get_xsign(xhs_url, query_params)

    def send_sms_code(self, phone_number):
        """发送短信验证码
        :param phone_number: 手机号码
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/system_service/vfc_code?'
        query_params = {
            'zone': '86',
            'phone': str(phone_number),
            'type': 'login'
        }
        return self.__http_get(xhs_url, query_params)

    def check_sms_code(self, phone_number, code):
        """校验短信验证码
        :param phone_number: 手机号码
        :param code: 验证码
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/system_service/check_code?'
        query_params = {
            'zone': '86',
            'phone': str(phone_number),
            'code': str(code),
        }
        return self.__http_get(xhs_url, query_params)

    def login_with_sms(self, phone_number, mobile_token):
        """ 短信验证码登录
        :param phone_number: 手机号码
        :param mobile_token: token，由check_sms_code返回
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v4/user/login/code'
        form_params = {
            'phone': str(phone_number),
            'imei': self.__imei,
            'zone': '86',
            'type': 'phone',
            'mobile_token': mobile_token,
            'android_id': self.__adid
        }
        return self.__http_post(xhs_url, form_params)

    def set_session_id(self, session_id):
        """设置session id
        :param session_id: session id
        :return:
        """
        self.__sid = 'session.' + session_id

    def active_user(self):
        """激活用户，获取sid
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/user/activate'
        form_params = {
            'imei': self.__imei,
            'android_id': self.__adid
        }

        ret = self.__http_post(xhs_url, form_params)
        return ret

    def active_user_proxy(self, proxy):
        """激活用户，获取sid
        :return:
        """
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/user/activate'
        form_params = {
            'imei': self.__imei,
            'android_id': self.__adid
        }

        ret = self.__http_post_proxy(xhs_url, proxy, form_params)
        return ret

    def encrypt_dev_info(self, register_info):
        """加密设备注册信息
        :param register_info:
        :return:
        """
        api_url = self.URL_BASE + "encryptDevInfo?cid=" + self.__cid
        return requests.post(api_url, data=register_info).text

    def base64_encode(self, str_to_encode):
        """base64编码
        :param str_to_encode:
        :return:
        """
        api_url = self.URL_BASE + "base64Encode?cid=" + self.__cid
        return requests.post(api_url, data=str_to_encode).text

    def base64_decode(self, str_to_decode):
        """base64解码
        :param str_to_decode:
        :return:
        """
        api_url = self.URL_BASE + "base64Decode?cid=" + self.__cid
        return requests.post(api_url, data=str_to_decode).text

    def __init_cloudconf(self):
        xhs_url = 'http://fp-it.fengkongcloud.com/v3/cloudconf'

        headers = {
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; Redmi 5A MIUI/V9.6.2.0.NCKCNFD)'
        }

        infos = {
            "organization": "eR46sBuqF0fdw7KWFLYa",
            "data": {
                "sdkver": "2.8.4",
                "enc": 1,
                "smid": self.__smid,
                "md5": "988751ca8e09f518984ff7b0bc98753a",
                "os": "android"
            }
        }
        return requests.post(xhs_url, data=str(json.dumps(infos)), headers=headers).text

    def __app_launch(self):
        xhs_url = 'https://www.xiaohongshu.com/api/sns/v1/system_service/launch?'
        query_params = {
            'build': '5450095',
            'package': 'discovery'
        }
        return self.__http_get(xhs_url, query_params)

    def __get_random(self, len):
        return ''.join(str(random.choice(range(10))) for _ in range(len))

    def __get_random_mac(self):
        mac = [0xf6, 0xf5, 0xdb,
               random.randint(0x00, 0x7f),
               random.randint(0x00, 0xff),
               random.randint(0x00, 0xff)]
        return ':'.join(map(lambda x: "%02x" % x, mac))

    def __get_sign_url(self):
        querys = {
            'cid': self.__cid
        }

        sign_url = self.URL_BASE + 'getSign?' + parse.urlencode(querys)
        return sign_url

    def __get_xsign_url(self):
        querys = {
            'cid': self.__cid
        }

        sign_url = self.URL_BASE + 'getXSign?' + parse.urlencode(querys)
        return sign_url

    def __http_get_xsign(self, url, query_params={}):
        params = parse.urlencode(query_params)
        if len(query_params) != 0:
            if not url.__contains__('?'):
                url = url + '?'

            if url.endswith('?') or url.endswith('&'):
                url = url + params
            else:
                url = url + '&' + params

        try:
            xsign_url = self.__get_xsign_url()
            form_params = {
                'url': url
            }

            xsign_resp = requests.post(xsign_url, data=form_params).json()
            print(11111, xsign_resp)
            headers = {
                'User-Agent': self.USER_AGENT,
                'X-Sign': xsign_resp['X-Sign']
                # 'X-Sign': 'Xe72b02d957d2bdbd1f40351992375e50'
            }
            return requests.get(url, headers=headers, proxies=self.proxies).text
        except Exception as e:
            print(repr(e))

    def get_xsign(self, url, query_params={}):
        params = parse.urlencode(query_params)
        if len(query_params) != 0:
            if not url.__contains__('?'):
                url = url + '?'

            if url.endswith('?') or url.endswith('&'):
                url = url + params
            else:
                url = url + '&' + params

        try:
            xsign_url = self.__get_xsign_url()
            form_params = {
                'url': url
            }

            xsign_resp = requests.post(xsign_url, data=form_params).json()
            print(22222, xsign_resp)
            headers = {
                'User-Agent': self.USER_AGENT,
                'X-Sign': xsign_resp['X-Sign']
                # 'X-Sign': 'Xe72b02d957d2bdbd1f40351992375e50'
            }
            return requests.get(url, headers=headers).text
        except Exception as e:
            print(repr(e))


    def __get_sign(self, url, params={}):
        form_params = {
            'url': url
        }

        form_params.update(params)
        # 获取sign和shield
        for i in range(10):
            try:
                sign_resp = requests.post(self.__get_sign_url(), data=form_params, timeout=60).json()
                if 'ret' in sign_resp:
                    print(sign_resp)
                    return ""

                if 'sign' not in sign_resp or 'shield' not in sign_resp:
                    continue

                sign = sign_resp['sign']
                shield = sign_resp['shield']
                if len(sign) == 0 or len(shield) == 0:
                    continue
                return sign_resp
            except Exception as e:
                print(repr(e))

    def __http_post(self, url, form_params={}):
        form_params.update(self.COMMON_DEVICE_PARAMS)
        device_params = {
            'deviceId': self.__device_id,
            'device_fingerprint': self.__smid,
            'device_fingerprint1': self.__smid
        }

        form_params.update(device_params)
        form_params['t'] = str(round(time.time()))
        form_params['sid'] = self.__sid

        sign = self.__get_sign(url, form_params)
        form_params['sign'] = sign['sign']
        header = {
            'device_id': self.__device_id,
            'User-Agent': self.USER_AGENT,
            'Authorization': self.__sid,
            'shield': sign['shield']
        }

        # 请求小红书
        resp = requests.post(url, data=form_params, headers=header, cookies=self.__cookie)
        self.__cookie = resp.cookies.get_dict()
        return resp.text

    def __http_post_proxy(self, url, proxy, form_params={}):
        form_params.update(self.COMMON_DEVICE_PARAMS)
        device_params = {
            'deviceId': self.__device_id,
            'device_fingerprint': self.__smid,
            'device_fingerprint1': self.__smid
        }

        form_params.update(device_params)
        form_params['t'] = str(round(time.time()))
        form_params['sid'] = self.__sid

        sign = self.__get_sign(url, form_params)
        form_params['sign'] = sign['sign']
        header = {
            'device_id': self.__device_id,
            'User-Agent': self.USER_AGENT,
            'Authorization': self.__sid,
            'shield': sign['shield']
        }

        # 请求小红书
        resp = requests.post(url, data=form_params, headers=header, cookies=self.__cookie, proxies={'http': proxy})
        self.__cookie = resp.cookies.get_dict()
        return resp.text

    def __http_get(self, url, query_params={}):
        query_params.update(self.COMMON_DEVICE_PARAMS)
        device_params = {
            'deviceId': self.__device_id,
            'device_fingerprint': self.__smid,
            'device_fingerprint1': self.__smid
        }

        query_params.update(device_params)
        query_params['t'] = str(round(time.time()))
        query_params['sid'] = self.__sid

        sign = self.__get_sign(url, query_params)
        print(sign)
        query_params['sign'] = sign['sign']
        header = {
            'device_id': self.__device_id,
            'User-Agent': self.USER_AGENT,
            'Authorization': self.__sid,
            'shield': sign['shield']
        }

        params = parse.urlencode(query_params)
        if not url.__contains__('?'):
            url = url + '?'

        if url.endswith('?') or url.endswith('&'):
            url = url + params
        else:
            url = url + '&' + params

        # 请求小红书
        resp = requests.get(url, headers=header, proxies=self.proxies)
        # self.__cookie = resp.cookies.get_dict()
        return resp.text

    def get_sign(self, url, par):
        return self.__get_sign(url , par)