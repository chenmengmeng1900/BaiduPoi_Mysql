import hashlib
import json
import re
import urllib.parse
from datetime import datetime
import os
from db_operation.dbOperat import MysqlPool
from settings import logging
import requests
import threadpool


class SolveLog(object):

    def __init__(self, firstId, firstName, secondId, secondName, uid):
        self.url = "http://api.map.baidu.com"
        self.firstId = firstId
        self.firstName = firstName
        self.secodId = secondId
        self.secondName = secondName
        self.uid = uid

    def GetSn(self):
        # http://api.map.baidu.com/place/v2/detail?uid={}&output=json&scope=2&ak=MtAE3QWnQ2OHvMIWNOMo6sX2MEXiftN3
        queryStr = '/place/v2/detail?uid={}&scope=2&output=json&ak=D5001ca2034af279bc5d86318dac8291'.format(self.uid)
        # 对queryStr进行转码，safe内的保留字符不转换
        encodedStr = urllib.parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
        # 在最后直接追加上yoursk
        rawStr = encodedStr + '1df25dad1d56f47f69565c2775a395de'
        # md5计算出的sn值7de5a22212ffaa9e326444c75a58f9a0
        sn = hashlib.md5(urllib.parse.quote_plus(rawStr).encode('utf-8')).hexdigest()
        return sn, queryStr

    def ReqUid(self):
        (sn, queryStr) = self.GetSn()
        url = self.url + queryStr + "&sn={}".format(sn)
        try:
            response = requests.get(url, timeout=(5, 15))
            data = json.loads(response.content.decode('utf-8'))
            print("2222")
            self.ParseData(data)
        except Exception as e:
            logging.error('Solve_error:%s' % e)

    def  a (self, data):
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw = data['result']
        if raw:
            items = []
            item = []
            name = raw['name'] if raw.get('name') else ""
            if raw.get('location'):
                lat = raw['location']['lat'] if raw.get('location').get('lat') else ""
                lng = raw['location']['lng'] if raw.get('location').get('lng') else ""
            else:
                lat = ""
                lng = ""
            address = raw['address'][:250] if raw.get('address') else ""
            city = raw['city'] if raw.get('city') else ""
            province = raw['province'] if raw.get('province') else ""
            area = raw['area'] if raw.get('area') else ""
            street_id = raw['street_id'] if raw.get('street_id') else ""
            detail = raw['detail'] if raw.get('detail') else ""
            uid = raw['uid'] if raw.get('uid') else ""
            tel = raw['telephone'] if raw.get('telephone') else ""
            if raw.get('detail_info'):
                baidu_tag = raw['detail_info']['tag'] if raw.get('detail_info').get('tag') else ""
                baidu_type = raw['detail_info']['type'] if raw.get('detail_info').get('type') else ""
                detail_url = raw['detail_info']['detail_url'] if raw.get('detail_info').get('detail_url') else ""
                comment_num = raw['detail_info']['comment_num'] if raw.get('detail_info').get('comment_num') else ""
                price = raw['detail_info']['price'] if raw.get('detail_info').get('price') else ""
                show_hours = raw['detail_info']['show_hours'] if raw.get('detail_info').get('show_hours') else ""
                overall_rating = raw['detail_info']['overall_rating'] if raw.get('detail_info').get('overall_rating') else ""
                taste_rating = raw['detail_info']['taste_rating'] if raw.get('detail_info').get('taste_rating') else ""
                service_rating = raw['detail_info']['service_rating'] if raw.get('detail_info').get('service_rating') else ""
                environment_rating = raw['detail_info']['environment_rating'] if raw.get('detail_info').get('environment_rating') else ""
                facility_rating = raw['detail_info']['facility_rating'] if raw.get('detail_info').get('facility_rating') else ""
                hygiene_rating = raw['detail_info']['hygiene_rating'] if raw.get('detail_info').get('hygiene_rating') else ""
                technology_rating = raw['detail_info']['technology_rating'] if raw.get('detail_info').get('technology_rating') else ""
                groupon_num = raw['detail_info']['groupon_num'] if raw.get('detail_info').get('groupon_num') else ""
                discount_num = raw['detail_info']['discount_num'] if raw.get('detail_info').get('discount_num') else ""
                favorite_num = raw['detail_info']['favorite_num'] if raw.get('detail_info').get('favorite_num') else ""
                checkin_num = raw['detail_info']['checkin_num'] if raw.get('detail_info').get('checkin_num') else ""
                image_num = raw['detail_info']['image_num'] if raw.get('detail_info').get('image_num') else ""
            else:
                baidu_tag = ""
                baidu_type = ""
                detail_url = ""
                comment_num = ""
                price = ""
                show_hours = ""
                overall_rating = ""
                taste_rating = ""
                service_rating = ""
                environment_rating = ""
                facility_rating = ""
                hygiene_rating = ""
                technology_rating = ""
                groupon_num = ""
                discount_num = ""
                favorite_num = ""
                checkin_num = ""
                image_num = ""
            item.append(self.firstId)
            item.append(self.firstName)
            item.append(self.secodId)
            item.append(self.secondName)
            item.append(address)
            item.append(area)
            item.append(province)
            item.append(city)
            item.append(detail)
            item.append(lat)
            item.append(lng)
            item.append(name)
            item.append(tel)
            item.append(uid)
            item.append(street_id)
            item.append(detail_url)
            item.append(baidu_tag)
            item.append(baidu_type)
            item.append(price)
            item.append(show_hours)
            item.append(overall_rating)
            item.append(taste_rating)
            item.append(environment_rating)
            item.append(service_rating)
            item.append(facility_rating)
            item.append(hygiene_rating)
            item.append(technology_rating)
            item.append(image_num)
            item.append(groupon_num)
            item.append(discount_num)
            item.append(comment_num)
            item.append(favorite_num)
            item.append(checkin_num)
            item.append(create_time)
            items.append(item)
        print("***", items)
        # 插入数据库
        MysqlPool().insert(items)


def read_file():

    base_url = "http://api.map.baidu.com"
    with open('proxy.log', 'r', encoding='utf-8', errors='ignore') as f:
        datas = f.readlines()
        for data in datas:
            data_dict = {}
            if base_url in data:
                result = re.findall(r':\[(.*?)\]', data)
                if len(result):
                    data_list = list(result)[0]
                    secondId = data_list.split(r',')[2]
                    url = re.findall(r'http://api.map.baidu.com(.*?)\'', data)
                    uid = re.findall(r'uid=(.*?)&', url[0])
                    if uid:
                        data_dict['secondId'] = secondId.strip()
                        data_dict['uid'] = uid[0]
                        # req_url_list.append(data_dict)
                        yield data_dict
    # return req_url_list


def Run(req_info):
    secondId = req_info.get('secondId')
    uid = req_info.get('uid')
    for firstInfos in all_query_list:
        firstId = firstInfos.get('firstId')
        firstName = firstInfos.get('firstName')
        secondInfos = firstInfos.get('secondInfo')
        for secondInfo in secondInfos:
            seconid = secondInfo.get('seconid')
            secondName = secondInfo.get('name')
            if str(secondId) == str(seconid):
                solveLog = SolveLog(firstId, firstName, secondId, secondName, uid)
                solveLog.ReqUid()


req_url_list = []
data_dict = read_file()
for dat in data_dict:
    req_url_list.append(dat)
path = os.path.dirname(os.getcwd()) + r"\constant_data\constant.json"
file = open(path, 'r', encoding='utf-8')
all_query_list = json.load(file)
pool = threadpool.ThreadPool(1)
reqs = threadpool.makeRequests(Run, req_url_list)
[pool.putRequest(req) for req in reqs]
pool.wait()

