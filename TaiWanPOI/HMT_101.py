import logging
import requests
import json
import urllib.parse
import hashlib
from jsonpath import jsonpath
import math
import threadpool
from datetime import datetime
from db_operation.dbOperat import MysqlPool


class Cut(object):

    def __init__(self, name, firstId, firstName, secondId):
        self.url = "http://api.map.baidu.com"
        self.query = name
        self.firstId = firstId
        self.firstName = firstName
        self.secodId = secondId

    def GetSn(self, query, page_num, bounds):
        # http://api.map.baidu.com/place/v2/search?query=ATM机&tag=银行&region=北京&output=json&ak=您的ak //GET请求
        queryStr = '/place/v2/search?query={}&page_size=20&scope=2&page_num={}&bounds={}&output=json&ak=D5001ca2034af279bc5d86318dac8291'.format(query, page_num, bounds)
        # 对queryStr进行转码，safe内的保留字符不转换
        encodedStr = urllib.parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
        # 在最后直接追加上yoursk
        rawStr = encodedStr + '1df25dad1d56f47f69565c2775a395de'
        # md5计算出的sn值7de5a22212ffaa9e326444c75a58f9a0
        sn = hashlib.md5(urllib.parse.quote_plus(rawStr).encode('utf-8')).hexdigest()
        return sn, queryStr

    def CutChina(self, rect):
        xl_yl = rect.split(r',')
        xmin = xl_yl[0]
        ymin = xl_yl[1]
        xmax = xl_yl[2]
        ymax = xl_yl[3]
        bounds = str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax)
        (sn, queryStr) = self.GetSn(self.query, 0, bounds)
        url = self.url + queryStr +"&sn={}".format(sn)
        jsonData = self.ReqData(url)
        if jsonData:
            try:
                total = int(jsonpath(jsonData, expr="$..total")[0])
            except Exception as e:
                logging.info('error_total:%s' % e)
                total = 0
            if total > 20:
                page = math.ceil(total / 20)
                for i in range(0, page):
                    (sn, queryStr) = self.GetSn(self.query, i, bounds)
                    url = self.url + queryStr + "&sn={}".format(sn)
                    jsonData = self.ReqData(url)
                    self.ParseData(jsonData, xmin, ymin, xmax, ymax)
            elif total > 0 and total <=20:
                # 解析数据存入数据
                self.ParseData(jsonData, xmin, ymin, xmax, ymax)

    def ReqData(self, url):
        # 有可能请求失败，做二次请求
        for i in range(2):
            try:
                response = requests.get(url=url, timeout=(5, 27))
                html = json.loads(response.content.decode('utf-8'))
                if html.get('message') == "天配额超限，限制访问" or html.get('message') == "当前并发量已经超过约定并发配额，限制访问":
                    print("天配额超限，限制访问")
                    pool.wait()
                    return
                return html
            except Exception as e:
                continue

    def ParseData(self, data, xmin, ymin, xmax, ymax):
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = data['results']
        items = []
        for raw in results:
            item = []
            if raw:
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
                item.append(self.query)
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
        print(items)
        # 插入数据库
        MysqlPool().insert(items)
        MysqlPool().update_data(self.query, xmin, ymin, xmax, ymax)

if __name__ == "__main__":
    # 一天的时间跑完
    all_query_list = [{"firstId": 100, "firstName": "旅游景区",
      "secondInfo": [{"name": "风景区", "seconid": 101}]},
     {"firstId": 200, "firstName": "旅游景点",
      "secondInfo": [{"name": "公园", "seconid": 201},
                     {"name": "动物园", "seconid": 202},
                     {"name": "植物园", "seconid": 203},
                     {"name": "游乐园", "seconid": 204},
                     {"name": "博物馆", "seconid": 205},
                     {"name": "水族馆", "seconid": 206},
                     {"name": "海滨浴场", "seconid": 207},
                     {"name": "文物古迹", "seconid": 208},
                     {"name": "教堂", "seconid": 209},
                     {"name": "岛屿", "seconid": 210},
                     {"name": "山峰", "seconid": 211},
                     {"name": "水系", "seconid": 212}]},
     {"firstId": 300, "firstName": "住宿酒店",
      "secondInfo": [{"name": "星级酒店", "seconid": 301},
                     {"name": "快捷酒店", "seconid": 302},
                     {"name": "公寓式酒店", "seconid": 303}]},
     {"firstId": 400, "firstName": "交通设施",
      "secondInfo": [{"name": "飞机场", "seconid": 401},
                     {"name": "火车站", "seconid": 402},
                     {"name": "地铁站", "seconid": 403},
                     {"name": "长途汽车站", "seconid": 404},
                     {"name": "公交车站", "seconid": 405},
                     {"name": "港口", "seconid": 406},
                     {"name": "停车场", "seconid": 407},
                     {"name": "加油加气站", "seconid": 408},
                     {"name": "服务区", "seconid": 409},
                     {"name": "收费站", "seconid": 410},
                     {"name": "桥", "seconid": 411},
                     {"name": "充电站", "seconid": 412},
                     {"name": "路侧停车位", "seconid": 413},
                     {"name": "高速公路出口", "seconid": 414},
                     {"name": "高速公路入口", "seconid": 415},
                     {"name": "机场出口", "seconid": 416},
                     {"name": "机场入口", "seconid": 417},
                     {"name": "车站出口", "seconid": 418},
                     {"name": "车站入口", "seconid": 419},
                     {"name": "门", "seconid": 420},
                     {"name": "停车场出入口", "seconid": 421}]},
     {"firstId": 500, "firstName": "商场购物",
      "secondInfo": [{"name": "购物中心", "seconid": 501},
                     {"name": "百货商场", "seconid": 502},
                     {"name": "超市", "seconid": 503},
                     {"name": "便利店", "seconid": 504},
                     {"name": "家居建材", "seconid": 505},
                     {"name": "家电数码", "seconid": 506},
                     {"name": "商铺", "seconid": 507},
                     {"name": "集市", "seconid": 508}]},
     {"firstId": 600, "firstName": "美食餐饮",
      "secondInfo": [{"name": "中餐厅", "seconid": 601},
                     {"name": "外国餐厅", "seconid": 602},
                     {"name": "小吃快餐店", "seconid": 603},
                     {"name": "蛋糕甜品店", "seconid": 604},
                     {"name": "咖啡厅", "seconid": 605},
                     {"name": "茶座", "seconid": 606},
                     {"name": "酒吧", "seconid": 607}]},
     {"firstId": 700, "firstName": "文体娱乐",
      "secondInfo": [{"name": "度假村", "seconid": 701},
                     {"name": "农家院", "seconid": 702},
                     {"name": "电影院", "seconid": 703},
                     {"name": "KTV", "seconid": 704},
                     {"name": "剧院", "seconid": 705},
                     {"name": "歌舞厅", "seconid": 706},
                     {"name": "网吧", "seconid": 707},
                     {"name": "游戏场所", "seconid": 708},
                     {"name": "洗浴按摩", "seconid": 709},
                     {"name": "休闲广场", "seconid": 710},
                     {"name": "新闻出版", "seconid": 711},
                     {"name": "广播电视", "seconid": 712},
                     {"name": "艺术团体", "seconid": 713},
                     {"name": "美术馆", "seconid": 714},
                     {"name": "展览馆", "seconid": 715},
                     {"name": "文化宫", "seconid": 716},
                     {"name": "美容", "seconid": 717},
                     {"name": "美发", "seconid": 718},
                     {"name": "美甲", "seconid": 719},
                     {"name": "美体", "seconid": 720},
                     {"name": "体育场馆", "seconid": 721},
                     {"name": "极限运动场所", "seconid": 722},
                     {"name": "健身中心", "seconid": 723}]},
     {"firstId": 800, "firstName": "生活服务",
      "secondInfo": [{"name": "通讯营业厅", "seconid": 801},
                     {"name": "邮局", "seconid": 802},
                     {"name": "物流公司", "seconid": 803},
                     {"name": "售票处", "seconid": 804},
                     {"name": "洗衣店", "seconid": 805},
                     {"name": "图文快印店", "seconid": 806},
                     {"name": "照相馆", "seconid": 807},
                     {"name": "房产中介机构", "seconid": 808},
                     {"name": "公用事业", "seconid": 809},
                     {"name": "维修点", "seconid": 810},
                     {"name": "家政服务", "seconid": 811},
                     {"name": "殡葬服务", "seconid": 812},
                     {"name": "彩票销售点", "seconid": 813},
                     {"name": "宠物服务", "seconid": 814},
                     {"name": "报刊亭", "seconid": 815},
                     {"name": "公共厕所", "seconid": 816}]},
     {"firstId": 900, "firstName": "医疗教育",
      "secondInfo": [{"name": "高等院校", "seconid": 901},
                     {"name": "中学", "seconid": 902},
                     {"name": "小学", "seconid": 903},
                     {"name": "幼儿园", "seconid": 904},
                     {"name": "成人教育", "seconid": 905},
                     {"name": "亲子教育", "seconid": 906},
                     {"name": "特殊教育学校", "seconid": 907},
                     {"name": "留学中介机构", "seconid": 908},
                     {"name": "科研机构", "seconid": 909},
                     {"name": "培训机构", "seconid": 910},
                     {"name": "图书馆", "seconid": 911},
                     {"name": "科技馆", "seconid": 912},
                     {"name": "综合医院", "seconid": 913},
                     {"name": "专科医院", "seconid": 914},
                     {"name": "诊所", "seconid": 915},
                     {"name": "药店", "seconid": 916},
                     {"name": "体检机构", "seconid": 917},
                     {"name": "疗养院", "seconid": 918},
                     {"name": "急救中心", "seconid": 919},
                     {"name": "疾控中心", "seconid": 920}]},
     {"firstId": 1000, "firstName": "政府机构",
      "secondInfo": [{"name": "中央机构", "seconid": 1001},
                     {"name": "各级政府", "seconid": 1002},
                     {"name": "行政单位", "seconid": 1003},
                     {"name": "公检法机构", "seconid": 1004},
                     {"name": "涉外机构", "seconid": 1005},
                     {"name": "党派团体", "seconid": 1006},
                     {"name": "福利机构", "seconid": 1007},
                     {"name": "政治教育机构", "seconid": 1008}]},
     {"firstId": 1100, "firstName": "金融服务",
      "secondInfo": [{"name": "银行", "seconid": 1101},
                     {"name": "ATM", "seconid": 1102},
                     {"name": "信用社", "seconid": 1103},
                     {"name": "投资理财", "seconid": 1104},
                     {"name": "典当行", "seconid": 1105}]},
     {"firstId": 1200, "firstName": "其他",
      "secondInfo": [{"name": "公司", "seconid": 1201},
                     {"name": "园区", "seconid": 1202},
                     {"name": "农林园艺", "seconid": 1203},
                     {"name": "厂矿", "seconid": 1204},
                     {"name": "汽车销售", "seconid": 1205},
                     {"name": "汽车维修", "seconid": 1206},
                     {"name": "汽车美容", "seconid": 1207},
                     {"name": "汽车配件", "seconid": 1208},
                     {"name": "汽车租赁", "seconid": 1209},
                     {"name": "汽车检测场", "seconid": 1210},
                     {"name": "写字楼", "seconid": 1211},
                     {"name": "住宅区", "seconid": 1212},
                     {"name": "宿舍", "seconid": 1213}]}
     ]
    for info in all_query_list:
        second_data = info.get("secondInfo")
        firstId = info.get("firstId")
        firstName = info.get('firstName')
        print(firstName)
        for second in second_data:
            name = second.get('name')
            secondId = second.get('seconid')
            rows = MysqlPool().selectData(name)
            print("&&", rows)
            print(second)
            args_list = []
            for row in rows:
                string = str(row[1]) + ',' + str(row[2]) + ',' + str(row[3]) + ',' + str(row[4])
                args_list.append(string)
            cut = Cut(name, firstId, firstName, secondId)
            pool = threadpool.ThreadPool(1)
            reqs = threadpool.makeRequests(cut.CutChina, args_list)
            [pool.putRequest(req) for req in reqs]
            pool.wait()
            break
        break

    print("程序完成结束")


