"""
通过百度地图爬取POI数据-分割中国地块
由于百度矩形获取POI智能返回400条数据，
采用接口方式：矩阵接口方式
代码思路：1.test_spider文件夹以二级分类为整体作为query，爬取这个二级分类的POI的整张地图的经纬度
            1.1：　1.txt 文件因为深度递归，python有递归限制，以及栈溢出问题，解决方案：先将中国地图拆分多个矩阵，保存在1.txt中，在去爬该矩阵内的ＰＯＩ经纬度
            1.2：　req_error_test.txt 无法保证带宽的影响，将请求崩溃的记录在这个文件
            1.3   unfinished.txt 因为账号每天限制额度为30万请求，会出现额度限制而请求不到数据，将中断的请求记录在这个文件中
        2. MapPOI 文件夹是爬取某一个二级分类保存的经纬度以及有参数，命名规则是二级分类的字符串截取前两位
        3. spider_data 文件夹是爬取数据入库
            3.1 proxy.log 文件插入失败的这条数据的原因以及item
            3.2 req_error.txt 文件是请求失败记录这条经纬度
            3.3 unfinshed_url.txt 记录限额的未爬取的经纬度数据
        4. db_operation文件夹是数据入库的代码，并发十条线程入库，会大量出现无法链接数据库的错误，可能用异步连接解决，或者数据库本身性能不好导致
"""
import requests, os
import time
import json
import urllib.parse
import hashlib
import sys
from decimal import Decimal
import threadpool
from db_operation.dbOperat import MysqlPool
from datetime import datetime
from db_operation.dbOperat import FilterUrl
from settings import logging
from settings import mainLand

sys.setrecursionlimit(1000000)  # 括号中的值为递归深度


# 矩形框
class Rect(object):
    def __init__(self, xmin, ymin, xmax, ymax):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax


class Cut(object):
    def __init__(self, queryList, firstClassfyId):
        self.queryList = queryList
        self.firstClassfyId = firstClassfyId
        self.url = "http://api.map.baidu.com"

    def GetSn(self, bounds):
        queryStr = '/place/v2/search?query={}&page_size=20&bounds={}&output=json&ak=D5001ca2034af279bc5d86318dac8291'.format(
            self.queryList, bounds)
        # 对queryStr进行转码，safe内的保留字符不转换
        encodedStr = urllib.parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
        # 在最后直接追加上yoursk
        rawStr = encodedStr + '1df25dad1d56f47f69565c2775a395de'
        # md5计算出的sn值7de5a22212ffaa9e326444c75a58f9a0
        sn = hashlib.md5(urllib.parse.quote_plus(rawStr).encode('utf-8')).hexdigest()
        return sn, queryStr

    # 切分地块
    def CutChina(self, rect):
        print(self.queryList)
        if rect not in queryFilterUrl:
            if rect in avg_lat_lng:
                filterUrl.SetAdd(self.queryList, rect)
                global sign_rect
                sign_rect = rect
            xl_yl = rect.split(r',')
            xmin = Decimal(xl_yl[0])
            ymin = Decimal(xl_yl[1])
            xmax = Decimal(xl_yl[2])
            ymax = Decimal(xl_yl[3])
            bounds = str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax)
            (sn, queryStr) = self.GetSn(bounds)
            url = self.url + queryStr + "&sn={}".format(sn)
            data = self.DownHtml(url=url)
            try:
                jsonData = json.loads(data)
                if jsonData.get('message') == "天配额超限，限制访问":
                    print("天配额超限，限制访问")
                    filterUrl.removeData(self.queryList, sign_rect)
                    # 若限额后程序直接阻塞
                    pool.wait()
            except Exception as e:
                logging.error("json_loads_error: %s" % url)
                jsonData = {}
            if jsonData.get('total'):
                count = int(jsonData["total"])
                print(count)
                if count < 400:
                    item = []
                    status = 1
                    createTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    item.append(self.queryList)
                    item.append(xmin)
                    item.append(ymin)
                    item.append(xmax)
                    item.append(ymax)
                    item.append(status)
                    item.append(createTime)
                    item.append(self.firstClassfyId)
                    item.append(url)
                    item.append("")
                    MysqlPool().insert_lng_lat(item)

                else:
                    print("555")
                    middleX = (xmin + xmax) / 2
                    middleY = (ymin + ymax) / 2
                    rect1 = str(xmin) + "," + str(ymin) + "," + str(middleX) + "," + str(middleY)
                    rect2 = str(middleX) + "," + str(ymin) + "," + str(xmax) + "," + str(middleY)
                    rect3 = str(xmin) + "," + str(middleY) + "," + str(middleX) + "," + str(ymax)
                    rect4 = str(middleX) + "," + str(middleY) + "," + str(xmax) + "," + str(ymax)
                    # 使用递归调用
                    time.sleep(0.1)  # 休眠1秒
                    self.CutChina(rect1)
                    time.sleep(0.1)  # 休眠1秒
                    self.CutChina(rect=rect2)
                    time.sleep(0.1)  # 休眠1秒
                    self.CutChina(rect=rect3)
                    time.sleep(0.1)  # 休眠1秒
                    self.CutChina(rect=rect4)
        else:
            print("去除重复！")

    # 获取经纬度
    def DownHtml(self, url):
        # 有可能请求失败，做二次请求
        for i in range(2):
            try:
                i += 1
                response = requests.get(url=url, timeout=(5, 27))
                html = response.text
                return html
            except Exception as e:
                if i == 1:
                    # 任务失败则移除当前经纬度所在的矩阵
                    filterUrl.removeData(self.queryList, sign_rect)
                continue


class Select(object):
    def __init__(self):
        pass

    def average(self, number):
        xmin = 17.725738
        ymin = 71.234018
        xmax = 55.28893
        ymax = 136.139681
        a = (xmax - xmin) / number
        b = (ymax - ymin) / number
        # 将中国版面的地图拆分成90000份
        f = open('1.txt', 'w', encoding='utf-8')
        f.write("")
        f.close()
        for i in range(number):
            for j in range(number):
                lat_lng = str(xmin + i * a) + "," + str(ymin + j * b) + "," + str(xmin + (i + 1) * a) + "," + str(
                    ymin + (j + 1) * b) + "\n"
                with open('1.txt', 'a', encoding='utf-8') as f:
                    f.write(lat_lng)

    def readFile(self):
        with open(r'1.txt', 'r') as f:
            response = f.readlines()
            for i in response:
                yield i


if __name__ == "__main__":
    filterUrl = FilterUrl(db=mainLand)
    queryDictDay1 = [{"queryList": "风景区", "number": 50, "firstClassfyId": 100},
                     {"queryList": "公园$动物园$植物园$游乐园$博物馆$水族馆$海滨浴场$文物古迹$教堂$岛屿", "number": 100, "firstClassfyId": 200},
                     {"queryList": "山峰$水系", "number": 50, "firstClassfyId": 200},
                     {"queryList": "星级酒店$快捷酒店$公寓式酒店$酒店", "number": 50, "firstClassfyId": 300},
                     {"queryList": "飞机场$火车站$地铁站$长途汽车站$公交车站$港口$停车场$加油加气站$收费站$桥", "number": 50, "firstClassfyId": 400},
                     {"queryList": "服务区$桥$充电站$路侧停车位", "number": 50, "firstClassfyId": 400},
                     {"queryList": "高速公路出口$高速公路入口$机场出口$机场入口$车站出口$车站入口$门$停车场出入口", "number": 50, "firstClassfyId": 400},
                     {"queryList": "购物中心$百货商场$超市$便利店$家居建材$家电数码$商铺$集市", "number": 200, "firstClassfyId": 500},
                     {"queryList": "中餐厅$外国餐厅$小吃快餐店$蛋糕甜品店$咖啡厅$茶座$酒吧$美食", "number": 100, "firstClassfyId": 600},
                     {"queryList": "度假村$农家院$电影院$KTV$剧院$歌舞厅$网吧$游戏场所$洗浴按摩$休闲广场", "number": 50, "firstClassfyId": 700},
                     {"queryList": "新闻出版$广播电视$艺术团体$美术馆$展览馆$文化宫", "number": 50, "firstClassfyId": 700},
                     {"queryList": "美容$美发$美甲$美体", "number": 50, "firstClassfyId": 700},
                     {"queryList": "体育场馆$极限运动场所$健身中心", "number": 50, "firstClassfyId": 700},
                     {"queryList": "通讯营业厅$邮局$物流公司$售票处$洗衣店$图文快印店$照相馆$房产中介机构$公用事业$维修点", "number": 50,
                      "firstClassfyId": 800},
                     {"queryList": "家政服务$殡葬服务$彩票销售点$宠物服务$报刊亭$公共厕所", "number": 20, "firstClassfyId": 800},
                     {"queryList": "高等院校$中学$小学$幼儿园$成人教育$亲子教育$特殊教育学校$留学中介机构$科研机构$培训机构", "number": 20,
                      "firstClassfyId": 900},
                     {"queryList": "图书馆$科技馆", "number": 10, "firstClassfyId": 900},
                     {"queryList": "综合医院$专科医院$诊所$药店$体检机构$疗养院$急救中心$疾控中心", "number": 10, "firstClassfyId": 900},
                     {"queryList": "中央机构$各级政府$行政单位$公检法机构$涉外机构$党派团体$福利机构$政治教育机构", "number": 10, "firstClassfyId": 1000},
                     {"queryList": "银行$ATM$信用社$投资理财$典当行", "number": 10, "firstClassfyId": 1100},
                     {"queryList": "公司$园区$农林园艺$厂矿", "number": 100, "firstClassfyId": 1200},
                     {"queryList": "汽车销售$汽车维修$汽车美容$汽车配件$汽车租赁$汽车检测场", "number": 20, "firstClassfyId": 1200},
                     {"queryList": "写字楼$住宅区$宿舍", "number": 100, "firstClassfyId": 1200}]

    # 当拆的分数较少，区域请求获取的数据较多，并发八条的时候，数据库插入失败
    queryFilterUrl = []
    for queryDictDay in queryDictDay1:
        queryList = queryDictDay.get("queryList")
        # 将拆分的矩阵参数放入redis中，防止程序中断或者额度不够，异常,实现增量式采集
        queryFilterUrl = filterUrl.ReadSmembers(queryList)
        number = queryDictDay.get("number")
        firstClassfyId = queryDictDay.get("firstClassfyId")
        avg_lat_lng = []
        sel = Select()
        average = sel.average(number)
        select = sel.readFile()
        for i in select:
            avg_lat_lng.append(i)
        pool = threadpool.ThreadPool(8)
        cut = Cut(queryList, firstClassfyId)
        reqs = threadpool.makeRequests(cut.CutChina, avg_lat_lng)
        [pool.putRequest(req) for req in reqs]
        pool.wait()
        print("程序完成结束")
