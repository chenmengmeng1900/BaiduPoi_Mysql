import pymysql
import redis
from settings import Redis_Host, Redis_Port, Redis_Pwd
from settings import DB_CONF
import time
from settings import logging


class MysqlPool(object):

    def __init__(self):
        pass

    def connect_db(self):
        conn = pymysql.connect(host=DB_CONF['host'],
                                    port=DB_CONF['port'],
                                    db=DB_CONF['db'],
                                    user=DB_CONF['user'],
                                    passwd=DB_CONF['passwd'],
                                    charset=DB_CONF['charset'])
        # 创建游标
        cur = conn.cursor()
        return conn, cur

    # 插入一条数据测试
    def insert(self, items):
        for item in items:
            try:
                conn, cur = self.connect_db()
                time.sleep(0.05)
            except Exception as e:
                logging.info('error_connect%s:%s' % (e, str(item)))
                return
            try:
                if item[9]:
                    sql = "INSERT INTO BaiDuMapPoi(firstClassfyID, firstClassfyName, secondClassfyId, secondClassfyName, address, area, province, city, detail, lat, lng, name, tel, uid, street_id, detailUrl, baiduTag, baiduType, price, showHours, overallRating, testeRating, environmentRating, serviceRating, facilityRating, hygieneRating, technologyRating, imageNum, groupNum, discountNum, commentNum, favoriteNum, checkinNum, createTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    cur.execute(sql, (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13], item[14], item[15], item[16], item[17], item[18], item[19], item[20], item[21], item[22], item[23], item[24], item[25], item[26], item[27], item[28], item[29], item[30], item[31], item[32], item[33]))
                    conn.commit()
                    print("插入成功！")
            except Exception as e:
                logging.error('insert_proxy_error%s:%s' % (e, str(item)))
                conn.rollback()
            finally:
                self.free_close_db(cur, conn)

    def free_close_db(self, cur, conn):
        cur.close()
        conn.close()

    def insert_lng_lat(self, item):
        try:
            print("item", item)
            conn, cur = self.connect_db()
            time.sleep(0.05)
        except Exception as e:
            logging.info('error_connect%s:%s' % (e, str(item)))
            return
        try:
            sql = "INSERT INTO BaiDuMapClassfyLatLng(queryName, LdLat, LdLng, RuLat, RuLng, status, createTime, secondId, detail_url,rs_1) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
            cur.execute(sql, (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9]))
            conn.commit()
            print("插入成功！")
        except Exception as e:
            logging.error('insert_proxy_error%s:%s' % (e, str(item)))
            conn.rollback()
        finally:
            self.free_close_db(cur, conn)

    def selectData(self, firstName):
        rows = set()
        try:
            conn, cur = self.connect_db()
        except Exception as e:
            logging.info('error_connect%s:%s' % (e, firstName))
            return
        try:
            sql = "SELECT queryName, LdLat, LdLng, RuLat, Rulng FROM BaiDuMapClassfyLatLng WHERE queryName LIKE '%%%%%s%%%%' and status = 1;" % firstName
            cur.execute(sql)
            conn.commit()
            rows = cur.fetchall()
        except Exception as e:
            logging.error("select_data_error: %s" % e)
            conn.rollback()
        finally:
            self.free_close_db(cur, conn)
            return rows

    def update_data(self, queryName, xmin, ymin, xmax, ymax):
        sql = "UPDATE BaiDuMapClassfyLatLng SET  status = 2 WHERE queryName = {} and LdLat={} and LdLng={} and RuLat={} and Rulng={}".format(queryName, xmin, ymin, xmax, ymax)
        try:
            conn, cur = self.connect_db()
        except Exception as e:
            logging.info('error_connect%s:%s' % (sql,))
            return
        try:
            cur.execute(sql)
            conn.commit()
            print("修改成功")
        except Exception as e:
            logging.error("select_data_error: %s" % e)
            conn.rollback()
        finally:
            self.free_close_db(cur, conn)

    def getCount(self, seconid):
        count = 1
        try:
            conn, cur = self.connect_db()
        except Exception as e:
            logging.info('error_connect%s:%s' % (e,))
            return
        try:
            sql = "SELECT count(*) FROM BaiDuMapPoi WHERE secondClassfyId=%s;"
            cur.execute(sql, (seconid,))
            conn.commit()
            count = cur.fetchall()
            print("插入成功！")
        except Exception as e:
            logging.error('insert_proxy_error%s:%s' % (e,))
            conn.rollback()
        finally:
            self.free_close_db(cur, conn)
            return count


class FilterUrl(object):

    def __init__(self, host=Redis_Host, port=Redis_Port, password=Redis_Pwd, db=None):
        rdp = redis.ConnectionPool(host=host, port=port, password=password, db=db, decode_responses=True)
        self.db = redis.StrictRedis(connection_pool=rdp)

    def SetAdd(self, name, values):
        try:
            status = self.db.sadd(name, values)
        except Exception as e:
            logging.error("sadd_data_error: %s %s" % (name, values))

    def ReadSmembers(self, name):
        status = ""
        try:
            name_data = name.encode("utf-8")
            status = self.db.smembers(name_data)
        except Exception as e:
            logging.error("smembers_data_error: %s" % e)
        finally:
            return status

    def removeData(self, name, values):
        status = ""
        try:
            status = self.db.srem(name, values)
        except Exception as e:
            logging.error("srem_data_error: %s %s" % (name, values))







# # 异步链接MySQL，插入数据
# class MySqlConnectPool(object):
#
#     def __init__(self):
#         self.db = {"host": DB_CONF['host'],
#                     "port": DB_CONF['port'],
#                     "db": DB_CONF['db'],
#                     "user": DB_CONF['user'],
#                     "passwd": DB_CONF['passwd'],
#                     "charset": DB_CONF['charset']}
#
#         self.db_conn = adbapi.ConnectionPool('pymysql', **self.db)
#
#     def connect_process(self, item):
#         try:
#             query = self.db_conn.runInteraction(self.insert_lng, item)
#             # query.addCallbacks(self.handle_error)
#             reactor.callLater(1, reactor.stop)
#             reactor.run()
#         except Exception as e:
#             print("%s" % e)
#
#     def insert_lng(self, cursor, item):
#         try:
#             sql = "INSERT INTO BaiDuMapClassfyLatLng(queryName, LdLat, LdLng, RuLat, RuLng, status, createTime, secondId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
#             cursor.execute(sql, item)
#         except Exception as e:
#             print("MysqlPool_insert:%s" % e)
#             logging.error("MysqlPool_insert:%s" % e)
#
#     def handle_error(self, failure):
#         if failure:
#             print('MysqlPool_error_sql:%s' % failure)
#             logging.info('MysqlPool_error_sql:%s' % failure)
#
#
# if __name__ == '__main__':
#     createTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     item = ["风景区", '20.73079336', '109.52835917', '20.91860932', '109.852887485', 1, createTime, 101]
#     # 20.73079336,109.52835917,20.91860932,109.852887485
#     mysqlpool = MySqlConnectPool()
#     mysqlpool.connect_process(item)

