import pymysql
import logging
# DB_CONF = {"host": "127.0.0.1",
#            "db": "baidumappoi",
#            "user": "root",
#            "passwd": "",
#            "port": 3306,
#            "cursorclass": pymysql.cursors.DictCursor,
#            "charset": "utf8",
#            "use_unicode": False,
#            }


DB_CONF = {"host": "192.168.0.139",
           "db": "lmm_vst_destination",
           "user": "root",
           "passwd": "123456",
           "port": 3306,
           "cursorclass": pymysql.cursors.DictCursor,
           "charset": "utf8",
           "use_unicode": False,
           }

# 日志的配置
logging.basicConfig(level=logging.INFO,
                    filename='../proxy.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d]-%(levelname)s: %(message)s')

# redis数据库IP
Redis_Host = "192.168.13.128"
# redis数据据密码
Redis_Pwd = "myredis"
# redis端口
Redis_Port = 6379
# redis几号数据库
mainLand = 15
# 港澳台
HMT = 14

