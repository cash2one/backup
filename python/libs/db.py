#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import MySQLdb
from MySQLdb.cursors import DictCursor
import redis
import base_conf
import tools
from DBUtils.PooledDB import PooledDB
import time

module_logger = tools.logger("./logs/sql.query")

class DbConfReader(object):
    '''
        name : table_name
        colums : 必须是完整的列名
        key : 索引项
        auto_key : 自增key
    '''
    def __init__(self, conf):
        self.conf = base_conf.base_conf_t([conf])
        self.gen_sql()

    def gen_sql(self):
        self.table_name = self.conf.name
        self.colums = [x.strip() for x in self.conf.colums.split(",")]
        try:
            self.auto_key = self.conf.auto_increment
        except:
            self.auto_key = None
        self.insert_colums = [col for col in self.colums if col != self.auto_key]
        try:
            self.key = {x.strip() : True for x in self.conf.key.split(",")}
        except:
            self.key = {}
        try:
            self.unique_key = {x.strip() : True for x in self.conf.unique_key.split(",")}
        except:
            self.unique_key = {}

        self.create_sql = self.gen_create_sql()
        self.update_sql = self.gen_update_sql()
        self.insert_sql = self.gen_insert_sql()

    def gen_update_sql(self, condition = "day", key = "is_del", value = 1):
        update_sql = '''UPDATE `%s` SET `%s` = "%s" WHERE `%s` = "%s"'''\
                     % (self.table_name, key, value, condition, "%s")
        self.update_sql = update_sql
        return update_sql

    def gen_insert_sql(self):
        ret = "INSERT INTO `%s` (" % self.table_name
        ret += ",".join(["`" + col + "`" for col in self.insert_colums])
        ret += ") VALUES ("
        format = ['''"%s"'''] * len(self.insert_colums)
        ret += ",".join(format)
        ret += ") "
        return ret

    def gen_create_sql(self):
        ret = "CREATE TABLE IF NOT EXISTS `%s` (" % self.table_name
        ret += ",".join(["`" + col + "` " + self.conf[col] for col in self.colums])
        for key in self.key:
            if key.find(":") == -1:
                ret += ", KEY `%s` (`%s`)" % (key, key)
            else:
                name, value = key.split(":")
                ret += ", KEY `%s` (" % name
                for v in value.split("#"):
                    ret += "`%s`," % v
                ret = ret.rstrip(",")
                ret += ")"
        for key in self.unique_key:
            if key.find(":") == -1:
                ret += ", UNIQUE KEY `%s` (`%s`)" % (key, key)
            else:
                name, value = key.split(":")
                ret += ", UNIQUE KEY `%s` (" % name
                for v in value.split("#"):
                    ret += "`%s`," % v
                ret = ret.rstrip(",")
                ret += ")"
        ret += ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='%s'" % self.table_name
        return ret

    def create(self):
        return self.create_sql

    def insert(self, values):
        sql = self.insert_sql % tuple(tools.convert_values(values))
        return sql

    def update(self, date):
        return self.update_sql % date

class DbBase(object):

    def __init__(self,
                 host = "",
                 user = "",
                 passwd = "",
                 db = "",
                 port = 3306,
                 charset = "utf8",
                 logger = None,
                 dict_cursor = False
                 ):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.conn = None
        self.cursor = None
        self.logger = logger
        self.dict_cursor = dict_cursor
        self.charset = charset
        self.table_create = False

    def connect(self):
        try:
            self.conn = MySQLdb.connect(
                                host = self.host,
                                user = self.user,
                                passwd = self.passwd,
                                db = self.db,
                                port = self.port,
                                charset = self.charset
                                )
            if self.dict_cursor:
                self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            else:
                self.cursor = self.conn.cursor()

        except Exception as info:
            print "db->connect", info
            sys.exit(-1)

    def close(self):
        self.conn.close()

    def reconnect(self):
        try:
            self.conn.ping()
        except:
            self.connect()

    def execute(self, sql, print_sql = True, commit = False):
        if print_sql:
            if self.logger is not None:
                self.logger.info("sql = [%s]" % sql)
            else:
                module_logger.info("sql = [%s]" % sql)
        try_times = 0
        while True:
            try:
                try_times += 1
                self.reconnect()
                self.info = self.cursor.execute(sql)
                break
            except Exception as info:
                if try_times >= 5:
                    break
                time.sleep(2)
                continue

        if commit:
            self.commit()

    def info(self):
        return self.info

    def commit(self):
        self.conn.commit()

    def next(self):
        return self.cursor.fetchone()

    def page(self, page, offset):
        pass

    def next_all(self):
        return self.cursor.fetchall()

class MysqlPool(object):
    __pool = None
    def __init__(self,
                 host = "",
                 user = "",
                 passwd = "",
                 db = "",
                 port = 3306,
                 charset = "utf8",
                 ):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.conn = None
        self.cursor = None
        self.charset = charset

    def connect(self):
        try:
            if self.__pool is None:
                self.__pool = PooledDB(creator = MySQLdb,
                                       host = self.host,
                                       user = self.user,
                                       passwd = self.passwd,
                                       db = self.db,
                                       port = self.port,
                                       charset = self.charset,
                                       mincached = 1,
                                       maxcached = 20,
                                       use_unicode = True,
                                       cursorclass = DictCursor
                                       )
            self.conn = self.__pool.connection()
            self.cursor = self.conn.cursor()

        except Exception as info:
            print info
            sys.exit(-1)

    def execute(self, sql, print_sql = True, commit = False):
        self.connect()
        if print_sql:
            print sql
        self.info = self.cursor.execute(sql)

        if commit:
            self.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def next(self):
        return self.cursor.fetchone()

    def next_all(self):
        return self.cursor.fetchall()

class Redis(object):

    def __init__(self, host, passwd, port, db):
        self.pool = redis.ConnectionPool(host = host,
                                         password = passwd,
                                         port = port,
                                         db = db)
        self.r = redis.Redis(connection_pool = self.pool)

    def hget(self, key, field):
        return self.r.hget(key, field)

    def hset(self, key, field, value):
        self.r.hset(key, field, value)

    def hgetall(self, key):
        return self.r.hgetall(key)

    def hkeys(self, table):
        return self.r.hkeys(table)

    def hdel(self, key, field):
        self.r.hdel(key, field)

    def keys(self, key):
        return self.r.keys(key)

    def delete(self, key):
        self.r.delete(key)

    def close(self):
        self.r.close()

    def client(self):
        return self.r

def get_db_ins(connect_conf, dict_cursor = False, logger = None):
    db_instance = DbBase(host = connect_conf.db_host,
                            user = connect_conf.db_user,
                            passwd = connect_conf.db_pass,
                            port = int(connect_conf.db_port),
                            db = connect_conf.db_name,
                            dict_cursor = dict_cursor,
                            logger = logger
                            )
    db_instance.connect()
    return db_instance

def get_db_pool(connect_conf):
    db_instance = MysqlPool(host = connect_conf.db_host,
                            user = connect_conf.db_user,
                            passwd = connect_conf.db_pass,
                            port = int(connect_conf.db_port),
                            db = connect_conf.db_name,
                            )
    return db_instance

def get_redis_client(conf):
    return Redis(
                host = conf.redis_host,
                passwd = conf.redis_passwd,
                port = conf.redis_port,
                db = conf.redis_db
                )

if __name__ == "__main__":
    a = "%s%saaa" % ("%s", "%s")
    print a
