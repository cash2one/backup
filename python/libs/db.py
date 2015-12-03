#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import MySQLdb
import redis
import base_conf
import tools

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
        except Exception as info:
            self.key = {}
            
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
            ret += ", KEY `%s` (`%s`)" % (key, key)
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
                 logger = None,
                 ):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.conn = None
        self.cursor = None
        self.logger = logger
        self.table_create = False
    
    def connect(self):
        try:
            self.conn = MySQLdb.connect(
                                host = self.host,
                                user = self.user,
                                passwd = self.passwd,
                                db = self.db,
                                port = self.port,
                                charset = "utf8"
                                )
            self.cursor = self.conn.cursor()
            
        except Exception as info:
            print info
            sys.exit(-1)
            
    def close(self):
        self.conn.close()
    
    def execute(self, sql, print_sql = True, commit = False):
        if print_sql:
            if self.logger is not None:
                self.logger("sql = [%s]" % sql)
            else:
                print sql
        self.info = self.cursor.execute(sql)
        
        if commit:
            self.commit()
        
    def info(self):
        return self.info
    
    def commit(self):
        self.conn.commit()
    
    def next(self):
        return self.cursor.fetchone()
    
    def next_all(self):
        return self.cursor.fetchall()

class Redis(object):
    
    def __init__(self, host, passwd, port, db):
        self.r = redis.Redis(host = host,
                             password = passwd,
                             port = port,
                             db = db)
    
    def hget(self, key, field):
        return self.r.hget(key, field)
    
    def hgetall(self, key):
        return self.r.hgetall(key)

def get_db_ins(connect_conf):
    db_instance = DbBase(host = connect_conf.db_host,
                            user = connect_conf.db_user,
                            passwd = connect_conf.db_pass,
                            port = int(connect_conf.db_port),
                            db = connect_conf.db_name,
                            )
    db_instance.connect()
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