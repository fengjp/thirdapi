#!/usr/bin/env python
# -*-coding:utf-8-*-

import time, datetime
import json
from dateutil.relativedelta import relativedelta
from websdk.application import Application as myApp
from websdk.web_logs import ins_log
from websdk.base_handler import LivenessProbe
from apscheduler.schedulers.tornado import TornadoScheduler
from libs.base_handler import BaseHandler
from tornado.web import RequestHandler
from tornado.options import options
from libs.mysql_conn import MysqlBase
from settings import *
from datetime import datetime


def getConn(db='codo_task'):
    db_conf = {
        'host': DEFAULT_DB_DBHOST,
        'port': DEFAULT_DB_DBPORT,
        'user': DEFAULT_DB_DBUSER,
        'passwd': DEFAULT_DB_DBPWD,
        'db': db
    }
    mysql_conn = MysqlBase(**db_conf)
    return mysql_conn


class PushConfHandler(RequestHandler):
    def post(self):
        red_data = {}
        try:
            data = json.loads(self.request.body.decode("utf-8"))
            mysql_conn = getConn()
            if not data['qid']:
                sql = '''
                    select max(id) from custom_query
                '''
                resp = mysql_conn.query(sql)
                mid = resp[0][0]
                new_id = int(mid) + 1
                data['qid'] = new_id

            colnames = json.dumps(data['colnames'])
            colalarms = json.dumps(data['colalarms'])
            sql = '''
                replace into `custom_query`(`id`, `title`, `dblinkId`, `database`, `user`, 
                `password`, `sql`, `colnames`, `timesTy`, `timesTyVal`, `colalarms`, `status`, 
                `create_time`, `update_time`, `description`, `seq`, `groupID`) 
                values ({id},'{title}',{dblinkId},'{database}','{user}','{password}',"{sql}",
                '{colnames}','{timesTy}','{timesTyVal}','{colalarms}','{status}','{create_time}','{update_time}',
                '{description}',{seq},'{groupID}')
            '''.format(id=data['qid'], title=data['title'], dblinkId=data['dblinkId'], database=data['database'],
                       user=data['user'], password=data['password'], sql=data['sql'],
                       colnames=colnames.replace("\\", "\\\\"), timesTy=data['timesTy'],
                       timesTyVal=data['timesTyVal'], colalarms=colalarms.replace("\\", "\\\\"), status=data['status'],
                       create_time=data['create_time'], update_time=str(datetime.now()),
                       description=data['description'], seq=data['seq'], groupID=data['groupID'])
            res = mysql_conn.change(sql)
            if res > 0:
                # 返回qid，并更新qid
                red_data[data['id']] = data['qid']

            # 更新组排序
            try:
                groupSeq = data['group2ndSeq']
                groupName = data['groupName']
                sql = '''
                    UPDATE custom_group set groupSeq = {} where groupName = '{}'
                '''.format(groupSeq, groupName)
                mysql_conn.change(sql)

            except Exception as e:
                ins_log.read_log('error', e)

        except Exception as e:
            ins_log.read_log('error', e)
            return self.write(dict(code=-1, msg='failed', data=red_data))

        return self.write(dict(code=0, msg='success', data=red_data))


class PullConfHandler(RequestHandler):
    def get(self):
        red_data = []
        try:
            mysql_conn = getConn()
            sql = '''
                select * from custom_query
            '''
            resp = mysql_conn.query(sql)
            for id, title, dblinkId, database, user, password, sql, colnames, timesTy, timesTyVal, colalarms, status, \
                create_time, update_time, description, seq, groupID in resp:
                _d = {}
                _d['qid'] = id
                _d['title'] = title
                _d['dblinkId'] = dblinkId
                _d['database'] = database
                _d['user'] = user
                _d['password'] = password
                _d['sql'] = sql
                _d['colnames'] = colnames
                _d['timesTy'] = timesTy
                _d['timesTyVal'] = timesTyVal
                _d['colalarms'] = colalarms
                _d['status'] = status
                _d['create_time'] = str(create_time)
                _d['update_time'] = str(update_time)
                _d['description'] = description
                _d['seq'] = seq
                _d['groupID'] = groupID
                groupIds = json.loads(groupID)
                groupInfo = self.getGroupInfo(groupIds[-1])
                _d['groupName'] = groupInfo[0]
                _d['group2ndSeq'] = groupInfo[-1]
                red_data.append(_d)

        except Exception as e:
            ins_log.read_log('error', e)
            return self.write(dict(code=-1, msg='failed', data=red_data))

        return self.write(dict(code=0, msg='success', data=red_data))

    def getGroupInfo(self, gid):
        groupName = ''
        group2ndSeq = 0
        try:
            mysql_conn = getConn()
            sql = '''
                select groupName,groupSeq from custom_group where id = %s
            ''' % gid
            resp = mysql_conn.query(sql)
            for name, seq in resp:
                groupName = name
                group2ndSeq = seq
        except Exception as e:
            ins_log.read_log('error', e)
        return [groupName, group2ndSeq]


class ConfInfoHandler(RequestHandler):
    def get(self):
        mysql_conn = getConn()
        groupObj = []
        try:
            sql = '''
                select id,groupName from custom_group where grouptype = 2
            '''
            resp = mysql_conn.query(sql)
            for id, name in resp:
                groupObj.append({'id': id, 'name': name})
        except Exception as e:
            groupObj = []
            ins_log.read_log('error', e)

        db_list = []
        try:
            sql = '''
                select id,db_code from codo_cmdb.asset_db
            '''
            resp = mysql_conn.query(sql)
            for id, name in resp:
                db_list.append({'id': id, 'name': name})
        except Exception as e:
            db_list = []
            ins_log.read_log('error', e)

        return self.write(dict(code=0, groupObj=groupObj, db_list=db_list))


class GroupHandler(RequestHandler):
    def post(self):
        try:
            data = json.loads(self.request.body.decode("utf-8"))
            mysql_conn = getConn()
            sql = '''
                INSERT INTO custom_group (`groupName`, `grouptype`, `groupSeq`) 
                VALUES ('{}', {}, {})
            '''.format(data['groupName'], data['grouptype'], 0)
            resnum = mysql_conn.change(sql)
            if resnum > 0:
                return self.write(dict(code=0, msg='success'))
            return self.write(dict(code=-1, msg='failed'))
        except:
            return self.write(dict(code=-1, msg='failed'))

    def delete(self):
        try:
            data = json.loads(self.request.body.decode("utf-8"))
            mysql_conn = getConn()
            sql = '''
                delete from custom_group where id = {}
            '''.format(data['id'])
            resnum = mysql_conn.change(sql)
            if resnum > 0:
                return self.write(dict(code=0, msg='success'))
            return self.write(dict(code=-1, msg='failed'))
        except:
            return self.write(dict(code=-1, msg='failed'))


class Application(myApp):
    def __init__(self, **settings):
        self.__settings = settings
        urls = []
        urls.extend(api_urls)
        super(Application, self).__init__(urls, **settings)


api_urls = [
    (r"/queryPushConf/", PushConfHandler),
    (r"/queryPullConf/", PullConfHandler),
    (r"/changeZdGroup/", GroupHandler),
    (r"/getInfo/", ConfInfoHandler),
    (r"/are_you_ok/", LivenessProbe),
]

if __name__ == '__main__':
    pass
