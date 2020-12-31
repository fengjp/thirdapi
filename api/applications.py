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
from libs.oracle_conn import OracleBase
from settings import *
import datetime
from libs.aes_coder import encrypt, decrypt
from collections import Counter
import traceback
import re

TypeObj = {
    '未知': -1,
    '正常': 0,
    '一般': 1,
    '严重': 2,
    '致命': 3,
}


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
                try:
                    sql = '''
                        select max(id) from custom_query
                    '''
                    resp = mysql_conn.query(sql)
                    mid = resp[0][0]
                    new_id = int(mid) + 1
                    data['qid'] = new_id
                except:
                    data['qid'] = 1

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
                       create_time=data['create_time'], update_time=str(datetime.datetime.now()),
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
            return self.write(dict(code=-1, msg='failed', data=red_data, err='%s' % e))

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
            return self.write(dict(code=-1, msg='failed', data=red_data, err='%s' % e))

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


class DoSqlHandler(RequestHandler):
    def get(self):
        qid = self.get_argument('qid', default=None, strip=True)
        dict_list = []
        errormsg = ''
        try:
            # 配置信息
            query_info = {}
            mysql_conn = getConn('codo_task')
            sql = '''
                select id,title,dblinkId,`database`,`user`,`password`,`sql`,colnames,timesTy,timesTyVal,colalarms,
                `status`,create_time,update_time,description,seq,groupID from custom_query where id = %s
            ''' % qid
            resp = mysql_conn.query(sql)

            for id, title, dblinkId, database, user, password, sql, colnames, timesTy, timesTyVal, colalarms, status, \
                create_time, update_time, description, seq, groupID in resp:
                query_info['id'] = id
                query_info['title'] = title
                query_info['dblinkId'] = dblinkId
                query_info['database'] = database
                query_info['user'] = user
                query_info['password'] = password
                query_info['sql'] = sql
                query_info['colnames'] = colnames
                query_info['timesTy'] = timesTy
                query_info['timesTyVal'] = timesTyVal
                query_info['colalarms'] = colalarms
                query_info['status'] = status
                query_info['create_time'] = create_time
                query_info['update_time'] = update_time
                query_info['description'] = description
                query_info['seq'] = seq
                query_info['groupID'] = groupID

            dblinkId = query_info['dblinkId']

            # 获取数据库源 连接地址
            select_db = '''
                select db_type, db_host, db_port, db_user, db_pwd, db_instance 
                from codo_cmdb.asset_db where id = {}
            '''.format(dblinkId)
            db_info = mysql_conn.query(select_db)
        except:
            errormsg = '获取数据库源连接信息失败'
            return self.write(dict(code=-1, msg='获取失败', errormsg=errormsg, data=[]))

        if len(db_info) > 0:
            db = db_info[0]
            db_obj = {}
            db_obj['host'] = db[1]
            db_obj['port'] = int(db[2])
            db_obj['user'] = db[3]
            db_obj['passwd'] = decrypt(db[4])
            if query_info['database']:
                db_obj['db'] = query_info['database']
            else:
                db_obj['db'] = db[5]
            sql = query_info['sql']

            if query_info['user']:
                db_obj['user'] = query_info['user']

            if query_info['password']:
                db_obj['passwd'] = decrypt(query_info['password'])

            sql = re.sub('update|drop', '', sql, 0, re.I)
            # ins_log.read_log('info', db_obj)
            res = []
            try:
                if db[0] == 'mysql':
                    mysql_conn = MysqlBase(**db_obj)
                    res = mysql_conn.query(sql)

                if db[0] == 'oracle':
                    oracle_conn = OracleBase(**db_obj)
                    res = oracle_conn.query(sql)
            except Exception as e:
                errormsg = '%s 数据库: 查询失败, %s' % (db_obj['host'], e)
                return self.write(dict(code=-1, msg='获取失败', errormsg=errormsg, data=[]))

            if res:
                try:
                    colnames = json.loads(query_info['colnames'])
                    colalarms = json.loads(query_info['colalarms'])
                    # 增加状态列
                    if len(colalarms) > 0:
                        colnames.append({'col': "target", 'name': "指标"})
                    dict_key = []
                    for i in colnames:
                        dict_key.append(i['col'])

                    for i in res:
                        _d = dict(zip(dict_key, i))
                        for selColObj in colalarms:
                            # 判断指标值 (同少取最少，同大取最大)
                            selCol = selColObj['selCol']
                            if selCol in _d:
                                dbval = _d[selCol]
                                if not dbval:
                                    dbval = 0
                                subColList = selColObj['subColList']
                                subColList = sorted(subColList, key=lambda x: TypeObj[x['alarmType']], reverse=True)
                                # ins_log.read_log('info', subColList)
                                for alarmObj in subColList:
                                    sign = alarmObj['sign']
                                    alarmVal = alarmObj['alarmVal']
                                    if sign == '>' and float(dbval) > float(alarmVal):
                                        _d['target'] = alarmObj['alarmType']
                                    if sign == '<' and float(dbval) < float(alarmVal):
                                        _d['target'] = alarmObj['alarmType']
                                    if sign == '>=' and float(dbval) >= float(alarmVal):
                                        _d['target'] = alarmObj['alarmType']
                                        break
                                    if sign == '<=' and float(dbval) <= float(alarmVal):
                                        _d['target'] = alarmObj['alarmType']
                                    if sign == '=' and float(dbval) == float(alarmVal):
                                        _d['target'] = alarmObj['alarmType']

                                    if 'target' not in _d:
                                        _d['target'] = '未知'
                        # ins_log.read_log('info', _d)
                        dict_list.append(_d)

                    if len(colalarms) > 0:
                        dict_list.sort(key=lambda x: TypeObj[x['target']], reverse=True)
                        countObj = dict(Counter([i['target'] for i in dict_list]))
                    else:
                        countObj = {}

                except Exception as e:
                    traceback.print_exc()
                    dict_list = []
                    countObj = {}
                    errormsg = '字段格式错误'
                    return self.write(dict(code=-2, msg='获取失败', errormsg=errormsg, data=[]))

                # 转换 时间类型字段
                for _d in dict_list:
                    for k, v in _d.items():
                        if isinstance(v, datetime.datetime):
                            _d[k] = v.strftime("%Y-%m-%d %H:%M:%S")

                return self.write(dict(code=0, msg='获取成功', errormsg=errormsg, data=dict_list, count=countObj))

        return self.write(dict(code=-1, msg='获取失败', errormsg=errormsg, data=[], count={}))


class Application(myApp):
    def __init__(self, **settings):
        self.__settings = settings
        urls = []
        urls.extend(api_urls)
        super(Application, self).__init__(urls, **settings)


api_urls = [
    (r"/thirdapi/queryPushConf/", PushConfHandler),
    (r"/thirdapi/queryPullConf/", PullConfHandler),
    (r"/thirdapi/changeZdGroup/", GroupHandler),
    (r"/thirdapi/getInfo/", ConfInfoHandler),
    (r"/thirdapi/doSql/", DoSqlHandler),
    (r"/thirdapi/are_you_ok/", LivenessProbe),
]

if __name__ == '__main__':
    pass
