#!/usr/local/python3.4/bin
#  -*- coding=utf8 -*-
#__author__ = 'rongrongl'
import json
import os
import  pymysql
import  datetime
now= datetime.datetime.now()
conn = pymysql.connect(host="172.19.50.77", user="wallet", password="admin_#1010", database="ods", port=3306,
                           charset="utf8")  # charset解决中文乱码问题
cur = conn.cursor()

colsql="select COLUMN_NAME from information_schema.`COLUMNS` where table_schema = 'ods' and table_name = 'bair_user_basic_info' ;"
cur.execute(colsql)
conn.commit()
column = cur.fetchall()
L=len(column)
List=[]
for i in  range(0,L):
    List.append(column[i][0])

sql='''SELECT uid,ruleid,loan_data,dw_last_update_time FROM wallet.w_uid_basic_data WHERE ruleid='1'
    and  LEFT(dw_last_update_time,10)=DATE_ADD(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL -1 DAY) ;'''
cur.execute(sql)
conn.commit()
result = cur.fetchall()
L = len(result)
for j in range(0, L):
    dict1 = dict({'uid': result[j][0], 'ruleid': result[j][1], 'dw_last_update_time': result[j][3]})
    s = json.loads(result[j][2])
    dictMerge=[]
    for key in s:
        dictMerge.append(key)
    ret_list = list((set(List).union(set(dictMerge)))^(set(List)^set(dictMerge)))
    length=len(ret_list)
    sql_parm='%s,%s,%s,'
    column_parm='ruleId,uid,dw_last_update_time,'
    value=str(dict1["ruleid"])+','+str(dict1["uid"])+','+repr(str(dict1["dw_last_update_time"]))+','
    value_parm=''
    print(s)
    for i in  range(0,length):
        column_parm+=ret_list[i]+','
        value_parm= ret_list[i]
        value+=repr(str(s[value_parm]))+','
    sql='insert into bair_user_basic_info('+ column_parm[:-1] +') '
    parm='('+ value[:-1] +')'
    print(sql)
    print(parm)
    execsql=sql+'values'+parm
    print(execsql)
    cur.execute(execsql)
    conn.commit()
conn.close()
cur.close()
now1= datetime.datetime.now()
print("waste time:",now1-now)
