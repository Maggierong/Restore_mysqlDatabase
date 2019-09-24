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
def read_json():

    sql='''SELECT uid,ruleid,loan_data,dw_last_update_time FROM wallet.w_uid_basic_data WHERE ruleid='9'
    and  LEFT(dw_last_update_time,10)=DATE_ADD(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL -1 DAY) ;'''
    cur.execute(sql)
    conn.commit()
    result = cur.fetchall()
    return result
def analysis_json():
    result = read_json()
    L = len(result)
    for j in range(0, L):
        List = {}
        dict1 = dict({'uid': result[j][0], 'ruleid': result[j][1], 'dw_last_update_time': result[j][3]})
        s = json.loads(result[j][2])
        i = len(list(s.keys()))
        for j in range(0, i):
            sc = s[list(s.keys())[j]]
            if isinstance(sc, (str, int)):
                List[list(s.keys())[j]] = sc
        return List
def array_json():
    result = read_json()
    List=analysis_json()
    L = len(result)
    for j in range(0, L):
        dict1 = dict({'uid': result[j][0], 'ruleid': result[j][1], 'dw_last_update_time': result[j][3]})
        s = json.loads(result[j][2])
        i = len(list(s.keys()))
        for j in range(0, i):
            sc = s[list(s.keys())[j]]
            if list(s.keys())[j] == 'loanInfos':
                a = len(sc)
                for j in range(0, a):
                    dictMerge = sc[j]
                    sql = '''insert into 91_credit_investigation( ruleId,uid,trxNo,borrowType,borrowState,borrowAmount,
                                      contractDate,loanPeriod,repayState,arrearsAmount,companyCode,dw_last_update_time ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' \
                                      % (repr(str(dict1['ruleid'])), repr(str(dict1['uid'])), repr(str(List['trxNo'])),
                                          repr(str(dictMerge['borrowType'])),
                                         repr(str(dictMerge['borrowState'])), repr(str(dictMerge['borrowAmount'])),
                                         repr(str(dictMerge['contractDate'])),
                                         repr(str(dictMerge['loanPeriod'])),
                                         repr(str(dictMerge['repayState'])),
                                         repr(str(dictMerge['arrearsAmount'])), repr(str(dictMerge['companyCode'])),
                                         repr(str(dict1['dw_last_update_time']))
                                         )

                    print(sql)
                    cur.execute(sql)
                    conn.commit()

if __name__ == '__main__':
    array_json()
conn.close()
cur.close()
now1= datetime.datetime.now()
print("waste time:",now1-now)
