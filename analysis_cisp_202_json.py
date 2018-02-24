#utf-8
import json
import os
import  pymysql
import  datetime

conn = pymysql.connect(host="101.132.73.63", user="wallet", password="admin_#1010", database="ods", port=3306,
                       charset="utf8")  # charset解决中文乱码问题
cur = conn.cursor()
now= datetime.datetime.now()

def read_json():
    sql='''SELECT uid,ruleid,loan_data,dw_last_update_time FROM wallet.w_uid_basic_data WHERE ruleid='12' and LEFT(dw_last_update_time,10)=DATE_ADD(DATE_FORMAT(NOW(),'%Y-%m-%d'),INTERVAL -1 DAY);'''
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
        print(dict1)
        s = json.loads(result[j][2])
        i=len(list(s.keys()))
        # print(s)
        for j in range(0,i):
            sc=s[list(s.keys())[j]]
            # print(sc)
            if isinstance(sc,(str,int)):
                List[list(s.keys())[j]]=sc
            if not isinstance(sc,(str,int)):
                a=len(list(sc.keys()))
                firstr=list(s.keys())[j]
                # print(firstr)
                # print(list(sc.keys()))
                for j in range(0,a):
                    secondstr=sc[list(sc.keys())[j]]
                    # print(secondstr)
                    if  isinstance(secondstr,(str,int)):
                        List[firstr+'_'+list(sc.keys())[j]]=secondstr
                    if not isinstance(secondstr,(str,int)):
                        b=len(list(secondstr.keys()))
                        secstr=list(sc.keys())[j]
                        # print(secstr)
                        # print('b:',list(secondstr.keys()))
                        for j in range(0,b):
                            thirdstr=secondstr[list(secondstr.keys())[j]]
                            if isinstance(thirdstr,(str,int)):
                                List[secstr+'_'+list(secondstr.keys())[j]]=thirdstr
                            if not isinstance(thirdstr,(str,int,list)):
                                c=len(list(thirdstr.keys()))
                                # print(c)
                                thistr=list(secondstr.keys())[j]
                                # print(list(thirdstr.keys()))
                                for j in  range(0,c):
                                    fourstr=thirdstr[list(thirdstr.keys())[j]]
                                    # print(fourstr)
                                    if isinstance(fourstr,(str,int)):
                                        List[ thistr+'_'+list(thirdstr.keys())[j]]=fourstr
                                    if not isinstance(fourstr,(str,int,list)):
                                        d=len(list(fourstr.keys()))
                                        fostr=list(thirdstr.keys())[j]
                                        for j in range(0,d):
                                            fivestr=fourstr[list(fourstr.keys())[j]]
                                            if isinstance(fivestr,(str,int)):
                                                List[fostr + '_' + list(fourstr.keys())[j]] = fivestr

        # print(List)
        if "inclusive_finance_report_iscreditrecord" in List.keys():
            sql='''insert into cisp_user_basic_info(  ruleid,uid,
            success,
              detail_token ,
              inclu_fin_re_code,
              inclu_fin_re_iscreditrecord,
              report_request_time  ,
              report_report_id,
              personal_info_name ,
              personal_info_cardid ,
              personal_info_edu ,
              personal_info_sinocardid ,
              authentication_identity,
              authentication_photo ,
              authentication_education,
              authentication_shixin,
              authentication_panjue ,
              authentication_zhixing,
              normal_pay_unpaid ,
              normal_pay_paid ,
              unnormal_pay_paid,
              unnormal_pay_unpaid ,
              trans_count_last_2years_his ,
              last6Monthapply_his_unreviewed ,
              last6Monthapply_his_cancel,
              last6Monthapply_his_reviewed,
              last6Monthapply_his_refuse,dw_last_update_time ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''\
                %(repr(str(dict1['ruleid'])), repr(str(dict1['uid'])),repr(str(List['success'])),repr(str(List['detail_token'])),repr(str(List['inclusive_finance_report_code'])),repr(str(List['inclusive_finance_report_iscreditrecord'])),
                repr(str(List['report_request_time'])),repr(str(List['report_report_id'])),repr(str(List['personal_info_name'])),repr(str(List['personal_info_cardid'])),repr(str(List['personal_info_edu'])),
                  repr(str(List['personal_info_sinocardid'])),repr(str(List['authentication_identity'])),repr(str(List['authentication_photo'])),
                  repr(str(List['authentication_education'])),repr(str(List['authentication_shixin'])),
                  repr(str(List['authentication_panjue'])),repr(str(List['authentication_zhixing'])),
                  repr(str(List['normal_pay_unpaid'])),repr(str(List['normal_pay_paid'])),repr(str(List['unnormal_pay_paid'])),repr(str(List['unnormal_pay_unpaid'])),
                  repr(str(List['transactions_count_last_2years_history'])),repr(str(List['last_6Month_apply_history_unreviewed'])),repr(str(List['last_6Month_apply_history_cancel'])),
                  repr(str(List['last_6Month_apply_history_reviewed'])),repr(str(List['last_6Month_apply_history_refuse'])),repr(str(dict1['dw_last_update_time'])))

            # print(sql)
            cur.execute(sql)
            conn.commit()
        else:
            sql = '''insert into cisp_user_basic_info(  ruleid,uid,
                     success,
                       detail_token ,
                       inclu_fin_re_code,
                       report_request_time  ,
                       report_report_id,
                       personal_info_name ,
                       personal_info_cardid ,
                       personal_info_edu ,
                       personal_info_sinocardid ,
                       authentication_identity,
                       authentication_photo ,
                       authentication_education,
                       authentication_shixin,
                       authentication_panjue ,
                       authentication_zhixing,
                       normal_pay_unpaid ,
                       normal_pay_paid ,
                       unnormal_pay_paid,
                       unnormal_pay_unpaid ,
                       trans_count_last_2years_his ,
                       last6Monthapply_his_unreviewed ,
                       last6Monthapply_his_cancel,
                       last6Monthapply_his_reviewed,
                       last6Monthapply_his_refuse,dw_last_update_time ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' \
                  % (repr(str(dict1['ruleid'])), repr(str(dict1['uid'])), repr(str(List['success'])), repr(str(List['detail_token'])),
                     repr(str(List['inclusive_finance_report_code'])),
                     repr(str(List['report_request_time'])), repr(str(List['report_report_id'])), repr(str(List['personal_info_name'])),
                     repr(str(List['personal_info_cardid'])), repr(str(List['personal_info_edu'])),
                     repr(str(List['personal_info_sinocardid'])), repr(str(List['authentication_identity'])),
                     repr(str(List['authentication_photo'])), repr(str(List['authentication_education'])),
                     repr(str(List['authentication_shixin'])),
                     repr(str(List['authentication_panjue'])), repr(str(List['authentication_zhixing'])),
                     repr(str(List['normal_pay_unpaid'])), repr(str(List['normal_pay_paid'])), repr(str(List['unnormal_pay_paid'])),
                     repr(str(List['unnormal_pay_unpaid'])),
                     repr(str(List['transactions_count_last_2years_history'])),
                     repr(str(List['last_6Month_apply_history_unreviewed'])), repr(str(List['last_6Month_apply_history_cancel'])),
                     repr(str(List['last_6Month_apply_history_reviewed'])), repr(str(List['last_6Month_apply_history_refuse'])),
                     repr(str(dict1['dw_last_update_time'])))

            # print(sql)
            cur.execute(sql)
            conn.commit()

def array_json():
    result = read_json()
    L = len(result)
    for j in range(0, L):
        List = {}
        dict1 = dict({'uid': result[j][0], 'ruleId': result[j][1], 'dw_last_update_time': result[j][3]})
        s = json.loads(result[j][2])
        i=len(list(s.keys()))
        for j in range(0,i):
            sc=s[list(s.keys())[j]]
            if not isinstance(sc,(str,int)):
                a=len(list(sc.keys()))
                firstr=list(s.keys())[j]
                for j in range(0,a):
                    secondstr=sc[list(sc.keys())[j]]
                    if not isinstance(secondstr,(str,int)):
                        b=len(list(secondstr.keys()))
                        secstr=list(sc.keys())[j]
                        for j in range(0,b):
                            thirdstr=secondstr[list(secondstr.keys())[j]]
                            thistr = list(secondstr.keys())[j]
                            if isinstance(thirdstr, (list)):
                                if thistr=="apply_history":
                                    L=len(thirdstr)
                                    for j in range(0,L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])

                                        sql='''insert into cisp_report_apply_history (ruleId,uid,apply_time,member_type,apply_location,loan_type,apply_money,apply_result,comment,dw_last_update_time)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                        '''%(repr(dictMerged['ruleId']),repr(dictMerged['uid']),repr(dictMerged['apply_time']),repr(dictMerged['member_type']),repr(dictMerged['apply_location']),
                                             repr(dictMerged['loan_type']),repr(dictMerged['apply_money']),repr(dictMerged['apply_result']),repr(dictMerged['comment']),repr(str(dictMerged['dw_last_update_time'])))
                                        print(sql)
                                        cur.execute(sql)
                                        conn.commit()

                                if thistr=="last_2years_history":
                                    L = len(thirdstr)
                                    for j in range(0, L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])
                                        sql = '''insert into cisp_report_last_2years_history (ruleId,uid,query_time,member_type,query_type,comment,dw_last_update_time)
                                                                   values(%s,%s,%s,%s,%s,%s,%s)
                                                                   ''' % (
                                            repr(dictMerged['ruleId']), repr(dictMerged['uid']), repr(dictMerged['query_time']),
                                            repr(dictMerged['member_type']), repr(dictMerged['query_type']),
                                            repr(dictMerged['comment']), repr(str(dictMerged['dw_last_update_time'])))
                                        print(sql)
                                        cur.execute(sql)
                                        conn.commit()

                                if thistr == "discredit":
                                    L = len(thirdstr)
                                    for j in range(0, L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])
                                        sql = '''insert into cisp_report_discredit
                                        (ruleId,uid,public_time,overdue_start_time,loan_location,
                                        overdue_money,overdue_duration,phone,email,
                                        personal_address,live_address,reason,comment,data_source,dw_last_update_time)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                                                                     ''' % (
                                            repr(dictMerged['ruleId']), repr(dictMerged['uid']), repr(dictMerged['public_time']),repr(dictMerged['overdue_start_time']),repr( dictMerged['loan_location']),
                                            repr(dictMerged['overdue_money']), repr(dictMerged['overdue_duration']),repr(dictMerged['phone']),repr(dictMerged['email']),
                                            repr(dictMerged['personal_address']),repr(dictMerged['live_address']),repr(dictMerged['reason']),repr(dictMerged['comment']),
                                            repr(dictMerged['data_source']),repr(str(dictMerged['dw_last_update_time'])))
                                        print(sql)
                                        cur.execute(sql)
                                        conn.commit()

                                if thistr == "common_info":
                                    L = len(thirdstr)
                                    for j in range(0, L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])

                                        sql = '''insert into cisp_report_common_info
                                                              (ruleId,uid,content,data_from,dw_last_update_time)
                                                              values(%s,%s,%s,%s,%s)
                                                                                                           ''' % (
                                            repr(dictMerged['ruleId']), repr(dictMerged['uid']), repr(dictMerged['content']),
                                             repr(dictMerged['data_from']),
                                            repr(str(dictMerged['dw_last_update_time'])))
                                        print(sql)
                                        cur.execute(sql)
                                        conn.commit()

                                if thistr == "dissent":
                                    L = len(thirdstr)
                                    for j in range(0, L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])

                                        sql = '''insert into cisp_report_dissent
                                                                                 (ruleId,uid,dissent_id,appeal_time,appeal_detail,status,dw_last_update_time)
                                                                                 values(%s,%s,%s,%s,%s,%s,%s)
                                                                                                                              ''' % (
                                            dictMerged['ruleId'], dictMerged['uid'], repr(dictMerged['dissent_id']),
                                            repr(dictMerged['appeal_time']),repr(dictMerged['appeal_detail']),repr(dictMerged['status']),
                                            repr(str(dictMerged['dw_last_update_time'])))
                                        print(sql)
                                        cur.execute(sql)
                                        conn.commit()

                                if thistr == "transactions":
                                    L = len(thirdstr)
                                    for j in range(0, L):
                                        dictMerged = dict1.copy()
                                        dictMerged.update(thirdstr[j])
                                        z=len(dictMerged["overdue_detail"])
                                        if z==0:

                                            sql = '''insert into cisp_report_transactions
                                                (ruleId,uid,loan_money,loan_start_time,loan_end_time,loan_type,loan_guarantee,pay_period,loan_location,loan_status,comment,paid_history,dw_last_update_time)
                                                  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                                                                                                                                    ''' % (
                                                dictMerged['ruleId'], dictMerged['uid'], repr(dictMerged['loan_money']),
                                                repr(dictMerged['loan_start_time']), repr(dictMerged['loan_end_time']),
                                                repr(dictMerged['loan_type']),repr(dictMerged['loan_guarantee']),repr(dictMerged['pay_period']),repr(dictMerged['loan_location']),repr(dictMerged['loan_status']),repr(dictMerged['comment']),repr(dictMerged['paid_history']),
                                                repr(str(dictMerged['dw_last_update_time'])))
                                            print(sql)
                                            cur.execute(sql)
                                            conn.commit()

                                        if z!=0:
                                            due=dictMerged["overdue_detail"]
                                            L=len(due)
                                            for j in range(0,L):
                                                dictMerged1 = dictMerged.copy()
                                                dictMerged1.update(due[j])
                                                sql = '''insert into cisp_report_transactions
                                                                                        (ruleId,uid,loan_money,loan_start_time,loan_end_time,loan_type,loan_guarantee,pay_period,loan_location,loan_status,comment,paid_history,done_time,
                                                                                        overdue_time,overdue_level,overdue_reason,now_status,dw_last_update_time)
                                                                                          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                                                                                                                                                                            ''' % (
                                                    dictMerged1['ruleId'], dictMerged1['uid'], repr(dictMerged1['loan_money']),
                                                    repr(dictMerged1['loan_start_time']), repr(dictMerged1['loan_end_time']),
                                                    repr(dictMerged1['loan_type']), repr(dictMerged1['loan_guarantee']),
                                                    repr(dictMerged1['pay_period']), repr(dictMerged1['loan_location']),
                                                    repr(dictMerged1['loan_status']), repr(dictMerged1['comment']),
                                                    repr(dictMerged1['paid_history']),repr(dictMerged1["done_time"]),repr(dictMerged1["overdue_time"]),repr(dictMerged1["overdue_level"]),repr(dictMerged1["overdue_reason"]),repr(dictMerged1["now_status"]),
                                                    repr(str(dictMerged1['dw_last_update_time'])))
                                                print(sql)
                                                cur.execute(sql)
                                                conn.commit()
if __name__ == '__main__':
    analysis_json()
    array_json()

conn.close()
cur.close()
now1= datetime.datetime.now()
print("waste time:",now1-now)
