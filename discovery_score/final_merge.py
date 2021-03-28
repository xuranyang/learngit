#!/usr/bin/env python
# -*- coding:utf-8 -*-

from sklearn import neighbors
import pandas as pd
import pymssql
import sys

reload(sys)
sys.setdefaultencoding("utf-8")


def mssql_script(pwd):
    # pwd 为sql日志的文件路径（要去掉注释，每段SQL只用一个分号";"隔开）
    with open(pwd) as fp:
        # python2中文编码问题 具体看保存个文件编码格式
        sql = fp.read().decode('GBK').encode('utf-8').split(';')[:-1]

    conn = pymssql.connect(host='127.0.0.1', port=1434, user='sa', password='root00', database='AiDb',
                           charset='utf8')

    cursor = conn.cursor()
    for i in sql:
        cursor.execute(i)
        conn.commit()

    data = []
    sql = "SELECT t1.*,t2.moment_tag,t3.isneed FROM AiDb.dbo.moment_score_0613 as t1 " \
          "INNER JOIN AiDb.dbo.moment_tag_all_0613 as t2 ON t1.userid=t2.userid " \
          "INNER JOIN AiDb.dbo.moment_behavior_tag_0613 as t3 ON t2.moment_tag=t3.moment_tag"
    cursor.execute(sql)
    rows = cursor.fetchall()
    for i in rows:
        data.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]])

    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=['userid', 'discovery_score', 'online_score', 'comment_score', 'praise_score',
                                     'com_pra_score', 'moment_score', 'moment_score_rub', 'moment_tag', 'isneed'])

    return df


# def knn_classifier(pwd):
#     # pwd = r"C:\Users\36243\Desktop\file\moment_tag_0606_1.xlsx"
#     base = pd.read_excel(pwd)
#
#     fe = base[base['isneed'] == 1]
#     ne = base[base['isneed'] == 0].drop(['moment_tag'], axis=1)
#     # print('分表完成')
#
#     # train
#     knn = neighbors.KNeighborsClassifier()
#     knn.fit(fe.drop(['moment_tag', 'isneed', 'userid'], axis=1), fe['moment_tag'])
#     # print('训练完成')
#
#     ne['moment_tag'] = knn.predict(ne.drop(['isneed', 'userid'], axis=1))
#     # print('分类完成')
#     # print(ne.head())
#
#     tol = pd.concat([fe, ne], axis=0, sort=True)
#
#     return tol


def knn_classifier(base):
    fe = base[base['isneed'] == 1]
    ne = base[base['isneed'] == 0].drop(['moment_tag'], axis=1)
    # print('分表完成')

    # train
    knn = neighbors.KNeighborsClassifier()
    knn.fit(fe.drop(['moment_tag', 'isneed', 'userid'], axis=1), fe['moment_tag'])
    # print('训练完成')

    ne['moment_tag'] = knn.predict(ne.drop(['isneed', 'userid'], axis=1))
    # print('分类完成')
    # print(ne.head())

    tol = pd.concat([fe, ne], axis=0, sort=True)

    return tol


def insert_to_mssql(df):
    conn = pymssql.connect(host='127.0.0.1', port=1434, user='sa', password='root00', database='AiDb',
                           charset='utf8')

    cursor = conn.cursor()

    # df = pd.read_excel(r"C:\Users\36243\Desktop\222.xlsx")
    sql_insert = "insert into aidb.dbo.all_moment_tag (userid,discovery_score,online_score,comment_score," \
                 "praise_score,com_pra_score,moment_score,moment_score_rub,moment_tag,isneed) values"

    # print sql_insert

    sql_values = []

    for i in df.values:
        com_pra_score = i[0]
        comment_score = i[1]
        discovery_score = i[2]
        isneed = i[3]
        moment_score = i[4]
        moment_score_rub = i[5]
        moment_tag = '\'%s\'' % i[6]
        online_score = i[7]
        praise_score = i[8]
        userid = i[9]

        sql = '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),\n' % \
              (userid, discovery_score, online_score, comment_score, praise_score, com_pra_score, moment_score,
               moment_score_rub, moment_tag, isneed)
        sql_values.append(sql)

    num = 0
    data = ''
    for i in range(len(sql_values)):
        num += 1
        data += str(sql_values[i])

        if i == len(sql_values) - 1:
            # 去掉最后一个多余逗号
            data = data[:-2]
            sql = sql_insert + data
            # print sql
            cursor.execute(sql)
            conn.commit()
            break

        if num == 800:
            # 去掉最后一个多余逗号
            data = data[:-2]
            sql = sql_insert + data
            # print sql
            cursor.execute(sql)
            conn.commit()
            num = 0
            data = ''

    # conn.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    # tol = knn_classifier(r"C:\Users\36243\Desktop\file\moment_tag_0606_1.xlsx")
    # insert_to_mssql(tol)
    tag = mssql_script(r"C:\Users\36243\Desktop\test.txt")
    clf = knn_classifier(tag)
    insert_to_mssql(clf)
