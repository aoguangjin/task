# -*- coding:utf-8 -*-
# pip install pymssql
# import pymssql
# pip install pyodbc
import pyodbc
#下载 Microsoft® ODBC Driver 13.1 for SQL Server®


class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        try:
            self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.host+';DATABASE='+self.db+';UID='+self.user+';PWD='+self.pwd)
            # print("使用pymssql连接数据库")
        except Exception as e:
            self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.host+';DATABASE='+self.db+';UID='+self.user+';PWD='+self.pwd)
            # print("使用pyodbc连接数据库")
        
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

# ms = MSSQL(host="172.16.12.35",user="sa",pwd="sa",db="SmallIsBeautiful_2017-03-15")
# ms = MSSQL(host=".",user="sa",pwd="sa",db="SmallIsBeautiful")

# reslist = ms.ExecQuery("select * from T_PC_User")
# for i in reslist:
#     print(i)

# newsql="update Space0002A set column_0='%s' where id='%s'" %(u'2012年测试',u'2')
# print(newsql)
# ms.ExecNonQuery(newsql.encode('utf-8'))