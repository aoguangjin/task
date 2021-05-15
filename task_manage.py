#! /usr/bin/env python
#coding=utf-8
# 任务管理

import requests
import json
import time, os 
import datetime
from MSSql_SqlHelp import MSSQL 
from apscheduler.schedulers.background import BackgroundScheduler

# MS Sql Server 链接字符串
ms = MSSQL(host="172.16.12.35",user="sa",pwd="sa",db="SmallIsBeautiful_2017-03-15")

# 正在执行的 job 缓存
cache_job = []
# job 完整记录
cache_job_value = []
# 是否运行
cache_job_working = []

scheduler = BackgroundScheduler(timezone='MST')
scheduler.start()

def getJobList():
    sql = "select TaskCode,TaskType,TaskAddress,RequestMethod,RequestParameters,ExecutionMode,IntervalTime,CronMonth,CronDay,CronHour,CronMinute from T_Bas_TaskInfo where TaskIsUse='%s' " %('启动') 
    job_list = ms.ExecQuery(sql)

    global cache_job,cache_job_value,cache_job_working

    # 重置所有正在运行任务为否，便于后续标识还需运行的任务
    if len(cache_job_working) > 0:
        cache_job_working = list(map(lambda x:0,cache_job_working))

    # print(cache_job)
    for job in job_list:
        if job[0] in cache_job:
            # 旧任务
            job_index = cache_job.index(job[0])
            cache_job_working[job_index] = 1
            # 判断任务内容是否进行修改
            if cache_job_value[job_index] != job:
                # 修改了
                removeJob(job[0])
                createJob(job)
        else:
            # 新任务
            cache_job.append(job[0])
            cache_job_value.append(job)
            cache_job_working.append(1)
            createJob(job)

    # 倒序删除
    for i in range(len(cache_job_working)-1,-1,-1):
        if cache_job_working[i] == 0:
            cache_job.remove(cache_job[i])
            cache_job_value.remove(cache_job_value[i])
            cache_job_working.remove(cache_job_working[i])
    print('正在运行job：',cache_job)

def exeJob():
    pass

def httpJob(job,url,method,param):
    now = datetime.datetime.now()
    if method == 'get':
        r = requests.get(url)
        print(job + ',' + now.strftime('%Y-%m-%d %H:%M:%S'),' 返回数据：',r.text)
    else:
        r = requests.post(url, data=param)
        print(job + ',' + now.strftime('%Y-%m-%d %H:%M:%S'),' 返回数据：',r.text)

def createJob(job):
    global scheduler
    TaskCode = job[0]
    TaskType = job[1]
    TaskAddress = job[2]
    RequestMethod = job[3]
    RequestParameters = job[4]
    ExecutionMode = job[5]
    IntervalTime = job[6]
    CronMonth = '*' if job[7] == '' or job[7] == None else job[7]
    CronDay = '*' if job[8] == '' or job[8] == None else job[8]
    CronHour = '*' if job[9] == '' or job[9] == None else job[9]
    CronMinute = '*' if job[10] == '' or job[10] == None else job[10]
    if TaskType == 'http' and ExecutionMode == 'interval':
        scheduler.add_job(httpJob, ExecutionMode, id=TaskCode, seconds=IntervalTime,args=[job[0],TaskAddress, RequestMethod, RequestParameters])
    elif TaskType == 'http' and ExecutionMode == 'cron':
        scheduler.add_job(httpJob, ExecutionMode, id=TaskCode, month=CronMonth,day=CronDay,hour=CronHour,minute=CronMinute,args=[job[0],TaskAddress, RequestMethod, RequestParameters])
    else:
        pass
    

def removeJob(job_id):
    global scheduler
    scheduler.remove_job(job_id)


if __name__=='__main__':
    getJobList()

    while(True):
        getJobList()
        # 60s 获取下数据库，和正在运行的任务进行比对
        time.sleep(15)