# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 12:36:33 2018

@author: 11940
"""

import requests
#import os
from bs4 import BeautifulSoup
#from docx import Document
import time
import random
import pymssql
import csv


def Get_html(Url):
    headers = {
            'USer-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
            }
    req = requests.get(Url,headers = headers )
    print("状态码：%d"%(req.status_code))
    if req.status_code==200:
        print("状态正常")
        return req.text
    else:
        print("状态异常")
        return None
def Parse(html):
    soup=BeautifulSoup(html,'lxml')
    all_list=[]
    count=0
    money_list=[]
    company_list=[]
    job_list=[]
    worday_list=[]
    money=soup.select('span.job-info-money')
    company=soup.select('div.job-info-company')
    job=soup.select('span.job-info-name')
    worday=soup.select('span.job-info-days')
    for m in money:
        money_list.append(m.get_text())
    for c in company:
        company_list.append(c.get_text())
    for j in job:
        count=count+1
        job_list.append(j.get_text())
    for w in worday:
        worday_list.append(w.get_text())
    for i in range(count):
        big_list=[]
        big_list.append(job_list[i])
        big_list.append(company_list[i])
        big_list.append(worday_list[i])
        big_list.append(money_list[i])
        all_list.extend([tuple(big_list)])
    if all_list:
        print("处理成功！")
        return all_list
def main():
    URL="http://www.internbird.com/j/search?lt=&k=&jt=&pt=&cid=1&wk=&et=&page="
    #all_info=[]
    all_get =[]
    begain = time.time()
    for i in range(1,125):
        all_info = []
        url = URL+str(i)
        html = Get_html(url)
        if html:
            print("正在爬取第%d页"%(i))
            all_info=Parse(html)
            all_get.extend(all_info)   
        time.sleep(random.randint(5,12))
    with open('Intern.csv','w',newline='') as f:
        head=["职位","公司","工作天数","薪资"]
        cw = csv.writer(f)
        cw.writerow(head)
        cw.writerows(all_get)
    server = 'localhost'
    user = 'sa'
    password = '130567'
    database = 'SudentDB'
    conn = pymssql.connect(server,user,password,database)
    cur = conn.cursor()
    temp = 0
    for i in all_get:
        #print("此时temp:%d   写入第%d行数据"%(temp,temp+1))
        sql = "insert into Intern(job,company,workday,money) values ('%s','%s','%s','%s')"%(all_get[temp])
        cur.execute(sql)
        temp = temp+1
    cur.execute("select job from Intern")
    job_num = cur.fetchall()
    if len(job_num)==temp:
        print("恭喜，所有数据处理并入库完毕!")
    conn.commit()
    conn.close()
    end=time.time()
    print("耗时%.3f"%(end-begain))
if __name__=='__main__':
    main()
        
        
    
