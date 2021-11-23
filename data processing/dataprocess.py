#!/usr/bin/env python
# coding: utf-8

import csv
import json
import sys
import os
import pandas as pd
import numpy as np


json_file_path='yelp_academic_dataset_business.json'
csv_file_path='yelp_academic_dataset_business.csv'

with open(json_file_path,'r',encoding='utf-8') as fin:
    for line in fin:
        line_contents = json.loads(line)
        headers=line_contents.keys()
        break
    print(headers)
with open(csv_file_path, 'w', newline='',encoding='utf-8') as fout:
    writer=csv.DictWriter(fout, headers)
    writer.writeheader()
    with open(json_file_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            line_contents = json.loads(line)
            #if 'Phoenix' in line_contents.values():
            writer.writerow(line_contents)   

json_file_path='yelp_academic_dataset_business.json'
csv_file_path='yelp_academic_dataset_business3.csv'

with open(json_file_path,'r',encoding='utf-8') as fin:
    for line in fin:
        line_contents = json.loads(line)
        headers=line_contents.keys()
        break
    print(headers)
with open(csv_file_path, 'w', newline='',encoding='utf-8') as fout:
    writer=csv.DictWriter(fout, headers)
    writer.writeheader()
    with open(json_file_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            line_contents = json.loads(line)
        
            writer.writerow(line_contents)   
df_bus=pd.read_csv(csv_file_path)
df_reduced=df_bus.drop(['attributes','hours'], axis = 1)
df_cleaned=df_reduced.dropna()
df_cleaned.to_csv(csv_file_path,index=False)

df_bus=pd.read_csv(csv_file_path)
#df_reduced=df_bus.drop(['state','postal_code','is_open','attributes'], axis = 1)
df_cleaned=df_bus.dropna()
df_cleaned.to_csv(csv_file_path,index=False)


df_bus=pd.read_csv(csv_file_path)
n=len(df_bus)
for i in range(n):
    k1 = str(df_bus.categories.loc[i]).split(',')
    if 'Restaurants' not in k1 and ' Restaurants' not in k1:
        df_bus.drop(index=i, inplace=True)

df_bus.to_csv(csv_file_path,index=False)

json_file_path='/home/yelp_dataset/business.json'
csv_file_path='/home/yelp_dataset/business.csv'

with open(json_file_path,'r',encoding='utf-8') as fin:
    for line in fin:
        line_contents = json.loads(line)
        headers=line_contents.keys()
        break
    print(headers)
with open(csv_file_path, 'w', newline='',encoding='utf-8') as fout:
    writer=csv.DictWriter(fout, headers)
    writer.writeheader()
    with open(json_file_path, 'r', encoding='utf-8') as fin:
        for line in fin:
            line_contents = json.loads(line)
            if 'Phoenix' in line_contents.values():
                writer.writerow(line_contents)
df_bus=pd.read_csv(csv_file_path)
df_reduced=df_bus.drop(['state','postal_code','is_open','attributes'], axis = 1)
df_cleaned=df_reduced.dropna()
df_cleaned.to_csv(csv_file_path,index=False)


df_bus=pd.read_csv(csv_file_path)
n=len(df_bus)
for i in range(n):
    k1 = str(df_bus.categories.loc[i]).split(',')
    if 'Restaurants' not in k1 and ' Restaurants' not in k1:
        df_bus.drop(index=i, inplace=True)

df_bus.to_csv(csv_file_path,index=False)

import csv
import pymysql
import codecs  
import time

'''
python-mysql
'''
class PyMysql:
    def __init__(self):
        self.db=input('name')
        self.table=input('name' )

    def conn_mysql(self):
        conn=pymysql.connect(
            host='localhost',   
            port=3306,    
            user='root',   
            password='hou199897',   
            charset='utf8'   
        )
        return conn

    def create_db(self,cur):
        db=cur.cursor() 
        db.execute("create database if not exists {} character set utf8;".format(self.db))  #创建数据库
        db.execute("use {};".format(self.db))  
        cur.commit() 
        print('创建数据库成功')
        return cur

    def create_table_head(self,db,head):
            sql='create table if not exists {}('.format(self.table)  
            for i in range(0,len(head)):   
                sql+='{} varchar(100)'.format(head[i])
                if i!=len(head)-1: 
                    sql+=','
                sql+='\n'
            sql+=');'
            cur = db.cursor()   
            cur.execute(sql)   
            db.commit() 
            time.sleep(0.1)  
            print('创建表完成')

    def insert_table_info(self,db,info):
        sql='insert into {} values ('.format(self.table)
        for i in range(0,len(info)):
            sql+='"{}" '.format(info[i])
            if i!=len(info)-1:
                sql+=','
        sql+=');'
        try:
            cur = db.cursor()
            cur.execute(sql)
            db.commit()
        except Exception as e:
            print('error',e)

    def table_head(self,filename):
        with codecs.open(filename=filename,mode='r',encoding='utf-8') as f:  
            reader=csv.reader(f)
            head=next(reader)
            return head

    def table_info(self,db,filename):
        with codecs.open(filename=filename,mode='r',encoding='utf-8') as f:
            data=csv.reader(f)  
            for index,rows in enumerate(data):
                if index!=0:   
                    row=rows
                    self.insert_table_info(db,row)


if __name__=='__main__':
    pysql=PyMysql()
    cur=pysql.conn_mysql()  
    db=pysql.create_db(cur)  
    filename='yelp_academic_dataset_business3.csv' 
    head=pysql.table_head(filename) 
    pysql.create_table_head(db,head)  
    pysql.table_info(db,filename) 





