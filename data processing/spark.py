#!/usr/bin/env python
# coding: utf-8

rom pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
 
def data_process(raw_data_path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.json(raw_data_path)
    split_col = f.split(business['categories'], ',')
    business = business.withColumn("categories", split_col).filter(business["city"] != "").dropna()
    business.createOrReplaceTempView("business")

    b_etl = spark.sql("SELECT business_id, name, city, state, latitude, longitude, stars, review_count, is_open, categories, attributes FROM business").cache()
    b_etl.createOrReplaceTempView("b_etl")
    outlier = spark.sql(
        "SELECT b1.business_id, SQRT(POWER(b1.latitude - b2.avg_lat, 2) + POWER(b1.longitude - b2.avg_long, 2)) \
        as dist FROM b_etl b1 INNER JOIN (SELECT state, AVG(latitude) as avg_lat, AVG(longitude) as avg_long \
        FROM b_etl GROUP BY state) b2 ON b1.state = b2.state ORDER BY dist DESC")
    outlier.createOrReplaceTempView("outlier")
    joined = spark.sql("SELECT b.* FROM b_etl b INNER JOIN outlier o ON b.business_id = o.business_id WHERE o.dist<10")
    joined.write.parquet("business_etl", mode="overwrite")
if __name__ == "__main__":
    raw_hdfs_path = 'yelp_academic_dataset_business.json'
    print("Start cleaning raw data!")
    data_process(raw_hdfs_path)
    print("Successfully done")

import os
 
def attribute_score(attribute):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    att = spark.sql("SELECT attributes.{attr} as {attr}, category, stars FROM for_att".format(attr=attribute)).dropna()
    att.createOrReplaceTempView("att")
    att_group = spark.sql("SELECT {attr}, AVG(stars) AS stars FROM att GROUP BY {attr} ORDER BY stars".format(attr=attribute))
    att_group.show()    
    att_group.write.json("{attr}".format(attr=attribute), mode='overwrite')
 
 
def analysis(data_path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.parquet(data_path).cache()
    business.createOrReplaceTempView("business")
 
    part_business = spark.sql("SELECT state, city, stars, review_count, 
                               explode(categories) AS category FROM business").cache()
    part_business.show()
    part_business.createOrReplaceTempView('part_business_1')
    part_business = spark.sql("SELECT state, city, stars, review_count, 
                               REPLACE(category, ' ','')as new_category FROM part_business_1")
    part_business.createOrReplaceTempView('part_business')
 
 
    print("## All distinct categories")
    all_categories = spark.sql("SELECT business_id, explode(categories)
                                 AS category FROM business")
    all_categories.createOrReplaceTempView('all_categories')
 
    distinct = spark.sql("SELECT COUNT(DISTINCT(new_category)) FROM part_business")
    distinct.show()
 
    print("## Top 10 business categories")
    top_cat = spark.sql("SELECT new_category, COUNT(*) as freq 
                         FROM part_business GROUP BY new_category ORDER BY freq DESC")
    top_cat.show(10)   
    top_cat.write.json("top_category", mode='overwrite')
 
    print("## Top business categories - in every city")
    top_cat_city = spark.sql("SELECT city, new_category, COUNT(*) as freq 
                              FROM part_business GROUP BY city, new_category 
                              ORDER BY freq DESC")
    top_cat_city.show()  
    top_cat.write.json("top_category_city", mode='overwrite')
 
    print("## Cities with most businesses")
    bus_city = spark.sql("SELECT city, COUNT(business_id) as no_of_bus FROM business GROUP BY city ORDER BY no_of_bus DESC")
    bus_city.show(10)   
    bus_city.write.json("top_business_city", mode='overwrite')
 
    print("## Average review count by category")
    avg_city = spark.sql(
        "SELECT new_category, AVG(review_count)as avg_review_count FROM part_business GROUP BY new_category ORDER BY avg_review_count DESC")
    avg_city.show()  
    avg_city.write.json("average_review_category", mode='overwrite')
 
 
    print("## Average stars by category")
    avg_state = spark.sql(
        "SELECT new_category, AVG(stars) as avg_stars FROM part_business GROUP BY new_category ORDER BY avg_stars DESC")
    avg_state.show()   
    avg_state.write.json("average_stars_category", mode='overwrite')
    
    print("## Data based on Attribute")
    for_att = spark.sql("SELECT attributes, stars, explode(categories) AS category FROM business")
    for_att.createOrReplaceTempView("for_att")
    attribute = 'RestaurantsTakeout'
    attribute_score(attribute)

if __name__ == "__main__":
    business_data_path = 'business_etl' 
    print("Start analysis data!")
    analysis(business_data_path)
    print("Analysis done")

import json
import os
import pandas as pd
import matplotlib.pyplot as plt
 
AVE_REVIEW_CATEGORY = 'average_review_category'
OPEN_CLOSE = 'open_close'
TOP_CATEGORY_CITY = 'top_category_city'
TOP_BUSINESS_CITY = 'top_business_city'
TOP_CATEGORY = 'top_category'
AVE_STARS_CATEGORY = 'average_stars_category'
TAKEOUT = 'RestaurantsTakeout'
 
def read_json(file_path):
    json_path_names = os.listdir(file_path)
    data = []
    for idx in range(len(json_path_names)):
        json_path = file_path + '/' + json_path_names[idx]
        if json_path.endswith('.json'):
            with open(json_path) as f:
                for line in f:
                    data.append(json.loads(line))
    return data
 
 
 
if __name__ == '__main__':
    ave_review_category_list = read_json(AVE_REVIEW_CATEGORY)
    top_category_city_list = read_json(TOP_CATEGORY_CITY)
    top_business_city_list = read_json(TOP_BUSINESS_CITY)
    top_category_list = read_json(TOP_CATEGORY)
    ave_stars_category_list = read_json(AVE_STARS_CATEGORY)
    takeout_list = read_json(TAKEOUT)
 
 
    top_category_list.sort(key=lambda x: x['freq'], reverse=True)
    top_category_key = []
    top_category_value = []
    for idx in range(10):
        one = top_category_list[idx]
        top_category_key.append(one['new_category'])
        top_category_value.append(one['freq'])
 
    plt.barh(top_category_key[:10], top_category_value[:10], tick_label=top_category_key[:10])
    plt.title('Top 10 Categories', size = 16)
    plt.xlabel('Frequency',size =8, color = 'Black')
    plt.ylabel('Category',size = 8, color = 'Black')
    plt.tight_layout()
    plt.show()
 
 
    top_business_city_list.sort(key=lambda x: x['no_of_bus'], reverse=True)
    top_business_city_key = []
    top_business_city_value = []
    for idx in range(10):
        one = top_business_city_list[idx]
        top_business_city_key.append(one['no_of_bus'])
        top_business_city_value.append(one['city'])
 
    
    plt.barh(top_business_city_value[:10], top_business_city_key[:10], tick_label=top_business_city_value[:10])
    plt.title('Top 10 Cities with most businesses', size = 16)
    plt.xlabel('no_of_number',size =8, color = 'Black')
    plt.ylabel('city',size = 8, color = 'Black')
    plt.tight_layout()
    plt.show()
 
    ave_review_category_list.sort(key=lambda x: x['avg_review_count'], reverse=True)
    ave_review_category_key = []
    ave_review_category_value = []
    for idx in range(10):
        one = ave_review_category_list[idx]
        ave_review_category_key.append(one['avg_review_count'])
        ave_review_category_value.append(one['new_category'])
 
    
    plt.barh(ave_review_category_value[:10], ave_review_category_key[:10], tick_label=ave_review_category_value[:10])
    plt.title('Top 10 categories with most review', size=16)
    plt.xlabel('avg_review_count', size=8, color='Black')
    plt.ylabel('category', size=8, color='Black')
    plt.tight_layout()
    plt.show()
    
 
 
    ave_stars_category_list.sort(key=lambda x: x['avg_stars'], reverse=True)
    ave_stars_category_key = []
    ave_stars_category_value = []
    for idx in range(10):
        one = ave_stars_category_list[idx]
        ave_stars_category_key.append(one['avg_stars'])
        ave_stars_category_value.append(one['new_category'])
    
    
    plt.barh(ave_stars_category_value[:10], ave_stars_category_key[:10], tick_label=ave_stars_category_value[:10])
    plt.title('Top 10 categories with most stars', size=16)
    plt.xlabel('avg_stars', size=8, color='Black')
    plt.ylabel('category', size=8, color='Black')
    plt.tight_layout()
    plt.show()
    
    takeout_list.sort(key=lambda x: x['stars'], reverse=True)
    takeout_key = []
    takeout_value = []
    for idx in range(len(takeout_list)):
        one = takeout_list[idx]
        takeout_key.append(one['stars'])
        takeout_value.append(one['RestaurantsTakeout'])
    
    explode = (0,0,0)
    plt.pie(takeout_key,explode=explode,labels=takeout_value, autopct='%1.1f%%',shadow=False, startangle=150)
    plt.title('Whether take out or not', size=16)
    plt.axis('equal')
    plt.tight_layout()
    plt.show()

def attribute_score(attribute):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    att = spark.sql("SELECT attributes.{attr} as {attr}, category, stars FROM for_att".format(attr=attribute)).dropna()
    att.createOrReplaceTempView("att")
    att_group = spark.sql("SELECT {attr}, AVG(stars) AS stars FROM att GROUP BY {attr} ORDER BY stars".format(attr=attribute))
    att_group.show()    
    att_group.write.json("{attr}".format(attr=attribute), mode='overwrite')
 
 
def analysis(data_path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.parquet(data_path).cache()
    business.createOrReplaceTempView("business")

 
    print("## Top business categories - IN MA")
    top_cat_city = spark.sql("SELECT new_category, AVG(stars) as avg,
                             count(*) FROM part_business where state = 'MA' 
                             GROUP BY new_category ORDER BY count(*) DESC")
    top_cat_city.show()  

def attribute_score(attribute):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    att = spark.sql("SELECT attributes.{attr} as {attr}, category, stars FROM for_att".format(attr=attribute)).dropna()
    att.createOrReplaceTempView("att")
    att_group = spark.sql("SELECT {attr}, AVG(stars) AS stars FROM att GROUP BY {attr} ORDER BY stars".format(attr=attribute))
    att_group.show()    
    att_group.write.json("{attr}".format(attr=attribute), mode='overwrite')
 
 
def analysis(data_path):
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    business = spark.read.parquet(data_path).cache()
    business.createOrReplaceTempView("business")

 
    print("## Top 10 states with most businesses")
    avg_city = spark.sql(
        "SELECT state, count(*) FROM part_business group by state order by count(*) desc")
    avg_city.show()  

if __name__ == "__main__":
    business_data_path = 'business_etl' 
    print("Start analysis data!")
    analysis(business_data_path)
    print("Analysis done")






