from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col
import pymysql

spark = SparkSession.builder.appName("pyspark mobike session").master("spark://spark-master:7077").config("spark.executor.memory","512m").getOrCreate()

mobike_df = spark.read.csv("mobike.csv", sep = ",", header = True)
weibo_df = spark.read.csv("weibo.csv", sep = ";", header = True)

weibo_df1 = weibo_df.select(['location_name', 'location_latitude', 'location_longitude'])
mobike_df1 = mobike_df.select(['bikeid', 'start_time', 'end_time','start_location_x', 'start_location_y', 'end_location_x', 'end_location_y'])

mobike_df1 = mobike_df1.withColumnRenamed('start_location_x', 'start_longitude')
mobike_df1 =mobike_df1.withColumnRenamed('start_location_y', 'start_latitude')
mobike_df1 =mobike_df1.withColumnRenamed('end_location_x', 'end_longitude')
mobike_df1 =mobike_df1.withColumnRenamed('end_location_y', 'end_latitude')



mobike_start_rdd = mobike_df1.rdd.map(lambda x: [x.bikeid, datetime.fromisoformat(x.start_time).isoweekday(), datetime.fromisoformat(x.start_time).hour, datetime.fromisoformat(x.start_time).minute, x.start_longitude,x.start_latitude])
mobike_end_rdd = mobike_df1.rdd.map(lambda x: [x.bikeid, datetime.fromisoformat(x.start_time).isoweekday(), datetime.fromisoformat(x.end_time).hour, datetime.fromisoformat(x.end_time).minute, x.end_longitude,x.end_latitude])
start_df = spark.createDataFrame(mobike_start_rdd, schema=['bikeid', 'weekday', 'hour', 'minute', 'location_longitude', 'location_latitude'])
end_df = spark.createDataFrame(mobike_end_rdd, schema=['bikeid', 'weekday', 'hour', 'minute', 'location_longitude', 'location_latitude'])


weibo_df1 = weibo_df1.filter(col('location_longitude').cast("float").isNotNull())
weibo_df1 = weibo_df1.filter(col('location_latitude').cast("float").isNotNull())
weibo_rdd = weibo_df1.rdd.map(lambda x: [x.location_name, round(float(x.location_longitude), 3), round(float(x.location_latitude), 3)])
weibo_df2 = spark.createDataFrame(weibo_rdd, schema=['location_name', 'location_longitude', 'location_latitude'])

weibo_df2 = weibo_df2.drop_duplicates(['location_name'])

start_df1 = start_df.join(weibo_df2, on = ['location_longitude', 'location_latitude'])
end_df1 = end_df.join(weibo_df2, on = ['location_longitude', 'location_latitude'])

start_df2 = start_df1.groupby(['location_name', 'location_longitude', 'location_latitude', 'weekday', 'hour']).agg({'bikeid': 'count'})
end_df2 = end_df1.groupby(['location_name', 'location_longitude', 'location_latitude', 'weekday', 'hour']).agg({'bikeid': 'count'})

start_df2 = start_df2.withColumnRenamed("count(bikeid)", 'bike_popularity')
end_df2 = end_df2.withColumnRenamed("count(bikeid)", 'bike_popularity')

start_df2 = start_df2.orderBy(["bike_popularity"], ascending = [0])
end_df2 = end_df2.orderBy(["bike_popularity"], ascending = [0])


con = pymysql.connect(host = 'mysql', port = 3306, user='root', password='root', charset = 'utf8')
sql = "CREATE DATABASE IF NOT EXISTS mobike"

cursor = con.cursor()

cursor.execute(sql)
sql = "USE mobike"
cursor.execute(sql)

sql = '''CREATE TABLE `start` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `location_name` CHAR(50) ,
  `location_longitude` FLOAT NOT NULL,
  `location_latitude` FLOAT NOT NULL,
  `weekday` INT NOT NULL,
  `hour` INT NOT NULL,
  `bike_popularity` INT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''
cursor.execute(sql)

sql = '''CREATE TABLE `end` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `location_name` CHAR(50) ,
  `location_longitude` FLOAT NOT NULL,
  `location_latitude` FLOAT NOT NULL,
  `weekday` INT NOT NULL,
  `hour` INT NOT NULL,
  `bike_popularity` INT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''
cursor.execute(sql)

dataCollect = start_df2.rdd.toLocalIterator()

for x in dataCollect:
    sql = "INSERT INTO start(location_name, location_longitude, location_latitude, weekday, hour, bike_popularity) \
           VALUES ('%s', '%s',  %s,  '%s',  %s, %s)" % \
           (x.location_name, x.location_longitude, x.location_latitude, x.weekday, x.hour, x.bike_popularity)
    try:
       # 执行sql语句
       cursor.execute(sql)
       # 执行sql语句
       con.commit()
    except:
       # 发生错误时回滚
       con.rollback()


enddataCollect = end_df2.rdd.toLocalIterator()
for x in enddataCollect:
    sql = "INSERT INTO end(location_name, location_longitude, location_latitude, weekday, hour, bike_popularity) \
           VALUES ('%s', '%s',  %s,  '%s',  %s, %s)" % \
           (x.location_name, x.location_longitude, x.location_latitude, x.weekday, x.hour, x.bike_popularity)
    try:
       # 执行sql语句
       cursor.execute(sql)
       # 执行sql语句
       con.commit()
    except:
       # 发生错误时回滚
       con.rollback()

print("write data done")