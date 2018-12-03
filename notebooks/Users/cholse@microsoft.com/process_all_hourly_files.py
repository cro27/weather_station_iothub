# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://wattleweather@ocpmelb.blob.core.windows.net/rawdata",
  mountPoint = "/mnt/raw_weather_data",
  extraConfigs = Map("fs.azure.account.key.ocpmelb.blob.core.windows.net" -> "rRaP7kXOaTtmuKN3zRXi2rOXvULXwbRxm+nMB12ac9VFyLRRac1bTRH4huzf6jamjJacmB+x0VhLcZi2riGbTA=="))

# COMMAND ----------

# Get hourly statistics from all weather data files and load to database
spark.conf.set("fs.azure.account.key.ocpmelb.blob.core.windows.net", "rRaP7kXOaTtmuKN3zRXi2rOXvULXwbRxm+nMB12ac9VFyLRRac1bTRH4huzf6jamjJacmB+x0VhLcZi2riGbTA==")

from datetime import datetime, timedelta
from time import strftime
from os import listdir, walk
from os.path import isfile, join
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

rootDir = '/dbfs/mnt/raw_weather_data/2018/07/12/01'
for dirName, subdirList, fileList in walk(rootDir):
    for fname in fileList:
        cFile = "%s/%s"
        dFile = cFile % (dirName, fname)
        currFile = re.sub('/dbfs', '', dFile)
        print(currFile)
        # Read file in
        sparkDF = spark.read.json(currFile)
        # Create temp table
        sparkDF.createOrReplaceTempView("raindata")

        # get weather data aggregates for last hour
        # as Spark SQL can only have one distinct function per query we build 3 views to hold distinct rec_timedate, enq_utctime and deviceID
        df1 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT distinct(deviceID) as deviceID, max(split(IoTHub['MessageID'], '_')[1]) as MessageID, cast((max(rain_total) - min(rain_total)) as decimal(10,1)) as rainfall_hour, min(temp_out) as temp_min, max(temp_out) as temp_max, max(wind_gust) as wind_gust_max, cast(avg(wind_avg) as decimal(10,1)) as wind_avg, max(hum_out) as humidity_max, min(hum_out) as humidity_min, max(abs_pres) as air_pressure_max, min(abs_pres) as air_pressure_min FROM raindata group by deviceID ")
        df2 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT distinct(date_trunc('hour', rec_time) + INTERVAL 1 HOURS) as rec_datetime, max(split(IoTHub['MessageID'], '_')[1]) as MessageID FROM raindata group by rec_datetime ")
        df3 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v3 AS SELECT distinct(date_trunc('hour', EventEnqueuedUtcTime) + INTERVAL 1 HOURS) as enq_utctime, max(split(IoTHub['MessageID'], '_')[1]) as MessageID FROM raindata group by enq_utctime")
        # Get dataframe based on select from all views joining on MessageID
        weatherhour = spark.sql("SELECT v1.deviceID, v2.rec_datetime, v3.enq_utctime, v1.rainfall_hour, v1.temp_min, v1.temp_max, v1.wind_gust_max, v1.wind_avg, v1.humidity_min, v1.humidity_max, v1.air_pressure_min, v1.air_pressure_max FROM v1, v2, v3 WHERE v1.MessageID = v2.MessageID and v1.MessageID = v3.MessageID")


        # Write dataframe to  file then upload 
        #df1.repartition(1).write.csv(path="/opt/Output/sqlcsvA.csv", mode="append")
        #weatherhour.write.save(path='wasbs://wattleweather@ocpmelb.blob.core.windows.net/output', format='csv', mode='append', sep=',')
        #weatherhour.repartition(1).write.csv(path='wasbs://wattleweather@ocpmelb.blob.core.windows.net/output/weatherhour.csv', mode='append', sep=',')
        
        # Write dataframe to SQL Datbase
        sqlserver = 'pdumelb-svr1.database.windows.net'
        port = '1433'
        database = 'pdumelb_sqldb1'
        user = 'sqladmin'
        pswd = 'Welcome12345'
  
        weatherhour.write \
           .option('user', user) \
           .option('password', pswd) \
           .jdbc('jdbc:sqlserver://' + sqlserver + ':' + port + ';database=' + database, 'dbo.Weather_hourly', mode = 'append' )

