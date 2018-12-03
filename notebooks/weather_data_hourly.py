# Databricks notebook source
# Get hourly statistics from weather data
# Designed to be run on a schedule each hour
# Reads from previous hours weather data file and writes to a SQL Database table

# Set access to Azure Blob storage - do this instead of mount as if already mounted whole notebook fails scheduled execution
blobkey = dbutils.secrets.get(scope = "sqldbscope", key = "ocpmelblob")
spark.conf.set("fs.azure.account.key.ocpmelb.blob.core.windows.net", blobkey)

from datetime import datetime, timedelta
from time import strftime
from os import listdir, walk
from os.path import isfile, join
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get current Year, Month, Day and Previous Hour to format path
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")
hour = (datetime.now() - timedelta(minutes=60)).strftime("%H")
# If we are at midnight (processing hour 23) adjust day to previous day
if hour == '23':
    day = (datetime.now() - timedelta(days=1)).strftime("%d")

# Format path string to process previous hour
mypath = "wasbs://wattleweather@ocpmelb.blob.core.windows.net/rawdata/%s/%s/%s/%s/*.json"
path = mypath % (year, month, day, hour)

print("Processing file: " + path)

# Read previous hour file in
sparkDF = spark.read.json(path)

# Create temp table
sparkDF.createOrReplaceTempView("raindata")

# get weather data aggregates for last hour
# as Spark SQL can only have one distinct function per query we build 3 views to hold distinct rec_timedate, enq_utctime and deviceID
df1 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT distinct(deviceID) as deviceID, max(split(IoTHub['MessageID'], '_')[1]) as MessageID, cast((max(rain_total) - min(rain_total)) as decimal(10,1)) as rainfall_hour, min(temp_out) as temp_min, max(temp_out) as temp_max, max(wind_gust) as wind_gust_max, cast(avg(wind_avg) as decimal(10,1)) as wind_avg, max(hum_out) as humidity_max, min(hum_out) as humidity_min, max(abs_pres) as air_pressure_max, min(abs_pres) as air_pressure_min FROM raindata group by deviceID ")

df2 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT distinct(date_trunc('hour', rec_time) + INTERVAL 1 HOURS) as rec_datetime, max(split(IoTHub['MessageID'], '_')[1]) as MessageID FROM raindata group by rec_datetime ")

df3 = spark.sql("CREATE OR REPLACE TEMPORARY VIEW v3 AS SELECT distinct(date_trunc('hour', EventEnqueuedUtcTime) + INTERVAL 1 HOURS) as enq_utctime, max(split(IoTHub['MessageID'], '_')[1]) as MessageID FROM raindata group by enq_utctime")

# Get dataframe based on select from all views joining on MessageID
weatherhour = spark.sql("SELECT v1.deviceID, v2.rec_datetime, v3.enq_utctime, v1.rainfall_hour, v1.temp_min, v1.temp_max, v1.wind_gust_max, v1.wind_avg, v1.humidity_min, v1.humidity_max, v1.air_pressure_min, v1.air_pressure_max FROM v1, v2, v3 WHERE v1.MessageID = v2.MessageID and v1.MessageID = v3.MessageID")

# Write dataframe to SQL Datbase
sqlserver = 'pdumelb-svr1.database.windows.net'
port = '1433'
database = 'pdumelb_sqldb1'
sqlUsername = dbutils.secrets.get(scope = "sqldbscope", key = "sqluser")
sqlPassword = dbutils.secrets.get(scope = "sqldbscope", key = "sqlpwd")

print("Writing data to database: " + database)
  
weatherhour.write \
   .option('user', sqlUsername) \
   .option('password', sqlPassword) \
   .jdbc('jdbc:sqlserver://' + sqlserver + ':' + port + ';database=' + database, 'dbo.Weather_hourly', mode = 'append' )
  
print("Finished")