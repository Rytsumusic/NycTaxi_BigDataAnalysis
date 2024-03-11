from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps



conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark.executor.memory", "2g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.cores", "5")
spark = SparkContext(conf=conf)

df =ps.read_parquet("/data/nyctaxi/set1/*.parquet")
print(df.info(verbose=True))

print(df.groupby('payment_type')['fare_amount'].mean())

#average ratio of trip cost tht is tolls
average_tolls = df['tolls_amount'].mean()/df['fare_amount'].mean()
print(average_tolls)

#total num of trips per month across all years
#Average price per mile, excluding tolls and mta taxes
#Most popular pickup and drop off locations(use lat/long but rounded to 3 decimal places)


