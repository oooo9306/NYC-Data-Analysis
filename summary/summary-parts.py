pyspark --packages com.databricks:spark-csv_2.10:1.2.0

from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, length, avg

sqlContext = SQLContext(sc)

df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
df.count()

df = df.withColumn('year', col('Created Date').substr(7,4))
df = df.withColumn('month', col('Created Date').substr(0,2))
df = df.withColumn('date', col('Created Date').substr(0,10))

# top 5 of the number of complaints in 2009-2017
df.where(col('year').like('%09')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%10')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%11')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%12')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%13')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%14')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%15')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)
df.where(col('year').like('%16')).groupBy(col('date')).count().orderBy('count',ascending=False).show(5)

# bottle 5 of the number of complaints in 2009-2017
df.where(col('year').like('%09')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%10')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%11')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%12')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%13')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%14')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%15')).groupBy(col('date')).count().orderBy('count').show(5)
df.where(col('year').like('%16')).groupBy(col('date')).count().orderBy('count').show(5)

# get the number of complaints on Christmas Day in 2013
df.where(col('date').like('12/25/2013')).count()

# the number of every month's complaints in 2009-2017
df.where(col('year').like('%09')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%10')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%11')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%12')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%13')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%14')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%15')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%16')).groupBy(col('month')).count().orderBy('month').show()
df.where(col('year').like('%17')).groupBy(col('month')).count().orderBy('month').show()
