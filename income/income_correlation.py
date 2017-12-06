# hfs -put income_original.csv
from sklearn import linear_model
from pyspark.sql import SQLContext 
from pyspark.sql.functions import isnan, when, count, col, length, desc, unix_timestamp, from_unixtime

import numpy as np

sqlContext = SQLContext(sc)

# Read income csv
df_income = sqlContext.read.load('income_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Read complaint csv and clean data
df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
# Discard Columns
drop_list = ['Facility Type', 'School Name', 'School Number', 'School Region', 'School Code', 'School Phone Number', 'School Address', 'School City', 'School State', 'School Zip']
df = df.select([column for column in df.columns if column not in drop_list])
# Coerce invalid values to normal
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^(\d{5}(-)?(\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d)$')== False, 'N/A').otherwise(df['Incident Zip']))
# Fill null values to N/A
df_complaint = df.fillna('N/A')

# Join income csv and complaint csv
df_count_zip = df_complaint.groupBy('Incident Zip').count()
zip_income_joined = df_count_zip.join(df_income, df_count_zip['Incident Zip'] == df_income['zip'])

# Use linear regression model for income and complaint amount
x_income_census = np.array(zip_income_joined.select('income').collect())
y_count = np.array(zip_income_joined.select('count').collect())
reg = linear_model.LinearRegression()
x_income_census = x_income_census.reshape(-1,1)
reg.fit(x_income_census, y_count)
r_square = reg.score(x_income_census, y_count)
print('Income & compaint: ',r_square)


# Use linear regression model for household and complaint amount
x_income_census = np.array(zip_income_joined.select('household').collect())
y_count = np.array(zip_income_joined.select('count').collect())
reg = linear_model.LinearRegression()
x_income_census = x_income_census.reshape(-1,1)
reg.fit(x_income_census, y_count)
r_square = reg.score(x_income_census, y_count)
print('Household & compaint: ',r_square)