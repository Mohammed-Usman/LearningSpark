from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *

from pyspark.sql.types import *

# StructType([
# 	StructField("ID",IntergerType(), True),
# 	StructField("ID",IntergerType(), True)
# 	])

# Programmatic way to define a schema

spark = SparkSession.builder.appName("DataFrameReaderEg").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

fire_schema = StructType([
	StructField('CallNumber', IntegerType(), True),
	StructField('UnitID', StringType(), True),
	StructField('IncidentNumber', IntegerType(), True),
	StructField('CallType', StringType(), True),
	StructField('CallDate', StringType(), True),
	StructField('WatchDate', StringType(), True),
	StructField('CallFinalDisposition', StringType(), True),
	StructField('AvailableDtTm', StringType(), True),
	StructField('Address', StringType(), True),
	StructField('City', StringType(), True),
	StructField('Zipcode', IntegerType(), True),
	StructField('Battalion', StringType(), True),
	StructField('StationArea', StringType(), True),
	StructField('Box', StringType(), True),
	StructField('OriginalPriority', StringType(), True),
	StructField('Priority', StringType(), True),
	StructField('FinalPriority', IntegerType(), True),
	StructField('ALSUnit', BooleanType(), True),
	StructField('CallTypeGroup', StringType(), True),
	StructField('NumAlarms', IntegerType(), True),
	StructField('UnitType', StringType(), True),
	StructField('UnitSequenceInCallDispatch', IntegerType(), True),
	StructField('FirePreventionDistrict', StringType(), True),
	StructField('SupervisorDistrict', StringType(), True),
	StructField('Neighborhood', StringType(), True),
	StructField('Location', StringType(), True),
	StructField('RowID', StringType(), True),
	StructField('Delay', FloatType(), True)
	])

sf_fire_file = "data/sf-fire-calls.csv"

fire_df = spark.read.csv(sf_fire_file,header=True,schema=fire_schema)
'''
parquet_path = "data/sf-fire-calls1.parquet"

fire_df.write.format("parquet").save(parquet_path)
fire_df.write.parquet(parquet_path)
'''

# fire_df.show(2)
# print("(",fire_df.count(),",",len(fire_df.columns),")")


'''
few_fire_df = fire_df.select("IncidentNumber","AvailableDtTm","CallType").where(col("CallType") != "Medical Incident")

few_fire_df.show(21000,truncate=False)
'''

fire_df.select("CallType").where(col("CallType").isNotNull()).agg(countDistinct("CallType").alias("DistinctCallTypes")).show()

fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(30,truncate=False)


fire_df.select("CallType").distinct().show(30,truncate=False)

fire_df.select("CallType").groupBy("CallType").count().show()

spark.stop()

