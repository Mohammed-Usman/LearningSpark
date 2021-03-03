from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("ExampleRow").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",["twitter", "LinkedIn"])


print("***************\n"*2)
print(blog_row[1])
print("***************\n"*2)




rows = [Row("Matri Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows,["Authors", "State"])

authors_df.show()
spark.stop()