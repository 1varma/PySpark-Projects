from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
rdd1 = spark.sparkContext.parallelize([20,32,45,62,8,5])
value = rdd1.reduce(lambda a,b : (a+b))
print(value)
rdd1.top(1)