from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
rdd1 = sc.parallelize(["b", "a", "c"])
mapRdd = rdd1.map(lambda x: (x, 1))
mapRdd.collect()