from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
rdd1 = sc.parallelize(["b", "a", "c"])
fmapRdd = rdd1.flatMap(lambda x: (x, 1))
fmapRdd.collect()