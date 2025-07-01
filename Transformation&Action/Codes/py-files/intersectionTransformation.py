from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
rdd1.intersection(rdd2).collect()