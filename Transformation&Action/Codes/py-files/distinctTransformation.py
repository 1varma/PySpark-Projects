from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())