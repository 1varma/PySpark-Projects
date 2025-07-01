from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
sc.parallelize([1, 2, 3, 4, 5], 3).glom().collect()
sc.parallelize([1, 2, 3, 4, 5], 3).coalesce(1).glom().collect()