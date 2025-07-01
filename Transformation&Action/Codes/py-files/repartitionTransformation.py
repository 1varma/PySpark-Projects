from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('repartitionTransformation').getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize([1,2,3,4,5,6,7], 4)
sorted(rdd.glom().collect())
len(rdd.repartition(2).glom().collect())
len(rdd.repartition(10).glom().collect())