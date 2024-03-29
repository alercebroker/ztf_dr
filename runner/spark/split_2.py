import sys
from pyspark.sql import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data_dir = sys.argv[1]
output = sys.argv[2]

df = spark.read.load(data_dir).withColumn("fid", col("filterid"))
df.repartition("nepochs").write.option("maxRecordsPerFile", 10000).partitionBy("fid").parquet(output)
