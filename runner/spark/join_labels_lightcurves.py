import sys
from pyspark.sql import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


labels = sys.argv[1]
data_dir = sys.argv[2]
output = sys.argv[3]

labels_df = spark.read.load(labels)
df = spark.read.load(data_dir)\
    .select("objectid", "filterid", "hmjd", "mag", "magerr", "clrcoeff", "catflags")\
    .withColumnRenamed("objectid", "oid")\
    .withColumnRenamed("filterid", "fid")


result = labels_df.join(df, on=[labels_df.objectid == df.oid, labels_df.filterid == df.fid], how="inner")
result = result.drop(col("oid"))
result.write.option("maxRecordsPerFile", 50000).partitionBy("fid").parquet(output)
