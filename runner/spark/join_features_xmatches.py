
from pyspark.sql import *
from pyspark.sql.functions import col, rank

spark = SparkSession.builder.getOrCreate()


features = "s3://ztf-data-releases/dr11/features/4most_fields_extragalactic/*/*/*.parquet"
features = spark.read.load(features)

# Join features with PS1
ps1 = "s3://ztf-data-releases/dr11/xmatch/4most_extragalactic/ps1/*.parquet"
ps1 = spark.read.load(ps1)\
    .withColumnRenamed("objectid", "oid")\
    .withColumnRenamed("filterid", "fid")\
    .drop("ra", "dec")
col_names = [c[:-2] if c.endswith("_2") else c for c in ps1.columns]  # draw _2 suffix
ps1 = ps1.toDF(*col_names)

result = features.join(ps1,
                       on=[features.objectid == ps1.oid, features.filterid == ps1.fid],
                       how="outer").drop("oid", "fid")

# drop duplicates by (objectid, filterid) with minimum distance
window = Window.partitionBy("filterid", "objectid").orderBy("distance")
result = result.withColumn("rank", rank().over(window)).filter(col("rank") == 1).drop("rank", "distance")

# Join result with CatWISE
catwise = "s3://ztf-data-releases/dr11/xmatch/4most_extragalactic/catwise/*.parquet"
catwise = spark.read.load(catwise)\
    .withColumnRenamed("objectid", "oid")\
    .withColumnRenamed("filterid", "fid")\
    .drop("ra", "dec")
col_names = [c[:-2] if c.endswith("_2") else c for c in ps1.columns]  # draw _2 suffix
result = result.join(catwise,
                     on=[result.objectid == catwise.oid, result.filterid == catwise.fid],
                     how="outer").drop("oid", "fid")
# drop duplicates by (objectid, filterid) with minimum distance
window = Window.partitionBy("filterid", "objectid").orderBy("distance")
result = result.withColumn("rank", rank().over(window)).filter(col("rank") == 1).drop("rank", "distance")


result.write.parquet("s3://ztf-data-releases/dr11/training_set/4most_extragalactic")

