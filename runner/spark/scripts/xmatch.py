import sys
from pyspark.sql import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
api = spark._jvm.org.alerce.minimal_astroide


labels = sys.argv[1]  # "s3://trainingsets2020/master_catalogs/master_cat4match_unique1.5arc_20220208.parquet"
data_dir = sys.argv[2]  # "s3://ztf-data-releases/dr8/raw/*"
output = sys.argv[3]  # "s3://ztf-data-releases/dr8/training_set/1.5arc_20220208"

radius = 1.5/3600.
healpix_level = 12

labels_df = spark.read.load(labels)
df = spark.read.load(data_dir).withColumnRenamed("objra", "ra").withColumnRenamed("objdec", "dec")

dfj_healpix = api.HealpixPartitioner.execute(spark._jsparkSession, df._jdf, healpix_level, 'ra', 'dec')
df_healpix = DataFrame(dfj_healpix, df.sql_ctx)

labelsj_healpix = api.HealpixPartitioner.execute(spark._jsparkSession, labels_df._jdf, healpix_level, 'ra', 'dec')
labels_healpix = DataFrame(labelsj_healpix, labels_df.sql_ctx)

jresult = api.Xmatcher.execute(spark._jsparkSession, df_healpix._jdf, labels_healpix._jdf, healpix_level, radius, False)
result = DataFrame(jresult, labels_df.sql_ctx)\
    .withColumnRenamed("source_cat_2", "source_cat")\
    .withColumnRenamed("source_class_2", "source_class")\
    .withColumnRenamed("classALeRCE_2", "classALeRCE")\
    .withColumnRenamed("source_id_2", "source_id")\
    .withColumnRenamed("ra", "objra")\
    .withColumnRenamed("dec", "objdec")

to_drop = ["ipix",
           "col1_2",
           "ra_2",
           "dec_2",
           "ipix_2",
           "ipix_neigh",
           "source_period_2",
           "source_redshift_2"]

result = result.drop(*to_drop)
duplicated = df.withColumn("fid", col("filterid"))
result.write.option("maxRecordsPerFile", 50000).partitionBy("fid").parquet(output)
