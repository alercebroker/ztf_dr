import sys
from pyspark.sql import *
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()
# spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
api = spark._jvm.org.alerce.minimal_astroide


labels = sys.argv[1]  # "s3://trainingsets2020/master_catalogs/master_cat4match_unique1.5arc_20220208.parquet"
data_dir = sys.argv[2]  # "s3://ztf-data-releases/dr8/features_20det/*"
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
result = DataFrame(jresult, labels_df.sql_ctx)

result.coalesce(16).write.parquet(output)
