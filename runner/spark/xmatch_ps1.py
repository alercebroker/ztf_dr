import sys
from pyspark.sql import *
from .utils.xmatch import xmatch

spark = SparkSession.builder.getOrCreate()

ps1_path = "s3://alerce-catalogs/catalogs-parquets/PS1_lite/*.parquet"
data_path = sys.argv[1]
output_path = sys.argv[2]

RADIUS = 1.5/3600.
HEALPIX_LEVEL = 12
TO_DROP = ["ipix", "ra_2", "dec_2", "ipix_2", "ipix_neigh"]

catalog = spark.read.load(ps1_path).withColumnRenamed("RAJ2000", "ra").withColumnRenamed("DEJ2000", "dec")
data_release = spark.read.load(data_path).withColumnRenamed("objra", "ra").withColumnRenamed("objdec", "dec")

xmatched = xmatch(data_release, catalog, spark, HEALPIX_LEVEL, RADIUS)

xmatched = xmatched.drop(*TO_DROP)
xmatched.write.mode("overwrite").parquet(output_path)
