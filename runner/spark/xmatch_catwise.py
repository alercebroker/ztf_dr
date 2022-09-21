import sys
from pyspark.sql import *
from utils.xmatch import xmatch

CATWISE_PATH = "s3://alerce-catalogs/catalogs-parquets/CatWISE/*.parquet"
RADIUS = 2.0/3600.
HEALPIX_LEVEL = 12
TO_DROP = ["ipix", "ra_2", "dec_2", "ipix_2", "ipix_neigh"]


if __name__ == "__main__":
    data_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.getOrCreate()
    catalog = spark.read.load(CATWISE_PATH).select("source_id", "ra", "dec", "w1mpro_pm", "w2mpro_pm")
    data_release = spark.read.load(data_path).select("objectid", "filterid", "objra", "objdec").withColumnRenamed(
        "objra", "ra").withColumnRenamed("objdec", "dec")

    xmatched = xmatch(data_release, catalog, spark, HEALPIX_LEVEL, RADIUS)
    xmatched = xmatched.drop(*TO_DROP)
    xmatched.write.mode("overwrite").parquet(output_path)
