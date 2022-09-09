from pyspark.sql import *


def xmatch(big_catalog: DataFrame,
           small_catalog: DataFrame,
           spark_session: SparkSession,
           healpix_level: int,
           radius: float) -> DataFrame:
    api = spark_session._jvm.org.alerce.minimal_astroide
    big_catalog_healpix = api.HealpixPartitioner.execute(spark_session._jsparkSession,
                                                         big_catalog._jdf,
                                                         healpix_level,
                                                         'ra',
                                                         'dec')
    big_catalog_healpix = DataFrame(big_catalog_healpix, big_catalog.sql_ctx)

    small_catalog_healpix = api.HealpixPartitioner.execute(spark_session._jsparkSession,
                                                           small_catalog._jdf,
                                                           healpix_level,
                                                           'ra',
                                                           'dec')
    small_catalog_healpix = DataFrame(small_catalog_healpix, small_catalog.sql_ctx)

    result = api.Xmatcher.execute(spark_session._jsparkSession,
                                  big_catalog_healpix._jdf,
                                  small_catalog_healpix._jdf,
                                  healpix_level,
                                  radius,
                                  False)

    result = DataFrame(result, result.sql_ctx)
    return result
