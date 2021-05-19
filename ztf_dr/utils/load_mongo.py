import boto3
import numpy as np
import pandas as pd
import pymongo
import logging
import os

from multiprocessing import Pool
from six.moves.urllib import parse
from ztf_dr.utils.preprocess import Preprocessor


def get_batches(data: pd.DataFrame, batch_size=100000):
    batches = [data.index.values[i:i + batch_size] for i in range(0, data.shape[0], batch_size)]
    return batches


def insert_batch(data: pd.DataFrame, indexes: list or np.ndarray, mongo_collection: pymongo.collection.Collection):
    batch = data.loc[indexes]
    records = list(batch.T.to_dict().values())
    total_records = len(records)
    for i in range(total_records):
        records[i]["hmjd"] = records[i]["hmjd"].tolist()
        records[i]["mag"] = records[i]["mag"].tolist()
        records[i]["magerr"] = records[i]["magerr"].tolist()

    mongo_collection.insert_many(records)
    return total_records


def s3_parquet_to_mongo(bucket_name: str, filename: str, mongo_config: dict, batch_size: int = 100000):
    input_file = os.path.join("s3://", bucket_name, filename)
    preprocessor = Preprocessor(limit_epochs=1)
    df = pd.read_parquet(input_file)
    df = preprocessor.run(df)
    logger = logging.getLogger("load_mongo")
    if df.shape[0] == 0:
        logger.info(f"[PID {os.getpid()}] Inserted {0: >7} from {filename}")
        return 0

    del df['catflags']
    del df['clrcoeff']

    df["loc"] = df.apply(lambda x: {
        "loc": {
            "type": "Point",
            "coordinates": [x["objra"] - 180, x["objdec"]]
        }
    }, axis=1, result_type='expand')

    del df["objra"]
    del df["objdec"]

    indexes_batches = get_batches(df, batch_size=batch_size)

    total_inserted = 0

    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        db = mongo_client[mongo_config["mongo_database"]]
        collection = db[mongo_config["mongo_collection"]]
        for batch in indexes_batches:
            inserted = insert_batch(df, batch, collection)
            total_inserted += inserted

    logger.info(f"[PID {os.getpid()}] Inserted {total_inserted: >7} from {filename}")
    return total_inserted


def insert_data(s3_url_bucket: str,
                mongo_uri: str = "localhost",
                mongo_database: str = "default",
                mongo_collection: str = "objects",
                batch_size: int = 10000,
                n_cores: int = 1):

    mongo_config = {
        "mongo_uri": mongo_uri,
        "mongo_database": mongo_database,
        "mongo_collection": mongo_collection
    }

    url_parse = parse.urlparse(s3_url_bucket)
    bucket_name = url_parse.netloc
    key_prefix = url_parse.path[1:]

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = sorted([x.key for x in bucket.objects.filter(Prefix=key_prefix) if x.key.endswith(".parquet")])

    if n_cores == 1:
        for f in files:
            s3_parquet_to_mongo(bucket_name, f, mongo_config, batch_size=batch_size)
    elif n_cores > 1:
        args = [(bucket_name, f, mongo_config, batch_size) for f in files]
        with Pool(n_cores) as p:
            p.starmap(s3_parquet_to_mongo, args)


if __name__ == "__main__":
    print("Don't use me :(")

    logger = logging.getLogger("load_mongo")
    logger.setLevel("INFO")

    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level="INFO")

    file = logging.FileHandler("load_mongo.log")
    logger.addHandler(file)
    logger.info("Init now")

    with pymongo.MongoClient("localhost", serverSelectionTimeoutMS=10000) as mongo_client:
        db = mongo_client['default']
        collection = db["objects"]
        resp = collection.create_index([("nepochs", 1)])
        logger.info(f"Index response: {resp}")
        resp = collection.create_index([("objectid", 1)])
        logger.info(f"Index response: {resp}")
        resp = collection.create_index([("filterid", "hashed")])
        logger.info(f"Index response: {resp}")
        resp = collection.create_index([("fieldid", 1)])
        logger.info(f"Index response: {resp}")
        resp = collection.create_index([("rcid", 1)])
        logger.info(f"Index response: {resp}")
        # http://strakul.blogspot.com/2019/07/data-science-mongodb-sky-searches-with.html
        resp = collection.create_index([("loc", "2dsphere")])
        logger.info(f"Index response: {resp}")
        insert_data("s3://ztf-data-releases/dr5/raw/field", n_cores=2)
