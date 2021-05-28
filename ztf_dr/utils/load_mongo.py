import boto3
import logging
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pymongo
import os
import gc

from typing import List, Set
from multiprocessing import Pool
from six.moves.urllib import parse
from ztf_dr.utils.preprocess import Preprocessor


def _get_already_preprocess(path="/tmp/") -> Set[str]:
    files = [f for f in os.listdir(path) if f.startswith("already")]
    files = [pd.read_csv(os.path.join(path, f), header=None) for f in files]
    if len(files) == 0:
        return {}
    df = pd.concat(files)
    df.columns = ["file", "inserted"]
    del files
    val = set(df["file"].values)
    return val


def get_batches(data: pd.DataFrame, batch_size=100000):
    batches = [data.index.values[i:i + batch_size] for i in range(0, data.shape[0], batch_size)]
    return batches


def insert_batch(data: pd.DataFrame, indexes: List or np.ndarray, mongo_collection: pymongo.collection.Collection):
    batch = data.loc[indexes]
    records = list(batch.T.to_dict().values())
    total_records = len(records)
    mongo_collection.insert_many(records)
    del records
    del batch
    return total_records


def s3_parquet_to_mongo(bucket_name: str, filename: str, mongo_config: dict, batch_size: int = 10000):
    input_file = os.path.join("s3://", bucket_name, filename)
    limit_epochs = {
        1: 5,
        2: 5,
        3: 1
    }
    preprocessor = Preprocessor(limit_epochs=limit_epochs)
    df = dd.read_parquet(input_file, engine="pyarrow")
    try:
        df = preprocessor.run(df)
        logger = logging.getLogger("load_mongo")
        if df is None or df.shape[0] == 0:
            logger.info(f"[PID {os.getpid()}] Inserted {0: >7} from {filename}")
            return 0

    except Exception as e:
        raise Exception(f"{filename} with problems: {e}")

    del df['catflags']
    del df['clrcoeff']

    df["loc"] = df.apply(lambda x: {
        "loc": {
            "type": "Point",
            "coordinates": [x["objra"] - 180, x["objdec"]]
        }
    }, axis=1, result_type='expand')

    df.rename(columns={"objectid": "_id"}, inplace=True)
    for col in ["mag", "magerr", "hmjd"]:
        df[col] = df[col].map(lambda x: x.tobytes())

    indexes_batches = get_batches(df, batch_size=batch_size)

    total_inserted = 0

    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        db = mongo_client[mongo_config["mongo_database"]]
        collection = db[mongo_config["mongo_collection"]]
        for batch in indexes_batches:
            inserted = insert_batch(df, batch, collection)
            total_inserted += inserted
    logger.info(f"[PID {os.getpid()}] Inserted {total_inserted: >7} from {filename}")
    del df
    del indexes_batches
    gc.collect()
    with open(f"/tmp/already_{os.getpid()}.csv", "a") as f:
        f.write(f"{filename}, {total_inserted}\n")
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
    files = set(files)
    total = len(files)
    processed = _get_already_preprocess()

    files = files.difference(processed)
    logger = logging.getLogger("load_mongo")
    logger.info(f"Total={total:>7} | Processed={len(processed):>7} | To process={len(files):>7}")
    if n_cores == 1:
        for f in files:
            s3_parquet_to_mongo(bucket_name, f, mongo_config, batch_size=batch_size)
    elif n_cores > 1:
        args = [(bucket_name, f, mongo_config, batch_size) for f in files]
        del files
        with Pool(n_cores) as p:
            p.starmap(s3_parquet_to_mongo, args)


def drop_mongo(mongo_uri: str, mongo_database: str, mongo_collection: str):
    logger = logging.getLogger("load_mongo")
    with pymongo.MongoClient(mongo_uri) as _mongo_client:
        logger.info(f"Dropping {mongo_database}/{mongo_collection}")
        _db = _mongo_client[mongo_database]
        _col = _db[mongo_collection]
        _col.drop()


def init_mongo(mongo_uri: str, mongo_database: str, mongo_collection: str):
    logger = logging.getLogger("load_mongo")
    with pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as _mongo_client:
        _db = _mongo_client[mongo_database]
        _collection = _db[mongo_collection]
        resp = _collection.create_index([("nepochs", 1)])
        logger.info(f"Index response: {resp}")
        resp = _collection.create_index([("objectid", 1)])
        logger.info(f"Index response: {resp}")
        resp = _collection.create_index([("filterid", "hashed")])
        logger.info(f"Index response: {resp}")
        resp = _collection.create_index([("fieldid", 1)])
        logger.info(f"Index response: {resp}")
        resp = _collection.create_index([("rcid", 1)])
        logger.info(f"Index response: {resp}")
        # http://strakul.blogspot.com/2019/07/data-science-mongodb-sky-searches-with.html
        resp = _collection.create_index([("loc", "2dsphere")])
        logger.info(f"Index response: {resp}")


if __name__ == "__main__":
    print("Don't use me :(")
