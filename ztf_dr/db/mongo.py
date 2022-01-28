import logging
import pandas as pd
import pymongo
import os

from ztf_dr.utils.preprocess import Preprocessor
from typing import List, Tuple


def _create_batches(data: pd.DataFrame, batch_size: int = 100000):
    batches = [data.index.values[i:i + batch_size] for i in range(0, data.shape[0], batch_size)]
    return batches


def _insert_dataframe(data: pd.DataFrame, mongo_config: dict, batch_size=100000) -> int:
    if len(data) == 0:
        return 0

    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        mongo_db = mongo_client[mongo_config["mongo_database"]]
        mongo_collection = mongo_db[mongo_config["mongo_collection"]]

        if "_id" in data.columns:
            existing_ids = mongo_collection.find({"_id": {"$in": data["_id"].values.tolist()}}, {"_id": 1})
            existing_ids = [_id["_id"] for _id in existing_ids]
            data = data[~data["_id"].isin(existing_ids)]
            if len(data) == 0:
                return 0
        indexes_batches = _create_batches(data, batch_size=batch_size)
        total_inserted = 0
        for indexes in indexes_batches:
            batch = data.loc[indexes]
            records = batch.to_dict("records")
            mongo_collection.insert_many(records)
            total_inserted += len(records)
            del records
            del batch
    return total_inserted


def create_indexes(mongo_config: dict, indexes: List[Tuple]):
    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        mongo_db = mongo_client[mongo_config["mongo_database"]]
        mongo_collection = mongo_db[mongo_config["mongo_collection"]]
        resp = mongo_collection.create_index(indexes)
        logging.info(f"Indexes created: {resp}")


def drop_mongo(mongo_config: dict):
    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        logging.info(f"Dropping {mongo_config['mongo_database']}/{mongo_config['mongo_collection']}")
        _db = mongo_client[mongo_config["mongo_database"]]
        _col = _db[mongo_config["mongo_collection"]]
        _col.drop()


def insert_lightcurves_to_mongo(filename: str, mongo_config: dict, batch_size: int = 10000):
    limit_epochs = {
        1: 5,
        2: 5,
        3: 1
    }
    preprocessor = Preprocessor(limit_epochs=limit_epochs)
    df = pd.read_parquet(filename, engine="pyarrow")
    try:
        df = preprocessor.run(df)
        if df is None or df.shape[0] == 0:
            logging.info(f"[PID {os.getpid()}] Inserted {0: >7} from {filename}")
            return 0

    except Exception as e:
        raise Exception(f"{filename} with problems: {e}")

    df.drop(columns=["catflags", "clrcoeff"], inplace=True)

    df["loc"] = df.apply(lambda x: {
        "loc": {
            "type": "Point",
            "coordinates": [x["objra"] - 180, x["objdec"]]
        }
    }, axis=1, result_type='expand')

    df.rename(columns={"objectid": "_id"}, inplace=True)
    for col in ["mag", "magerr", "hmjd"]:
        df[col] = df[col].map(lambda x: x.tobytes())
    inserted = _insert_dataframe(df, mongo_config, batch_size=batch_size)
    return inserted


def insert_features_to_mongo(file_path: str, mongo_config: dict, batch_size=10000):
    features = pd.read_parquet(file_path)
    features["_id"] = features.index
    inserted = _insert_dataframe(features, mongo_config, batch_size=batch_size)
    return inserted
