import logging
import pandas as pd
import pymongo

from typing import List, Tuple


def _create_batches(data: pd.DataFrame, batch_size: int = 100000):
    batches = [data.index.values[i:i + batch_size] for i in range(0, data.shape[0], batch_size)]
    return batches


def insert_dataframe(data: pd.DataFrame, mongo_config: dict, batch_size=100000) -> int:
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


def insert_features(file_path, mongo_config, batch_size=10000):
    features = pd.read_parquet(file_path)
    features["_id"] = features.index
    inserted = insert_dataframe(features, mongo_config, batch_size=batch_size)
    return inserted


def create_indexes(mongo_config: dict, indexes: List[Tuple]):
    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        mongo_db = mongo_client[mongo_config["mongo_database"]]
        mongo_collection = mongo_db[mongo_config["mongo_collection"]]
        resp = mongo_collection.create_index(indexes)
        logging.info(f"Indexes created: {resp}")


def drop_mongo(mongo_config):
    logger = logging.getLogger("load_mongo")
    with pymongo.MongoClient(mongo_config["mongo_uri"]) as mongo_client:
        logger.info(f"Dropping {mongo_config['mongo_database']}/{mongo_config['mongo_collection']}")
        _db = mongo_client[mongo_config["mongo_database"]]
        _col = _db[mongo_config["mongo_collection"]]
        _col.drop()
