import mongomock
import os

import pandas as pd
from moto import mock_s3
from unittest import TestCase
from ztf_dr.db.mongo import drop_mongo, create_indexes, insert_lightcurves_to_mongo, insert_features_to_mongo


@mock_s3
class UtilsTest(TestCase):
    def setUp(self) -> None:
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "data/DR5_field_example.parquet")

        self.mongo_uri = "test.mongo.com"
        self.mongo_database = "test"
        self.mongo_collection = "release"

        self.config = {
            "mongo_uri": self.mongo_uri,
            "mongo_database": self.mongo_database
        }

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_mongo_drop_and_create_indexes(self):
        config = self.config.copy()
        config["mongo_collection"] = "test"
        drop_mongo(config)
        indexes = [("loc", "2dsphere"), ("fieldid", 1), ("filterid", 1)]
        create_indexes(config, indexes)

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_insert_lightcurves(self):
        config = self.config.copy()
        config["mongo_collection"] = "test"
        insert_lightcurves_to_mongo(self.parquet_path, config)

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_insert_lightcurves(self):
        config = self.config.copy()
        config["mongo_collection"] = "test"
        inserted = insert_lightcurves_to_mongo(self.parquet_path, config)
        self.assertEqual(1327, inserted)

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_insert_features(self):
        config = self.config.copy()
        config["mongo_collection"] = "test"
        tmp_path = "/tmp/datatmp.parquet"
        data = pd.DataFrame({"feature_1": [1, 2, 3], "feature_2": [4, 5, 6]})
        data.to_parquet(tmp_path)
        inserted = insert_features_to_mongo(tmp_path, config)
        self.assertEqual(inserted, len(data))
