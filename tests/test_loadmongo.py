import boto3
import mongomock

import os

from moto import mock_s3
from unittest import TestCase, mock

from ztf_dr.utils.load_mongo import init_mongo, drop_mongo, insert_data


@mock_s3
class UtilsTest(TestCase):
    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "../data/DR5_field_example.parquet")
        self.bucket_name = "test_bucket"
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket=self.bucket_name)
        self.init_bucket()

        self.mongo_uri = "test.mongo.com"
        self.mongo_database = "test"
        self.mongo_collection = "release"

    def init_bucket(self):
        with open(self.parquet_path, "rb") as file:
            self.client.upload_fileobj(file, self.bucket_name, "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_mongo_drop_and_init(self):
        drop_mongo(self.mongo_uri, self.mongo_database, self.mongo_collection)
        init_mongo(self.mongo_uri, self.mongo_database, self.mongo_collection)

    @mongomock.patch(servers=(('test.mongo.com', 27017),))
    def test_insert_data(self):
        s3_uri = f"s3://{self.bucket_name}/drx"
        print(self.client.list_objects(Bucket=self.bucket_name))
        insert_data(s3_uri, self.mongo_uri, self.mongo_database, self.mongo_collection)
