import boto3
import os

from ztf_dr.utils.post_processing import get_objects_table, get_objects
from moto import mock_s3
from unittest import TestCase, mock


@mock_s3
class UtilsTest(TestCase):
    def init_bucket(self):
        file_path = os.path.dirname(__file__)
        parquet_path = os.path.join(file_path, "../data/DR5_field_example.parquet")
        with open(parquet_path, "rb") as file:
            self.client.upload_fileobj(file, "test_bucket", "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

    def setUp(self) -> None:
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")
        self.init_bucket()
        pass

    @mock.patch("ztf_dr.utils.post_processing.get_objects")
    def test_get_objects_table(self, objects: mock.Mock):
        get_objects_table("test_bucket", "drx", "drx/field", "drx/objects", "s3://test_bucket/drx/objects")
        objects.assert_called()

    def test_get_objects(self):
        get_objects("s3://test_bucket/drx/field0202/", "s3://test_bucket/dr5/o/0202")
