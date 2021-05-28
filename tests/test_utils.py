import boto3
import os

import pandas as pd

from moto import mock_s3
from unittest import TestCase, mock
from ztf_dr.utils.post_processing import get_objects_table, get_objects
from ztf_dr.utils.preprocess import Preprocessor
from ztf_dr import utils


@mock_s3
class UtilsTest(TestCase):
    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "../data/DR5_field_example.parquet")
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")
        self.init_bucket()
        pass

    def init_bucket(self):
        with open(self.parquet_path, "rb") as file:
            self.client.upload_fileobj(file, "test_bucket", "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

    @mock.patch("ztf_dr.utils.post_processing.get_objects")
    def test_get_objects_table(self, objects: mock.Mock):
        get_objects_table("test_bucket", "drx", "drx/field", "drx/objects", "s3://test_bucket/drx/objects")
        objects.assert_called()

    def test_get_objects(self):
        get_objects("s3://test_bucket/drx/field0202/", "s3://test_bucket/dr5/o/0202")

    def test_preprocess(self):
        preprocessor = Preprocessor()
        data = pd.read_parquet(self.parquet_path)

        # Test only one row
        one_row = preprocessor.preprocess(data.iloc[0])
        self.assertIsInstance(one_row, pd.Series)

        # Test with all bad data: haven't conditions to pass preprocess
        all_data = preprocessor.run(data)
        self.assertIsInstance(all_data, pd.DataFrame)
        self.assertEqual(all_data.shape, (654, 12))

        # Test apply
        preprocessor.apply(self.parquet_path, "/tmp/example.parquet")

        # Test _apply
        preprocessor._apply([self.parquet_path, "/tmp/example.parquet"])

    def test_preprocess_bucket(self):
        preprocessor = Preprocessor()
        preprocessor.preprocess_bucket("test_bucket",
                                       "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet",
                                       "s3://test_bucket/drx/preprocessed.parquet", n_cores=1)

    def test_split_list(self):
        input_list = list(range(0, 100))
        split_list = utils.split_list(input_list, 20)
        self.assertEqual(len(split_list), 20)

    def test_existing_in_bucket(self):
        bucket_path = "s3://test_bucket/drx/"
        in_bucket = utils.existing_in_bucket(bucket_path)
        self.assertIsInstance(in_bucket, list)
        self.assertEqual(1, len(in_bucket))
