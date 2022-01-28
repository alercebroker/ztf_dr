import boto3
import os
import pandas as pd
import types
import random

from moto import mock_s3
import unittest.mock
from unittest import TestCase
from ztf_dr.utils.preprocess import Preprocessor
from ztf_dr.utils.jobs import chunker_list, run, run_jobs
from ztf_dr.utils.s3 import s3_uri_bucket, get_s3_path_to_files, s3_filename_difference
from ztf_dr.utils.parse_parquet import parse_parquets
from ztf_dr import utils


def fake_process(func, y, args=None):
    class FakeResponse:
        def __init__(self, a):
            self.a = a

        def get(self):
            return self.a
    pid, fun, args = args[0], args[1], args[2]
    response = y(pid, fun, args)
    return FakeResponse(response)


@mock_s3
class PreprocessTest(TestCase):
    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "data/DR5_field_example.parquet")
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")
        self.init_bucket()

    def init_bucket(self):
        with open(self.parquet_path, "rb") as file:
            self.client.upload_fileobj(file, "test_bucket", "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

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
        preprocessor.preprocess_bucket("s3://test_bucket/drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet",
                                       "s3://test_bucket/drx/preprocessed.parquet",
                                       n_cores=1)
        bucket, path = s3_uri_bucket("s3://test_bucket/drx/preprocessed.parquet")
        files = get_s3_path_to_files(bucket, path)
        self.assertEqual(len(files), 1)

    def test_split_list(self):
        input_list = list(range(0, 100))
        split_list = utils.split_list(input_list, 20)
        self.assertEqual(len(split_list), 20)


@mock_s3
class S3Test(TestCase):
    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "data/DR5_field_example.parquet")
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")
        self.init_bucket()

    def init_bucket(self):
        with open(self.parquet_path, "rb") as file:
            self.client.upload_fileobj(file, "test_bucket", "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

    def test_s3_uri_bucket(self):
        bucket, path = s3_uri_bucket("s3://test_bucket/drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")
        self.assertEqual(bucket, "test_bucket")
        self.assertEqual(path, "drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")

        with self.assertRaises(Exception) as context:
            s3_uri_bucket("http://test_bucket/drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")
        self.assertIsInstance(context.exception, Exception)

    def test_get_s3_path_to_files(self):
        bucket, path = s3_uri_bucket("s3://test_bucket/drx/field0202/ztf_000202_zg_c10_q1_dr5.parquet")
        files = get_s3_path_to_files(bucket, path)
        self.assertEqual(len(files), 1)

    def test_s3_filename_difference(self):
        a = ["aaa/a", "aaa/b", "aaa/c"]
        b = ["aaa/a"]
        c = s3_filename_difference(a, b)
        self.assertListEqual(c, ["aaa/b", "aaa/c"])

        d = s3_filename_difference(b, a)
        self.assertListEqual(d, [])

    @unittest.mock.patch('multiprocessing.pool.Pool.apply_async', new=fake_process)
    def test_parse_parquet(self):
        parse_parquets("s3://test_bucket/drx/field0202/",
                       "s3://test_bucket/drx/parsed/")
        files = get_s3_path_to_files("test_bucket", "drx/parsed/")
        self.assertEqual(len(files), 1)


class JobsTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_chunker_split(self):
        for chunks in range(1, 5):
            my_list = range(0, 100)
            my_splitted_list = chunker_list(my_list, chunks)
            output = [x for x in my_splitted_list]
            self.assertIsInstance(my_splitted_list, types.GeneratorType)
            self.assertEqual(len(output), chunks)

    def test_run(self):
        arguments = [(random.randint(0, 100), random.randint(0, 100)) for _ in range(100)]
        function = lambda x, y: x + y
        pid = 1313

        response = run(pid, function, arguments)
        self.assertEqual(len(response), 100)
        self.assertIsInstance(response, list)

    @unittest.mock.patch('multiprocessing.pool.Pool.apply_async', new=fake_process)
    def test_run_jobs(self):
        arguments = [(random.randint(0, 100), random.randint(0, 100)) for _ in range(100)]
        function = lambda x, y: x + y
        process = 2

        response = run_jobs(arguments, function, num_processes=process)
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 100)
