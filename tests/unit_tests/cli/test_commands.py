import boto3
import mongomock
import os

from unittest import mock, TestCase
from data import collector
from click.testing import CliRunner
from moto import mock_s3
from ztf_dr.main import download_data_release, compute_features


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
class CliTest(TestCase):
    def setUp(self) -> None:
        self.data_release_example = "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/"
        self.checksums = "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5"
        self.bucket_name = "test_example"
        self.bucket_uri = f"s3://{self.bucket_name}/drx"
        self.fake_fields = 5
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket=self.bucket_name)
        file_path = os.path.dirname(__file__)
        self.parquet_path = os.path.join(file_path, "data/some_lc.parquet")
        self.init_bucket()

    def init_bucket(self):
        for i in range(self.fake_fields):
            with open(self.parquet_path, "rb") as file:
                self.client.upload_fileobj(file, self.bucket_name, f"drx/field020{i}/ztf_00020{i}_zg_c10_q1_dr5.parquet")

    @mock.patch("ztf_dr.collectors.downloader.DRDownloader.get_checksums",
                mock.MagicMock(return_value=collector.CHECKSUMS_TEST))
    @mock.patch('multiprocessing.pool.Pool.apply_async', new=fake_process)
    def test_download_data_release(self):
        runner = CliRunner()
        result = runner.invoke(download_data_release, [self.data_release_example,
                                                       self.checksums,
                                                       self.bucket_uri])
        self.assertEqual(result.exit_code, 0)

    def test_compute_features(self):
        # Run one job for one partition (partition 0).
        runner = CliRunner()
        result = runner.invoke(compute_features, [self.bucket_uri,  # input data
                                                  f"s3://{self.bucket_name}/features",  # output folder
                                                  "0",  # partition
                                                  "-t", f"{self.fake_fields}"  # number of partitions
                                                  ])
        self.assertEqual(result.exit_code, 0)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('test_example')
        features = [f for f in bucket.objects.filter(Prefix="features")]
        self.assertEqual(len(features), 1)
