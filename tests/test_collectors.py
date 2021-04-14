import boto3
import pandas as pd

from moto import mock_s3
from unittest import TestCase, mock
from ztf_dr.collectors.downloader import DRDownloader

CHECKSUMS_TEST = pd.DataFrame({
    "field": ["0202", "0202", "0202"],
    "checksum": ["8719c8505aa2a7f47fc9b195e64aba02",
                 "88e06637b21e7aab94f178dcb911585a",
                 "f597201471d7e188f1786c606c9614cf"],
    "file": ["https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c10_q1_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c14_q3_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c16_q2_dr5.parquet"]
})


@mock_s3
class GetDataReleaseTest(TestCase):
    def setUp(self) -> None:
        self.data_release_example = "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/"
        self.checksums = "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5"
        self.bucket_name = "test_example"
        self.bucket = f"s3://{self.bucket_name}/DRX"

        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket=self.bucket_name)

    @mock.patch("ztf_dr.collectors.downloader.DRDownloader.get_checksums")
    #@mock.patch("ztf_dr.collectors.downloader.DRDownloader.in_s3_files")
    def test_run_download(self, get_checksums: mock.Mock):
        #in_s3_files.return_value = []
        self.dw = DRDownloader(self.data_release_example,
                               self.checksums,
                               self.bucket)
        #in_s3_files.assert_called()

        get_checksums.assert_called()
        get_checksums.return_value = CHECKSUMS_TEST

        self.dw.checksums = CHECKSUMS_TEST
        fields = self.dw.checksums.groupby(["field"])
        print("slkdjalkdsa")
        for r in fields:
            r = (r[0], r[1][["checksum", "file"]])
            self.dw.process(r)
