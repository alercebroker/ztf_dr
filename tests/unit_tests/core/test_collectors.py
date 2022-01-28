import boto3

from data import collector

from io import StringIO
from moto import mock_s3
from unittest import TestCase, mock
from ztf_dr.collectors.downloader import DRDownloader
from ztf_dr.collectors.crawler import *


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
    def test_run_download(self, get_checksums: mock.Mock):
        self.dw = DRDownloader(self.data_release_example,
                               self.checksums,
                               self.bucket)

        get_checksums.assert_called()
        get_checksums.return_value = collector.CHECKSUMS_TEST

        fields = self.dw.checksums.groupby(["field"])
        for r in fields:
            field, row = (r[0], r[1][["checksum", "file"]])
            self.dw.process(field, row)


class CrawlerTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_parse_size(self):
        case = "1M"
        response = 10**6
        test = parse_size(case)
        self.assertEqual(response, test)

        bad_response = 10**2
        self.assertNotEqual(bad_response, test)

    def test_is_folder(self):
        file = "file.parquet"
        folder = "dir/"

        self.assertTrue(is_folder(folder))
        self.assertFalse(is_folder(file))

    @mock.patch("ztf_dr.collectors.crawler.get_content", return_value=collector.EXPECTED_REQUEST_RESPONSE)
    def test_get_data_release(self, request: mock.Mock):
        df = get_data_release("https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/")
        request.assert_called_once()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 3))

    def test_size_by_field(self):
        input_ = pd.read_csv(StringIO(collector.DF_EXAMPLE))
        output = size_by_field(input_)
        self.assertIsInstance(output, pd.DataFrame)
        self.assertEqual(output.shape, (2, 2))
