import boto3

from io import StringIO
from moto import mock_s3
from unittest import TestCase, mock
from ztf_dr.collectors.downloader import DRDownloader
from ztf_dr.collectors.crawler import *

CHECKSUMS_TEST = pd.DataFrame({
    "field": ["0202", "0202", "0202"],
    "checksum": ["8719c8505aa2a7f47fc9b195e64aba02",
                 "88e06637b21e7aab94f178dcb911585a",
                 "f597201471d7e188f1786c606c9614cf"],
    "file": ["https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c10_q1_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c14_q3_dr5.parquet",
             "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5//0/field0202/ztf_000202_zg_c16_q2_dr5.parquet"]
})

EXPECTED_REQUEST_RESPONSE = '''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /data/ZTF/lc_dr5/0/field0202</title>
 </head>
 <body>
<h1>Index of /data/ZTF/lc_dr5/0/field0202</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                             <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c10_q1_dr5.parquet">ztf_000202_zg_c10_q1_dr5.parquet</a> 2021-03-12 15:05  313K  
<img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c14_q3_dr5.parquet">ztf_000202_zg_c14_q3_dr5.parquet</a> 2021-03-12 15:05  476K  
<img src="/icons/unknown.gif" alt="[   ]"> <a href="ztf_000202_zg_c16_q2_dr5.parquet">ztf_000202_zg_c16_q2_dr5.parquet</a> 2021-03-12 15:05  468K  
<hr></pre>
</body></html>
'''


DF_EXAMPLE = """file,date,size
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c10_q1_dr5.parquet,2021-03-12 15:05,313K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c14_q3_dr5.parquet,2021-03-12 15:05,476K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0202/ztf_000202_zg_c16_q2_dr5.parquet,2021-03-12 15:05,468K  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q1_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q2_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q3_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c01_q4_dr5.parquet,2021-03-11 17:01,1.9M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q1_dr5.parquet,2021-03-11 17:01,2.0M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q2_dr5.parquet,2021-03-11 17:01,2.1M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q3_dr5.parquet,2021-03-11 17:01,2.2M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c02_q4_dr5.parquet,2021-03-11 17:01,2.4M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c03_q1_dr5.parquet,2021-03-11 17:01,2.3M  
https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0245/ztf_000245_zg_c03_q2_dr5.parquet,2021-03-11 17:01,2.2M  """


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
        get_checksums.return_value = CHECKSUMS_TEST

        self.dw.checksums = CHECKSUMS_TEST
        fields = self.dw.checksums.groupby(["field"])
        for r in fields:
            r = (r[0], r[1][["checksum", "file"]])
            self.dw.process(r)


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

    @mock.patch("ztf_dr.collectors.crawler.get_content", return_value=EXPECTED_REQUEST_RESPONSE)
    def test_get_data_release(self, request: mock.Mock):
        df = get_data_release("https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/")
        request.assert_called_once()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 3))

    def test_size_by_field(self):
        input_ = pd.read_csv(StringIO(DF_EXAMPLE))
        output = size_by_field(input_)
        self.assertIsInstance(output, pd.DataFrame)
        self.assertEqual(output.shape, (2, 2))