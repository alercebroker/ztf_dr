import boto3
import hashlib
import pandas as pd
import re
import os
import wget
import logging

from ztf_dr.utils.s3 import s3_uri_bucket
from ztf_dr.utils.jobs import run_jobs


def generate_md5_checksum(filename: str, chunk_size: int = 4096):
    """
    Generate md5 checksum with specific checksum.

    :param filename: Name of file to get checksum.
    :param chunk_size: Size to read for each iteration
    :return: Checksum in hex
    """
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class DRDownloader:
    """
    A class used to get, download and upload Data Releases of ZTF.

    ...

    Attributes:
        logger: specific logger of class.
        data_release_url: valid url of data release version.
        checksum_path: url/path direction of checksums.
        s3_uri: S3 bucket to save data release files.
        checksums: dataframe that contain parquet urls and checksums.

    """
    def __init__(self,
                 data_release_url: str,
                 checksum_path: str,
                 s3_uri: str,
                 output_folder="/tmp"):
        self.logger = self.init_logging()
        self.data_release_url = data_release_url
        self.checksum_path = checksum_path
        self.output_folder = output_folder
        self.bucket_name, self.path = s3_uri_bucket(s3_uri)
        self.checksums = None
        self.uploaded_files = self.in_s3_files()
        self.get_checksums()

    def in_s3_files(self) -> list:
        """
        Get the list of parquet files in S3 bucket.
        :return: List of files in S3
        """
        self.logger.info(f"Finding existing parquets in {self.bucket_name} of {self.path}")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        files = [x.key.split("/")[-1] for x in bucket.objects.filter(Prefix=self.path)]
        self.logger.info(f"Found {len(files)} parquets in {self.bucket_name}")
        return files

    def init_logging(self, loglevel="INFO"):
        """
        Init logging format of class

        :param loglevel: numeric representation of log
        :return: logger
        """
        numeric_level = getattr(logging, loglevel.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError("Invalid log level: %s" % loglevel)

        logger = logging.getLogger(__name__)
        logger.setLevel(numeric_level)

        logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

        file = logging.FileHandler("downloader.log")
        logger.addHandler(file)
        return logger

    def get_checksums(self) -> pd.DataFrame:
        """
        Load checksums of all parquets in memory. Also generate urls to download and add a column of field of parquet.

        :return: Dataframe that contain data release information
        """
        def find_field(string):
            found = re.findall(r".*(field[0-9]+).*", string)
            return found[0] if len(found) > 0 else None
        checksums = pd.read_csv(self.checksum_path,
                                delimiter="\s+",
                                names=["checksum", "file"],
                                squeeze=True)

        checksums["field"] = checksums["file"].map(lambda x: find_field(x))
        checksums["file"] = checksums['file'].map(lambda x: os.path.join(self.data_release_url, x[2:]))
        self.checksums = checksums
        return checksums

    def download(self,
                 local_path: str,
                 link: str,
                 checksum_reference: str) -> None:
        """
        Download one parquet, verify if checksum of downloaded file is the same of reference checksum.

        :param local_path: Target folder to download file
        :param link: File to download
        :param checksum_reference: Theoretical checksum of file
        :return:
        """
        if os.path.exists(local_path):
            checksum = generate_md5_checksum(local_path)
            if checksum == checksum_reference:
                self.logger.info(f"File {link} already exists (correct checksum)")
                return

        try:
            wget.download(link, local_path, bar=None)

        except Exception as e:
            self.logger.info(f"Conflict with {link}: {e}")

        checksum = generate_md5_checksum(local_path)
        if checksum != checksum_reference:
            raise ValueError(
                'The MD5 checksum of local file %s differs from %s, please manually remove \
                 the file and try again.' %
                (local_path, checksum_reference))
        return

    def bulk_upload_s3(self,
                       local_path: str,
                       field_name: str) -> int:
        """
        Upload a specific folder to S3 bucket.

        :param local_path: Folder to upload to S3
        :param field_name: Name of field to upload
        :return: 1 if is impossible to upload and 0 if there were no errors
        """
        if not self.bucket_name:
            return 1
        bucket_dir = os.path.join("s3://", self.bucket_name, self.path, field_name)
        command = f"aws s3 sync {local_path} {bucket_dir} > /dev/null"
        return os.system(command)

    def process(self, field: str, rows: pd.DataFrame) -> None:
        """
        Basic method to process one field of data release, download all parquets and finish uploading all files to S3.
        After that remove all temp files.

        :param field: Data of one field
        :param rows:
        :return:
        """
        field_path = os.path.join(self.output_folder, field)
        if not os.path.exists(field_path):
            os.makedirs(field_path)

        for index, row in rows.iterrows():
            checksum_reference = row[0]
            link = row[1]
            parquet = link.split("/")[-1]
            parquet_path = os.path.join(field_path, parquet)

            if parquet not in self.uploaded_files:
                self.download(parquet_path, link, checksum_reference)

        if self.bucket_name:
            self.bulk_upload_s3(field_path, field)
        os.system(f"rm -rf {field_path}")
        return

    def run(self, num_processes: int = 10) -> None:
        """
        Method to start massive parallel download with specific number of process.
        :param num_processes: Number of process to execute the routine.
        :return:
        """
        fields = self.checksums.groupby(["field"])
        fields = [f for f in fields]
        run_jobs(fields, self.process, num_processes=num_processes)
        return
