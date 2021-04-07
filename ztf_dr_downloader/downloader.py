import boto3
import hashlib
import pandas as pd
import re
import os
import wget
import logging


from tqdm import tqdm
from multiprocessing import Pool


def generate_md5_checksum(fname: str, chunksize=4096):
    """
    Generate md5 checksum with specific checksum.

    :param fname: Name of file to get checksum.
    :param chunksize: Size to read for each iteration
    :return: Checksum in hex
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def field_stats(path):
    """
    Get total files and total size of a specific field.

    :param path: Path in local machine that point to field
    :return: Tuple of 2 positions. [0] -> count of files, [1] -> total size in bytes
    """
    total_size = 0
    files = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for i in filenames:
            f = os.path.join(dirpath, i)
            total_size += os.path.getsize(f)
            files += 1
    return files, total_size


class DRDownloader:
    """
    A class used to get, download and upload Data Releases of ZTF.

    ...

    Attributes:
        logger: specific logger of class
        data_release_url: valid url of data release version
        checksum_path: url/path direction of checksums
        bucket: S3 bucket to save data release files
        output_folder: target folder to contain temps files
        checksums: dataframe that contain parquet urls and checksums
        uploaded_files: list files that are contained in S3 bucket.

    """
    def __init__(self,
                 data_release_url,
                 checksum_path,
                 bucket,
                 output_folder="/tmp"):
        self.logger = self.init_logging()
        self.data_release_url = data_release_url
        self.checksum_path = checksum_path
        self.bucket = bucket
        self.output_folder = output_folder
        self.checksums = None
        self.uploaded_files = self.in_s3_files()

        self.get_checksums()

    def in_s3_files(self) -> list:
        """
        Get the list of parquet files in S3 bucket.
        :return: List of files in S3
        """
        pattern = r"s3://([\w'-]+)/([\w'-]+).*"
        data = re.findall(pattern, self.bucket)
        if len(data) != 1:
            raise ValueError("Put a correct format path: s3://<bucket-name>/<dr-folder>")
        data = data[0]
        bucket_name, data_release = data[0], data[1]
        self.logger.info(f"Finding existing parquets in {bucket_name} of {data_release}")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        files = [x.key.split("/")[-1] for x in bucket.objects.filter(Prefix=data_release)]
        self.logger.info(f"Found {len(files)} parquets in {self.bucket}")
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
        checksums["file"] = checksums['file'].map(lambda x: self.data_release_url + x[2:])
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
        if not self.bucket:
            return 1
        bucket_dir = os.path.join(self.bucket, field_name)
        command = f"aws s3 sync {local_path} {bucket_dir} > /dev/null"
        files, size = field_stats(local_path)
        self.logger.info(f"Uploading {local_path} ({files} files, {size/1000000}MB)")
        return os.system(command)

    def process(self, data) -> None:
        """
        Basic method to process one field of data release, download all parquets and finish uploading all files to S3.
        After that remove all temp files.

        :param data: Data of one field
        :return:
        """
        field = data[0]
        rows = data[1]

        field_path = os.path.join(self.output_folder, field)

        if not os.path.exists(field_path):
            os.makedirs(field_path)

        self.logger.info(f"Downloading field: {field} ({len(rows)} parquets)")
        for index, row in rows.iterrows():
            checksum_reference = row[0]
            link = row[1]
            parquet = link.split("/")[-1]
            parquet_path = os.path.join(field_path, parquet)

            if parquet in self.uploaded_files:
                self.logger.info(f"Already exists {parquet} in {self.bucket}")
            else:
                self.download(parquet_path, link, checksum_reference)

        if self.bucket:
            self.bulk_upload_s3(field_path, field)
        os.system(f"rm -rf {field_path}")
        return

    def run(self, n_proc=10) -> None:
        """
        Method to start massive parallel download with specific number of process.
        :param n_proc: Number of process to execute the routine.
        :return:
        """
        pool = Pool(n_proc)
        fields = self.checksums.groupby(["field"])
        for _ in tqdm(pool.imap_unordered(self.process, fields),
                      total=len(fields)):
            pass
        return
