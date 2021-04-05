import hashlib
import pandas as pd
import re
import os
import wget
import logging


from tqdm import tqdm
from multiprocessing import Pool


def generate_md5_checksum(fname, chunksize=4096):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class DRDownloader:
    def __init__(self,
                 data_release_url,
                 checksum_path,
                 bucket,
                 output_folder="/tmp",
                 auto_clean=True):
        self.data_release_url = data_release_url
        self.checksum_path = checksum_path
        self.bucket = bucket
        self.output_folder = output_folder
        self.auto_clean = auto_clean
        self.checksums = None

        self.init_logging()
        self.get_checksums()

    def init_logging(self, loglevel="INFO"):
        numeric_level = getattr(logging, loglevel.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError("Invalid log level: %s" % loglevel)
        logging.basicConfig(level=numeric_level,
                            format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def get_checksums(self) -> pd.DataFrame:
        def find_field(string):
            found = re.findall(r".*(field[0-9]+).*", string)
            return found[0] if len(found) > 0 else None
        checksums = pd.read_csv(self.checksum_path,
                                delimiter="\s+",
                                names=["checksum", "file"],
                                squeeze=True)

        checksums["field"] = checksums["file"].map(lambda x: find_field(x))
        checksums["file"] = checksums['file'].map(lambda x: self.data_release_url + x[1:])
        self.checksums = checksums
        return checksums

    def download(self, local_path, link, checksum_reference):
        if os.path.exists(local_path):
            checksum = generate_md5_checksum(local_path)
            if checksum == checksum_reference:
                logging.info(f"File {link} already exists (correct checksum)")
                return

        wget.download(link, local_path)
        checksum = generate_md5_checksum(local_path)
        if checksum != checksum_reference:
            raise ValueError(
                'The MD5 checksum of local file %s differs from %s, please manually remove \
                 the file and try again.' %
                (local_path, checksum_reference))
        return

    def bulk_upload_s3(self, local_path, field_name) -> int:
        if not self.bucket:
            return 1
        bucket_dir = os.path.join(self.bucket, field_name)
        command = f"aws s3 sync {local_path} {bucket_dir} > /dev/null"
        return os.system(command)

    def process(self, data):
        field = data[0]
        rows = data[1]

        field_path = os.path.join(self.output_folder, field)

        if not os.path.exists(field_path):
            os.makedirs(field_path)

        for index, row in rows.iterrows():
            checksum_reference = row[0]
            link = row[1]
            parquet = link.split("/")[-1]
            parquet_path = os.path.join(field_path, parquet)
            self.download(parquet_path, link, checksum_reference)

        if self.bucket:
            self.bulk_upload_s3(field_path, field)
        os.system(f"rm -rf {field_path}")
        return

    def run(self, n_proc=10):
        pool = Pool(n_proc)
        fields = self.checksums.groupby(["field"])
        for _ in tqdm(pool.imap_unordered(self.process, fields),
                      total=len(fields)):
            pass
