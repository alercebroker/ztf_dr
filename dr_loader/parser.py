import hashlib
import numpy as np
import pandas as pd
import re
import requests
import os
import tarfile
import wget

from io import StringIO
from tqdm import tqdm
from multiprocessing import Pool

REGEX_STR = r'<img.*> <a href="(field.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)\n'
UNITS = {"B": 1, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}


# TODO: With regex
def parse_size(size: str) -> int:
    size = size.strip()
    unit = size[-1]
    value = float(size[:-1])
    return int(value * UNITS[unit])


def generate_md5_checksum(fname, chunksize=4096):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class Parser:
    def __init__(self,
                 checksum_path: str,
                 data_release_url: str,
                 sort=True,
                 bucket=None):
        self.checksums_path = checksum_path
        self.data_release_url = data_release_url
        self.columns = ["ObjectID", "nDR4epochs", "FilterID", "FieldID", "RcID", "ObjRA", "ObjDec", "HMJD", "mag", "magerr", "clrcoeff", "catflags"]
        self.bucket = bucket

        self.checksums = None
        self.data_release = None

        self.get_checksums()
        self.get_data_release(sort)

    def get_checksums(self) -> pd.DataFrame:
        checksums = pd.read_csv(self.checksums_path, delimiter="\s+", names=["checksum", "file"], index_col="file",
                                squeeze=True)
        self.checksums = checksums
        return checksums

    def get_data_release(self, sort=False) -> np.ndarray:
        response = requests.get(self.data_release_url).content.decode("utf-8")
        files = re.findall(REGEX_STR, response)
        df = pd.DataFrame(files, columns=["file", "date", "size"])
        df = df.join(self.checksums, on="file")
        df["size"] = df["size"].apply(lambda x: parse_size(x))
        if sort:
            df.sort_values(by=['size'], inplace=True)
        self.data_release = df.values
        return df

    def download_file(self, local_path, link, checksum_reference=None):
        if not link.endswith(".tar.gz"):
            raise ValueError(f"The file {link} to download is not a tar.gz file.")
        if not os.path.exists(local_path):
            wget.download(link, local_path)
        if checksum_reference is not None:
            checksum = generate_md5_checksum(local_path)
            if checksum != checksum_reference:
                raise ValueError(
                    'The MD5 checksum of local file %s differs from %s, please manually remove \
                     the file and try again.' %
                    (local_path, checksum_reference))
        return local_path

    def to_parquet(self, text, output):
        data = pd.read_csv(StringIO(text), header=None, names=self.columns)
        data.to_parquet(output, engine="pyarrow")
        return

    def parse(self, local_path, output_path=".", objects=100000):
        field_name = local_path.split("/")[-1][:-4]
        output_folder = os.path.join(output_path, field_name)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        header = ""
        content = ""
        count = 0
        partition = 0
        with open(local_path) as input_field:
            for line in input_field:
                if "#" in line:
                    line = line.strip().split()
                    header = ",".join(line)[2:]
                    count += 1
                else:
                    line = line.strip().split()
                    line = ",".join(line)
                    line = f"{header},{line}\n"
                    content += line

                if count == objects:
                    self.to_parquet(content, os.path.join(output_folder, f"{partition}.parquet"))
                    content = ""
                    partition += 1
                    count = 0
            if content != "":
                self.to_parquet(content, os.path.join(output_folder, f"{partition}.parquet"))
        return output_folder

    def bulk_upload_s3(self, field_dir, field_name):
        bucket_dir = os.path.join(self.bucket, field_name)
        command = f"aws s3 sync {field_dir} {bucket_dir}"
        return os.system(command)


    def process_tarfile(self, row, output_path="/tmp"):
        """
            (1) Download tar.gz file
            (2) Untar file
            (3) Remove tar.file
            (4) Process untar file, get parquets
            (5) Upload parquets file
            (6) Remove untar file
        :param row:
        :param output_path:
        :return:
        """
        file = row[0]
        checksum = row[3]
        field_name = file[:-7]
        output_path = os.path.join(output_path, file)
        download_url = os.path.join(self.data_release_url, file)
        self.download_file(output_path, download_url, checksum)  # (1)

        untar_path = output_path[:-7]
        tar = tarfile.open(output_path, "r:gz")
        tar.extractall(untar_path)  # (2)

        txt_path = os.path.join(untar_path, f"{field_name}.txt")
        if os.path.isfile(output_path):
            os.remove(output_path)  # (3)
        field_dir = self.parse(txt_path, output_path="/tmp")  # (4)

        if os.path.isfile(txt_path):
            os.remove(txt_path)  # (3)
        if self.bucket:
            self.bulk_upload_s3(field_dir, field_name)  # (6)
            os.system(f"rm -rf {field_dir}")
        return

    def process_tarfile_parallel(self, n_proc=10):
        pool = Pool(n_proc)
        for _ in tqdm(pool.imap_unordered(self.process_tarfile, self.data_release), total=len(self.data_release)):
            pass


if __name__ == "__main__":
    dw = Parser("https://irsa.ipac.caltech.edu/data/ZTF/lc_dr4/checksums.md5",
                "https://irsa.ipac.caltech.edu/data/ZTF/lc_dr4/",
                bucket="s3://ztf-data-releases/dr4/parquet")

    dw.data_release = dw.data_release
    dw.process_tarfile_parallel(n_proc=5)
