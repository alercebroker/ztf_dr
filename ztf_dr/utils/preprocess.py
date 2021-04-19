import boto3
import numpy as np
import pandas as pd
import re
import os

from multiprocessing import Pool
from tqdm import tqdm


class Preprocessor:
    def __init__(self,
                 limit_epochs: int = 20,
                 mag_error_tolerance: float = 1.0,
                 catflags_filter: int = 0):
        self.limit_epochs = limit_epochs
        self.mag_error_tolerance = mag_error_tolerance
        self.catflags_filter = catflags_filter

    def preprocess(self, dataframe: pd.Series) -> pd.Series:
        if dataframe["nepochs"] < self.limit_epochs:
            dataframe["flag"] = False
            return dataframe

        filter_error = dataframe["magerr"] < self.mag_error_tolerance
        filter_catflags = dataframe["catflags"] == self.catflags_filter

        filters = np.logical_and(filter_error, filter_catflags)
        n_epochs = filters.sum()

        if n_epochs < self.limit_epochs:
            dataframe["flag"] = False
            return dataframe

        dataframe["flag"] = True
        dataframe["catflags"] = dataframe["catflags"][filters]
        dataframe["clrcoeff"] = dataframe["clrcoeff"][filters]
        dataframe["hmjd"] = dataframe["hmjd"][filters]
        dataframe["mag"] = dataframe["mag"][filters]
        dataframe["magerr"] = dataframe["magerr"][filters]
        dataframe["nepochs"] = n_epochs
        return dataframe

    def run(self, dataframe: pd.DataFrame):
        filtered = dataframe.apply(self.preprocess, axis=1)
        filtered = filtered[filtered["flag"]]
        del filtered["flag"]
        return filtered

    def apply(self, input_path: str, output_path: str):
        dataframe = pd.read_parquet(input_path)
        filtered = self.run(dataframe)
        filtered.to_parquet(output_path)
        return

    def _apply(self, row):
        self.apply(row[0], row[1])

    def preprocess_bucket(self, bucket_name: str, prefix: str, output_bucket: str, n_cores=1):
        field_and_parquet_regex = r".*/(field.*\.parquet)"
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        files = [x.key for x in bucket.objects.filter(Prefix=prefix) if x.key.endswith(".parquet")]
        output = [re.findall(field_and_parquet_regex, x)[0] for x in files]

        data = pd.DataFrame({
            "input_file": [os.path.join("s3://", bucket_name, f) for f in files],
            "output_file": [os.path.join(output_bucket, o) for o in output]
        })

        if n_cores == 1:
            data.apply(lambda x: self.apply(x["input_file"], x["output_file"]), axis=1)

        elif n_cores > 1:
            pool = Pool(n_cores)
            for _ in tqdm(pool.imap_unordered(self._apply, data.values), total=len(data)):
                pass
        return data
