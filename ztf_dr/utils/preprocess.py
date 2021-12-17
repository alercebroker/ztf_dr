import boto3
import dask.dataframe as dd
import numpy as np
import pandas as pd
import re
import os

from multiprocessing import Pool
from tqdm import tqdm
from ztf_dr.utils.s3 import s3_uri_bucket, get_s3_path_to_files


class Preprocessor:
    def __init__(self,
                 limit_epochs: dict or int = 20,
                 mag_error_tolerance: float = 1.0,
                 catflags_filter: int = 0):
        self.limit_epochs = limit_epochs
        self.mag_error_tolerance = mag_error_tolerance
        self.catflags_filter = catflags_filter

    def _create_nepochs_query(self):
        query_string = lambda f, n: f"(filterid == {f} and nepochs >= {n})"
        query = [query_string(k, v) for k, v in self.limit_epochs.items()]
        query = ' or '.join(query)
        return query

    def discard_by_nepochs(self, dataframe: pd.DataFrame):
        if isinstance(self.limit_epochs, int):
            mask = dataframe["nepochs"] >= self.limit_epochs
            return dataframe[mask]
        elif isinstance(self.limit_epochs, dict):
            return dataframe.query(self._create_nepochs_query())
        else:
            raise Exception(f"Fatal error, {self.limit_epochs} must be an integer that indicates min. nepochs or dict "
                            f"that indicates filterid (key) and nepochs (value)")

    def preprocess(self, series: pd.Series) -> pd.Series:
        filter_error = series["magerr"] <= self.mag_error_tolerance
        filter_catflags = series["catflags"] == self.catflags_filter
        filters = np.logical_and(filter_error, filter_catflags)
        n_epochs = filters.sum()
        if not isinstance(self.limit_epochs, dict) and not (isinstance(self.limit_epochs, int)):
            raise Exception(f"Fatal error, {self.limit_epochs} must be an integer that indicates min. nepochs or dict "
                            f"that indicates filterid (key) and nepochs (value)")
        elif isinstance(self.limit_epochs, int) and n_epochs < self.limit_epochs:
            series["flag"] = False
            return series
        elif isinstance(self.limit_epochs, dict) and n_epochs < self.limit_epochs[series["filterid"]]:
            series["flag"] = False
            return series
        series["flag"] = True
        series["catflags"] = series["catflags"][filters]
        series["clrcoeff"] = series["clrcoeff"][filters]
        series["hmjd"] = series["hmjd"][filters]
        series["mag"] = series["mag"][filters]
        series["magerr"] = series["magerr"][filters]
        series["nepochs"] = n_epochs
        return series

    def run(self, dataframe: pd.DataFrame):
        dataframe = self.discard_by_nepochs(dataframe)
        if isinstance(dataframe, dd.DataFrame):
            dataframe = dataframe.compute()

        if len(dataframe) == 0:
            return None

        dataframe = dataframe.apply(self.preprocess, axis=1)

        if len(dataframe) == 0:
            return None

        dataframe = dataframe[dataframe["flag"]]
        del dataframe["flag"]
        return dataframe

    def apply(self, input_path: str, output_path: str):
        dataframe = pd.read_parquet(input_path)
        filtered = self.run(dataframe)
        if filtered is not None:
            filtered.to_parquet(output_path)
        return

    def _apply(self, row):
        self.apply(row[0], row[1])

    def preprocess_bucket(self, s3_uri_input, s3_uri_output: str, n_cores=1):
        bucket_name_input, path_input = s3_uri_bucket(s3_uri_input)
        bucket_name_output, path_output = s3_uri_bucket(s3_uri_output)

        files = get_s3_path_to_files(bucket_name_input, path_input)
        output_join_path = lambda f: os.path.join("s3://", bucket_name_output, path_output, "/".join(f.split("/")[-2:]))

        data = pd.DataFrame({
            "input_file": files,
            "output_file": [output_join_path(f) for f in files]
        })

        existing_files = get_s3_path_to_files(bucket_name_output, path_output)
        data = data[~data["output_file"].isin(existing_files)]
        if n_cores == 1:
            data.apply(lambda x: self.apply(x["input_file"], x["output_file"]), axis=1)

        elif n_cores > 1:
            pool = Pool(n_cores)
            for _ in tqdm(pool.imap_unordered(self._apply, data.values), total=len(data)):
                pass
        return data
