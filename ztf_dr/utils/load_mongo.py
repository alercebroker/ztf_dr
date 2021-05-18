import boto3
import pandas as pd
import pymongo
import os

from six.moves.urllib import parse
from ztf_dr.utils.preprocess import Preprocessor
import glob
import time
import json
import gzip


def insert_data(s3_url_bucket: str,
                mongo_host: str = "localhost",
                mongo_port: int = 27017,
                mongo_database: str = "default",
                mongo_col: str = "objects",
                batch_size: int = 10000):
    mongo_client = pymongo.MongoClient(host=mongo_host,
                                       port=mongo_port)

    db = mongo_client[mongo_database]
    col = db[mongo_col]
    preprocessor = Preprocessor()

    url_parse = parse.urlparse(s3_url_bucket)
    bucket_name = url_parse.netloc
    key_prefix = url_parse.path[1:]

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = sorted([x.key for x in bucket.objects.filter(Prefix=key_prefix) if x.key.endswith(".parquet")])

    for f in files:
        input_file = os.path.join("s3://", bucket_name, f)
        df = pd.read_parquet(input_file)
        df = preprocessor.run(df)
        if len(df) == 0:
            continue

        del df['catflags']
        del df['clrcoeff']

        records = list(df.T.to_dict().values())

        for i in range(len(records)):
            records[i]["hmjd"] = records[i]["hmjd"].tolist()
            records[i]["mag"] = records[i]["mag"].tolist()
            records[i]["magerr"] = records[i]["magerr"].tolist()
        col.insert_many(records)
        break


if __name__ == "__main__":
    print("Don't use me :(")
    insert_data("s3://ztf-data-releases/dr5/raw/")