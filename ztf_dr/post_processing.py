import os

import boto3
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa

from dask.diagnostics import ProgressBar
from lc_classifier.features import CustomHierarchicalExtractor

OBJECT_FIELDS = {
    'objectid': pa.int64(),
    'filterid': pa.int32(),
    'fieldid': pa.int32(),
    'rcid': pa.int32(),
    'objra': pa.float64(),
    'objdec': pa.float64(),
    'nepochs': pa.int32()
}

OBJECT_SCHEMA = pa.schema(OBJECT_FIELDS)

OBJECT_COLUMNS = list(OBJECT_FIELDS.keys())


def get_objects(field_path: str,
                output_path: str) -> None:
    field_path = os.path.join(field_path, "*.parquet")
    df = dd.read_parquet(field_path, columns=OBJECT_COLUMNS, engine="pyarrow")
    with ProgressBar():
        df.to_parquet(output_path, schema=OBJECT_SCHEMA)
    return


def get_objects_table(bucket_name: str,
                      dr: str,
                      fields_prefix: str,
                      objects_prefix: str,
                      output_path: str) -> None:
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    fields = set([x.key.split("/")[1] for x in bucket.objects.filter(Prefix=fields_prefix)])
    objects_files = set([x.key.split("/")[-2] for x in bucket.objects.filter(Prefix=objects_prefix)])
    for field in fields:
        print(field)
        if field not in objects_files:
            get_objects(os.path.join("s3://", bucket_name, dr, field),
                        os.path.join(output_path, field))
    return


def compute_features(field_path):
    data = pd.read_parquet(field_path)
    
    extractor = CustomHierarchicalExtractor()
    pass


if __name__ == "__main__":
    get_objects_table("ztf-data-releases", "dr5", "dr5/field", "dr5/objects", "s3://ztf-data-releases/dr5/objects")
    print("end")
