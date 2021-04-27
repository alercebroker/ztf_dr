import os
import boto3
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa

from tqdm import tqdm
from dask.diagnostics import ProgressBar
from multiprocessing import Pool, cpu_count

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
        if field not in objects_files:
            get_objects(os.path.join("s3://", bucket_name, dr, field),
                        os.path.join(output_path, field))
    return


def get_objects_reference(input_file: str, output_file: str, extension="csv") -> None:
    df = pd.read_parquet(input_file, columns=OBJECT_COLUMNS, engine="pyarrow")
    df["link"] = input_file
    output_file = f"{output_file}.{extension}"
    if extension == "csv":
        df.to_csv(output_file, index=False)
    elif extension == "parquet":
        df.to_parquet(output_file, schema=OBJECT_SCHEMA)
    return


def get_objects_table_with_reference(bucket_name: str,
                                     dr: str,
                                     objects_prefix: str,
                                     n_cores: int = 3) -> None:
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    formatter = lambda x: f"{x[-2]}/{x[-1].split('.')[0]}"

    files = set([
        formatter(x.key.split("/"))
        for x in bucket.objects.filter(Prefix=f"{dr}/raw") if len(x.key.split("/")[-1]) > 1 and x.key.endswith("parquet")
    ])

    existing_files = set([
        formatter(x.key.split("/"))
        for x in bucket.objects.filter(Prefix=objects_prefix) if len(x.key.split("/")[-1]) > 1
    ])

    to_process = [(os.path.join("s3://", bucket_name, dr, "raw", f"{f}.parquet"),
                   os.path.join("s3://", bucket_name, dr, "objects_reference", f))
                  for f in files if f not in existing_files]
    if n_cores is None:
        n_cores = cpu_count()

    with Pool(n_cores) as p:
        p.starmap(get_objects_reference, to_process)
    """for file in tqdm(files):
        if file not in existing_files:
            input_bucket_file = os.path.join("s3://", bucket_name, dr, "raw", file)
            output_bucket_file = os.path.join("s3://", bucket_name, dr, "objects_reference", file)
            get_objects_reference(f"{input_bucket_file}.parquet", output_bucket_file, extension="csv")"""
    return


if __name__ == "__main__":
    get_objects_table("ztf-data-releases", "dr5", "dr5/field", "dr5/objects", "s3://ztf-data-releases/dr5/objects")
    print("end")
