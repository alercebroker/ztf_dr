import os
import boto3
import logging
import pyarrow as pa
import dask.dataframe as dd

from urllib.parse import urlparse
from dask.diagnostics import ProgressBar

logging.basicConfig(level="INFO",
                    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

LC_FIELDS = {
    'objectid': pa.int64(),
    'filterid': pa.int8(),
    'fieldid': pa.int16(),
    'rcid': pa.int8(),
    'objra': pa.float32(),
    'objdec': pa.float32(),
    'nepochs': pa.int64(),
    'hmjd': pa.list_(pa.float64()),
    'mag': pa.list_(pa.float32()),
    'magerr': pa.list_(pa.float32()),
    'clrcoeff': pa.list_(pa.float32()),
    'catflags': pa.list_(pa.int32())
}

LC_SCHEMA = pa.schema(LC_FIELDS)


def parse_field(field_path: str, output_path: str) -> None:
    field_path = os.path.join(field_path, "*.parquet")
    df = dd.read_parquet(field_path, engine="pyarrow")
    with ProgressBar():
        df.to_parquet(output_path, schema=LC_SCHEMA)
    return


def parse_parquets(s3_uri_input: str, output_path: str) -> None:
    parsed_url = urlparse(s3_uri_input)
    bucket_name = parsed_url.hostname
    protocol = parsed_url.scheme
    path = parsed_url.path[1:]
    if protocol != "s3":
        raise Exception(f"The uri {s3_uri_input} doesn't comply with the s3 protocol (e.g. s3://bucket/path_to_folder)")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    fields = set([x.key.split("/")[-2] for x in bucket.objects.filter(Prefix=path)])
    parsed_fields = set([x.key.split("/")[-2] for x in bucket.objects.filter(Prefix=output_path)])

    logging.info(f"{len(parsed_fields)}/{len(fields)} fields processed")
    for field in fields:
        if field not in parsed_fields:
            path = "/".join(path.split("/")[:-1])
            field_path = os.path.join("s3://", bucket_name, path, field)
            output_path = os.path.join("s3://", bucket_name, output_path, field)
            parse_field(field_path, output_path)
    return


if __name__ == "__main__":
    parse_parquets("s3://ztf-data-releases/dr5/raw/field020", "dr5/parsed")
    print("end")
