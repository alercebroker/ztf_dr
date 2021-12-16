import os
import logging
import pandas as pd
import pyarrow as pa

from ztf_dr.utils.jobs import run_jobs
from ztf_dr.utils.s3 import s3_uri_bucket, s3_filename_difference, get_s3_path_to_files

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


def parse_field(field_path: str, output_path: str) -> int:
    df = pd.read_parquet(field_path)
    df.to_parquet(output_path, schema=LC_SCHEMA)
    return 1


def parse_parquets(s3_uri_input: str, s3_uri_output: str, n_processes: int = 2) -> None:
    bucket_name_input, path_input = s3_uri_bucket(s3_uri_input)
    bucket_name_output, path_output = s3_uri_bucket(s3_uri_output)

    fields = get_s3_path_to_files(bucket_name_input, path_input)
    parsed_fields = get_s3_path_to_files(bucket_name_output, path_output)

    logging.info(f"{len(parsed_fields)}/{len(fields)} fields processed")

    to_process = s3_filename_difference(fields, parsed_fields)
    n_to_process = len(to_process)
    if n_to_process:
        logging.info(f"Process {n_to_process} files in {n_processes} processes")
        input_join_path = lambda f: os.path.join("s3://", bucket_name_output, f)
        output_join_path = lambda f: os.path.join("s3://", bucket_name_output, path_output, "/".join(f.split("/")[-2:]))
        arguments = [(input_join_path(x), output_join_path(x)) for x in to_process]
        run_jobs(arguments, parse_field, num_processes=n_processes)
    return
