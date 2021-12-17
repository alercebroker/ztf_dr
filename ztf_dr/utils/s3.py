import boto3
import os

from urllib.parse import urlparse
from typing import List, Tuple


def s3_uri_bucket(s3_uri: str) -> Tuple:
    parsed_url = urlparse(s3_uri)
    protocol = parsed_url.scheme
    if protocol != "s3":
        raise Exception(f"The uri {s3_uri} doesn't comply with the s3 protocol (e.g. s3://bucket/path_to_folder)")
    bucket_name = parsed_url.hostname
    path = parsed_url.path[1:]
    return bucket_name, path


def get_s3_path_to_files(bucket_name: str, path: str) -> List[str]:
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    input_files = [
        os.path.join("s3://", bucket_name, x.key)
        for x in bucket.objects.filter(Prefix=path)
        if "tmp" not in x.key
    ]
    return input_files


def s3_filename_difference(list_a: List[str], list_b: List[str]) -> List[str]:
    response = []
    list_b = [x.split("/")[-1] for x in list_b]
    for file in list_a:
        _file = file.split("/")[-1]
        if _file not in list_b:
            response.append(file)
    return response
