import boto3
import os


def existing_in_bucket(bucket_path: str) -> list:
    bucket_name = bucket_path.split("/")[2]
    prefix = bucket_path.split(bucket_name)[-1][1:]
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = [
        os.path.join("s3://", bucket_name, x.key)
        for x in bucket.objects.filter(Prefix=prefix)
        if x.key.endswith(".parquet")
    ]
    return files


def split_list(seq: list, num: int):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out
