import pandas as pd
import boto3
import psycopg2
import os
from multiprocessing import Pool

def execute_copy(file, temp_file, config, table_name, bucket_name):

    con = psycopg2.connect(**config)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.download_file(file, f'/tmp/{temp_file}')
    cursor = con.cursor()
    with open(f'/tmp/{temp_file}', 'r') as f:
        next(f) # Skip the header row.
        cursor.copy_from(f, table_name, sep=',')
    con.commit()
    con.close()
    os.remove(f'/tmp/{temp_file}')
    
def load_csv_to_psql(
    bucket_name: str,
    datarelease: str,
    dbname: str,
    user: str,
    password: str,
    host: str,
    ) -> None:

    config = {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host
    }
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = [(dr_object.key, dr_object.key.split('/')[-1], config, 'objects', bucket_name) for dr_object in bucket.objects.filter(Prefix=f'{datarelease}/objects_reference/field')]
    print(f"{len(files)} obtained from S3...")
    with Pool(4) as p:
        p.starmap(execute_copy, files)
    print(f"Data saved on DB")
    return





