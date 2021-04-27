import pandas as pd
import boto3
import psycopg2
import os


def load_csv_to_psql(
    bucket_name: str,
    datarelease: str,
    dbname: str,
    user: str,
    password: str,
    host: str,
    ) -> None:

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    for dr_object in bucket.objects.filter(Prefix=f'{datarelease}/objects_reference/field'):
        s3_csv = dr_object.key
        bucket.download_file(s3_csv, 'temp.csv')
        cursor = conn.cursor()
        with open('temp.csv', 'r') as f:
            next(f) # Skip the header row.
            cursor.copy_from(f, 'objects', sep=',')
        conn.commit()
        os.remove('temp.csv')
        break
    conn.close()
    return





