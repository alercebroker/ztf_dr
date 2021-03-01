import re
import os
import boto3
import click
import requests
import multiprocessing

import pandas as pd

from helpers import (
    transform_size,
    download_and_process
)
 

REGEX_STR = r'<img.*> <a href="(field.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)\n'
s3 = boto3.resource('s3')

@click.command()
@click.argument('url')
@click.argument('bucket')
@click.option('-o', '--output-directory', default="/tmp", type=click.Path(exists=True))
@click.option('-n', '--n-proc', default=multiprocessing.cpu_count(), type=int)
def load(url, bucket, output_directory, n_proc):
    """Download and Load ZTF Data Release to S3."""
    # TODO: Validate parameters
    try:
        click.echo("Preparing Checksums")
        checksums_url = os.path.join(url, "checksums.md5")
        checksums = pd.read_csv(checksums_url, delimiter="\s+", names=["checksum", "file"], index_col="file", squeeze=True)
    except Exception as e:
        click.echo("Something went wrong reading checksums")
        raise e

    try:    
        click.echo("Loading file paths")
        response = requests.get(url).content.decode("utf-8")
        files = re.findall(REGEX_STR,response)
        df = pd.DataFrame(files, columns=["file", "date", "size"])
        df["file_path"] = df.file.apply(lambda x: os.path.join(url, x))
        df = df.join(checksums,on="file")
        df.drop(columns=["date","size"], inplace=True)
        df["bucket"] = bucket
        df["output_directory"] = output_directory
    except Exception as e:
        click.echo("Something went wrong loading files paths")
        raise e

    with multiprocessing.Pool(n_proc) as p:
        parameters = df[
                [
                    "file", 
                    "file_path", 
                    "checksum", 
                    "bucket", 
                    "output_directory"
                ]
            ].to_numpy()
        p.starmap(download_and_process, parameters)
    
    return
    #1.- Check bucket (if file exists go to 5) 
    #2.- Download
    #3.- Checksum
    #4.- Upload Raw to S3
    #5.- Process
    #6.- Upload processed csv to S3


if __name__ == '__main__':
    load()