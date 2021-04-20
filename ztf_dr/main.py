import click
import logging
import pandas as pd
import re
import os

from ztf_dr.collectors.downloader import DRDownloader
from ztf_dr.extractors import DataReleaseExtractor
from ztf_dr.utils.post_processing import get_objects_table
from ztf_dr.utils.preprocess import Preprocessor
from ztf_dr.utils import existing_in_bucket, split_list


logging.basicConfig(level="INFO",
                    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


@click.group()
def cli():
    pass


@click.command()
@click.argument("data_release_url", type=str)
@click.argument("checksum_path", type=str)
@click.argument("bucket_path", type=str)
@click.option("--ncores", "-n", default=2, help="Number of cores")
@click.option("--output", "-o", type=str, default="/tmp")
def download_data_release(data_release_url, checksum_path, bucket_path, ncores, output):
    dr = DRDownloader(data_release_url,
                      checksum_path,
                      bucket=bucket_path,
                      output_folder=output)
    dr.run(ncores)
    return


@click.command()
@click.argument("bucket_name", type=str)
@click.argument("data_release", type=str)
@click.option("--prefix-field", "-pf", default=None)
def get_objects(bucket_name, data_release, prefix_field):
    if prefix_field is None:
        prefix_field = f"{data_release}/field"
    get_objects_table(bucket_name,
                      data_release,
                      prefix_field,
                      f"{data_release}/objects",
                      f"s3://{bucket_name}/{data_release}/objects")
    return


@click.command()
@click.argument("input_file", type=str)
@click.argument("output_file", type=str)
def get_features(input_file, output_file):
    extractor = DataReleaseExtractor()
    zone = pd.read_parquet(input_file)
    features = extractor.compute_features(zone)
    features.to_parquet(output_file)
    return


@click.command()
@click.argument("bucket_name", type=str)
@click.argument("bucket_prefix", type=str)
@click.argument("bucket_output", type=str)
@click.option("--n-cores", "-n", default=2)
def do_preprocess(bucket_name: str, bucket_prefix: str, bucket_output: str, n_cores: int):
    pr = Preprocessor(limit_epochs=20, mag_error_tolerance=1.0, catflags_filter=0)
    pr.preprocess_bucket(bucket_name, bucket_prefix, bucket_output, n_cores=n_cores)


@click.command()
@click.argument("bucket_input", type=str)
@click.argument("bucket_output", type=str)
@click.argument("partition", type=int)
@click.option("--total-cores", "-t", default=300)
@click.option("--preprocess", "-p", is_flag=True, default=False, help="Do preprocess")
def compute_features(bucket_input: str, bucket_output: str, partition: int, total_cores: int, preprocess: bool):
    logging.info("Initializing features computer")
    data_release = existing_in_bucket(bucket_input)
    existing_features = existing_in_bucket(bucket_output)

    partitions = split_list(data_release, total_cores)
    my_partition = partitions[partition]
    del partitions

    logging.info(f"Partition {partition} get {len(my_partition)} files")
    dr_ext = DataReleaseExtractor()
    dr_pre = Preprocessor(limit_epochs=20, mag_error_tolerance=1.0, catflags_filter=0)
    for file in my_partition:
        output_file = re.findall(r".*/(field.*)", file)[0]
        output_file = os.path.join(bucket_output, output_file)

        if output_file in existing_features:
            logging.info(f"already exists {file}")
            continue
        logging.info(f"Processing {file}")
        data = pd.read_parquet(file)
        if preprocess:
            data = dr_pre.run(data)
        features = dr_ext.compute_features(data)
        features.to_parquet(output_file)
    pass


def cmd():
    cli.add_command(download_data_release)
    cli.add_command(get_objects)
    cli.add_command(get_features)
    cli.add_command(do_preprocess)
    cli.add_command(compute_features)
    cli()


if __name__ == "__main__":
    cmd()
