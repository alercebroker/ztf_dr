import click
import logging
import pandas as pd
import os

from aiohttp.client_exceptions import ServerTimeoutError
from ztf_dr.utils.parse_parquet import parse_parquets
from ztf_dr.utils import split_list, monitor
from ztf_dr.utils.jobs import run_jobs
from ztf_dr.utils.s3 import s3_uri_bucket, get_s3_path_to_files, s3_filename_difference
from ztf_dr.collectors.downloader import DRDownloader
from ztf_dr.db.mongo import insert_features_to_mongo, insert_lightcurves_to_mongo, drop_mongo, create_indexes


@click.group()
def cli():
    pass


@click.command()
@click.argument("data_release_url", type=str)
@click.argument("checksum_path", type=str)
@click.argument("s3_uri", type=str)
@click.option("--n-processes", "-n", default=2, help="Number of processes")
@click.option("--output", "-o", type=str, default="/tmp")
def download_data_release(data_release_url: str,
                          checksum_path: str,
                          s3_uri: str,
                          n_processes: int,
                          output: str):
    dr = DRDownloader(data_release_url,
                      checksum_path,
                      s3_uri=s3_uri,
                      output_folder=output)
    dr.run(n_processes)
    return


@click.command()
@click.argument("s3_uri", type=str)
@click.argument("output_path", type=str)
@click.option("--n-processes", "-n", default=2, help="Number of processes")
def parse_data_release_parquets(s3_uri: str,
                                output_path: str,
                                n_processes: int):
    parse_parquets(s3_uri, output_path, n_processes=n_processes)
    return


@click.command()
@click.argument("s3_uri_input", type=str)
@click.argument("s3_uri_output", type=str)
@click.argument("partition", type=int)
@click.option("--total-cores", "-t", default=300)
@click.option("--preprocess", "-p", is_flag=True, default=False, help="Do preprocess")
@click.option("--use-monitor", "-m", is_flag=True, default=False, help="Measure uses of resources using psrecord")
@click.option("--path-monitor", "-mp", type=str, default="/home/apps/astro/alercebroker/resources/dr")
def compute_features(s3_uri_input: str,
                     s3_uri_output: str,
                     partition: int,
                     total_cores: int,
                     preprocess: bool,
                     use_monitor: bool,
                     path_monitor: str):
    from ztf_dr.extractors import DataReleaseExtractor
    from ztf_dr.utils.preprocess import Preprocessor
    if use_monitor:
        monitor(path_monitor, f"compute_features_{partition}", log=True, plot=False)
    logging.info("Initializing features computer")
    bucket_name_input, path_input = s3_uri_bucket(s3_uri_input)
    bucket_name_output, path_output = s3_uri_bucket(s3_uri_output)

    data_release = get_s3_path_to_files(bucket_name_input, path_input)
    existing_features = get_s3_path_to_files(bucket_name_output, path_output)
    to_process = s3_filename_difference(data_release, existing_features)

    partitions = split_list(to_process, total_cores)
    my_partition = partitions[partition]
    logging.info(f"Partition {partition} has {len(my_partition)} files")
    del partitions
    del data_release
    del to_process
    del existing_features
    dr_ext = DataReleaseExtractor()
    dr_pre = Preprocessor(limit_epochs=20, mag_error_tolerance=1.0, catflags_filter=0)
    for index, file in enumerate(my_partition):
        out_file = "/".join(file.split("/")[-2:])
        output_file = os.path.join("s3://", bucket_name_output, path_output, out_file)
        logging.info(f"{index+1}/{len(my_partition)} processing {file}")
        data = pd.read_parquet(file)
        if preprocess:
            data = dr_pre.run(data)

        if data is None:
            continue

        features = dr_ext.compute_features(data)
        del data
        if len(features) == 0:
            continue
        if features is not None:
            tries = 0
            while tries < 5:
                try:
                    features.to_parquet(output_file)
                    tries = 5
                except ServerTimeoutError:
                    tries += 1

        del features
    logging.info(f"Features computed")


@click.command()
@click.argument("mongo_uri", type=str)
@click.argument("mongo_database", type=str)
@click.argument("mongo_collection", type=str)
@click.argument("s3_bucket", type=str)
@click.option("--n-processes", "-n", default=1)
@click.option("--batch-size", "-b", default=10000)
@click.option("--drop", "-d", is_flag=True, default=False)
def insert_lightcurves(mongo_uri: str,
                       mongo_database: str,
                       mongo_collection: str,
                       s3_uri: str,
                       n_processes: int,
                       batch_size: int,
                       drop: bool):
    logging.info("Init now")
    mongo_config = {
        "mongo_uri": mongo_uri,
        "mongo_database": mongo_database,
        "mongo_collection": mongo_collection
    }
    if drop:
        drop_mongo(mongo_config)
    bucket_name, path = s3_uri_bucket(s3_uri)
    to_process = get_s3_path_to_files(bucket_name, path)

    if n_processes == 1:
        for file in to_process:
            file_path = os.path.join("s3://", bucket_name, file)
            insert_lightcurves_to_mongo(file_path, mongo_config, batch_size=batch_size)

    else:
        args = [(os.path.join("s3://", bucket_name, f), mongo_config, batch_size) for f in to_process]
        run_jobs(args, insert_lightcurves_to_mongo, num_processes=n_processes)

    mongo_indexes = [("loc", "2dsphere"), ("fieldid", 1), ("filterid", 1)]
    create_indexes(mongo_config, mongo_indexes)


@click.command()
@click.argument("s3_uri", type=str)
@click.argument("mongo_uri", type=str)
@click.argument("mongo_database", type=str)
@click.argument("mongo_collection", type=str)
@click.option("--n-process", "-n", default=2)
@click.option("--batch-size", "-b", default=10000)
def insert_features(s3_uri: str,
                    mongo_uri: str,
                    mongo_database: str,
                    mongo_collection: str,
                    n_process: int,
                    batch_size: int):
    bucket_name, path = s3_uri_bucket(s3_uri)
    to_process = get_s3_path_to_files(bucket_name, path)
    mongo_config = {
        "mongo_uri": mongo_uri,
        "mongo_database": mongo_database,
        "mongo_collection": mongo_collection
    }

    if n_process == 1:
        for file in to_process:
            file_path = os.path.join("s3://", bucket_name, file)
            insert_features_to_mongo(file_path, mongo_config, batch_size=batch_size)

    else:
        args = [(os.path.join("s3://", bucket_name, f), mongo_config, batch_size) for f in to_process]
        run_jobs(args, insert_features_to_mongo, num_processes=n_process)


def cmd():
    cli.add_command(download_data_release)
    cli.add_command(parse_data_release_parquets)
    cli.add_command(compute_features)
    cli.add_command(insert_features)
    cli.add_command(insert_lightcurves)
    cli()


if __name__ == "__main__":
    logging.basicConfig(level="INFO",
                        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    cmd()
