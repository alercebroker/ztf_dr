import click
import logging
import pandas as pd
import re
import os

from ztf_dr.utils.parse_parquet import parse_parquets
from ztf_dr.utils.preprocess import Preprocessor
from ztf_dr.utils.load_mongo import init_mongo, insert_data, drop_mongo
from ztf_dr.utils import existing_in_bucket, split_list, monitor
from ztf_dr.utils.s3 import s3_uri_bucket, get_s3_path_to_files
from ztf_dr.db.mongo import create_indexes, insert_dataframe
from ztf_dr.utils.jobs import run_jobs


def _insert_features(file_path, mongo_config, batch_size=10000):
    features = pd.read_parquet(file_path)
    features["_id"] = features.index
    inserted = insert_dataframe(features, mongo_config, batch_size=batch_size)
    return inserted


@click.group()
def cli():
    pass


@click.command()
@click.argument("data_release_url", type=str)
@click.argument("checksum_path", type=str)
@click.argument("s3_uri", type=str)
@click.option("--n-process", "-n", default=2, help="Number of cores")
@click.option("--output", "-o", type=str, default="/tmp")
def download_data_release(data_release_url, checksum_path, s3_uri, n_process, output):
    from ztf_dr.collectors.downloader import DRDownloader
    dr = DRDownloader(data_release_url,
                      checksum_path,
                      s3_uri=s3_uri,
                      output_folder=output)
    dr.run(n_process)
    return


@click.command()
@click.argument("s3_uri", type=str)
@click.argument("output_path", type=str)
def parse_data_release_parquets(s3_uri, output_path):
    parse_parquets(s3_uri, output_path)
    return


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
            _insert_features(file_path, mongo_config, batch_size=batch_size)

    else:
        args = [(os.path.join("s3://", bucket_name, f), mongo_config, batch_size) for f in to_process]
        run_jobs(args, _insert_features, num_processes=n_process)


@click.command()
@click.argument("bucket_name", type=str)
@click.argument("bucket_prefix", type=str)
@click.argument("bucket_output", type=str)
@click.option("--n-cores", "-n", default=2)
@click.option("--limit-epochs", "-l", default=20)
@click.option("--mag-error-tolerance", "-t", default=1.0)
def do_preprocess(bucket_name: str, bucket_prefix: str, bucket_output: str, n_cores: int, limit_epochs: int,
                  mag_error_tolerance: float):
    pr = Preprocessor(limit_epochs=limit_epochs, mag_error_tolerance=mag_error_tolerance, catflags_filter=0)
    pr.preprocess_bucket(bucket_name, bucket_prefix, bucket_output, n_cores=n_cores)


@click.command()
@click.argument("bucket_input", type=str)
@click.argument("bucket_output", type=str)
@click.argument("partition", type=int)
@click.option("--total-cores", "-t", default=300)
@click.option("--preprocess", "-p", is_flag=True, default=False, help="Do preprocess")
@click.option("--use-monitor", "-m", is_flag=True, default=False, help="Measure uses of resources using psrecord")
@click.option("--path-monitor", "-mp", type=str, default="/home/apps/astro/alercebroker/resources/dr")
def compute_features(bucket_input: str, bucket_output: str, partition: int, total_cores: int, preprocess: bool,
                     use_monitor: bool, path_monitor: str):
    from ztf_dr.extractors import DataReleaseExtractor
    if use_monitor:
        monitor(path_monitor, f"compute_features_{partition}", log=True, plot=False)
    logging.info("Initializing features computer")
    data_release = sorted(existing_in_bucket(bucket_input))

    partitions = split_list(data_release, total_cores)
    my_partition = partitions[partition]
    del partitions
    del data_release
    logging.info("Local partition read")
    existing_features = existing_in_bucket(bucket_output)
    existing_features = [
        x for x in existing_features
        if x.split("/")[-1] in [y.split("/")[-1] for y in my_partition]
    ]

    logging.info(f"Partition {partition} get {len(existing_features)}/{len(my_partition)} files")

    dr_ext = DataReleaseExtractor()
    dr_pre = Preprocessor(limit_epochs=20, mag_error_tolerance=1.0, catflags_filter=0)
    for index, file in enumerate(my_partition):
        output_file = re.findall(r".*/(field.*)", file)[0]
        output_file = os.path.join(bucket_output, output_file)

        if output_file in existing_features:
            logging.info(f"{index}/{len(my_partition)} already exists {file}")
            continue
        logging.info(f"{index}/{len(my_partition)} processing {file}")
        data = pd.read_parquet(file)
        if preprocess:
            data = dr_pre.run(data)

        if data is None:
            continue

        features = dr_ext.compute_features(data)
        del data
        if features is not None:
            features.to_parquet(output_file)
        del features
    pass


@click.command()
@click.argument("mongo_uri", type=str)
@click.argument("mongo_database", type=str)
@click.argument("mongo_collection", type=str)
@click.argument("s3_bucket", type=str)
@click.option("--n-cores", "-n", default=1)
@click.option("--batch-size", "-b", default=10000)
@click.option("--drop", "-d", is_flag=True, default=False)
def load_mongo(mongo_uri: str, mongo_database: str, mongo_collection: str, s3_bucket: str, n_cores: int,
               batch_size: int, drop: bool):
    logger = logging.getLogger("load_mongo")
    logger.setLevel("INFO")
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level="INFO")
    file = logging.FileHandler("load_mongo.log")
    logger.addHandler(file)
    logger.info("Init now")
    if drop:
        drop_mongo(mongo_uri, mongo_database, mongo_collection)
    insert_data(s3_bucket, mongo_uri, mongo_database, mongo_collection, batch_size=batch_size, n_cores=n_cores)
    init_mongo(mongo_uri, mongo_database, mongo_collection)


def cmd():
    cli.add_command(download_data_release)
    cli.add_command(parse_data_release_parquets)
    cli.add_command(insert_features)
    cli.add_command(do_preprocess)
    cli.add_command(compute_features)
    cli.add_command(load_mongo)
    cli()


if __name__ == "__main__":
    logging.basicConfig(level="INFO",
                        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    cmd()
