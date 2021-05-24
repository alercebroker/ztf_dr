import click
import logging
import pandas as pd
import re
import os

from ztf_dr.utils.post_processing import get_objects_table, get_objects_table_with_reference
from ztf_dr.utils.preprocess import Preprocessor
from ztf_dr.utils.load_psql import load_csv_to_psql
from ztf_dr.utils.load_mongo import init_mongo, insert_data, drop_mongo
from ztf_dr.utils import existing_in_bucket, split_list, monitor


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
    from ztf_dr.collectors.downloader import DRDownloader
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
@click.argument("bucket_name", type=str)
@click.argument("data_release", type=str)
def get_objects_with_reference(bucket_name, data_release):
    get_objects_table_with_reference(bucket_name,
                                     data_release,
                                     f"{data_release}/objects_reference")
    return


@click.command()
@click.argument("input_file", type=str)
@click.argument("output_file", type=str)
def get_features(input_file, output_file):
    from ztf_dr.extractors import DataReleaseExtractor
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
@click.argument("bucket_name", type=str)
@click.argument("datarelease", type=str)
@click.option("--dbname", "-t", default="datarelease")
@click.option("--user", default="postgres")
@click.option("--password", default="root")
@click.option("--host", default="127.0.0.1")
def load_psql(bucket_name: str, datarelease: str, dbname: str, user: str, password: str, host: str):
    load_csv_to_psql(bucket_name, datarelease, dbname, user, password, host)


@click.command()
@click.argument("mongo_uri", type=str)
@click.argument("mongo_database", type=str)
@click.argument("mongo_collection", type=str)
@click.argument("s3_bucket", type=str)
@click.option("--n-cores", "-n", default=1)
@click.option("--batch-size", "-b", default=10000)
def load_mongo(mongo_uri: str, mongo_database: str, mongo_collection: str, s3_bucket: str, n_cores: int, batch_size: int):
    logger = logging.getLogger("load_mongo")
    logger.setLevel("INFO")
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level="INFO")
    file = logging.FileHandler("load_mongo.log")
    logger.addHandler(file)
    logger.info("Init now")

    drop_mongo(mongo_uri, mongo_database, mongo_collection)
    insert_data(s3_bucket, mongo_uri, mongo_database, mongo_collection, batch_size=batch_size, n_cores=n_cores)
    init_mongo(mongo_uri, mongo_database, mongo_collection)


def cmd():
    cli.add_command(download_data_release)
    cli.add_command(get_objects)
    cli.add_command(get_objects_with_reference)
    cli.add_command(get_features)
    cli.add_command(do_preprocess)
    cli.add_command(compute_features)
    cli.add_command(load_psql)
    cli.add_command(load_mongo)
    cli()


if __name__ == "__main__":
    cmd()
