import click
import pandas as pd

from ztf_dr.collectors.downloader import DRDownloader
from ztf_dr.utils.post_processing import get_objects_table
from ztf_dr.extractors import DataReleaseExtractor


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
@click.argument("data_release", type=str, help="e.g DR5")
@click.argument("bucket_path", type=str)
@click.option("--prefix-field", "-pf", default=None, help="Prefix of field in DR")
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


def cmd():
    cli.add_command(download_data_release)
    cli.add_command(get_objects)
    cli.add_command(get_features)
    cli()


if __name__ == "__main__":
    cmd()
