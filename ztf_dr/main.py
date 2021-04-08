import click

from ztf_dr.downloader import DRDownloader


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


def cmd():
    cli.add_command(download_data_release)
    cli()


if __name__ == "__main__":
    cmd()
