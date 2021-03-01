import os 
import wget 
import click 
import hashlib

def generate_md5_checksum(fname, chunksize=4096):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def transform_size(x):
    x = x.replace(" ","")
    letter = x[-1]
    number = x[:-1]
    values = {"M": 1/1024, "K": 1/1024/1024, "G": 1 }
    return (float(number)*values[letter])


def download_file(local_path, link, checksum_reference=None):
    """Checks if a local file is present and downloads it from the specified path otherwise.
    If checksum_reference is specified, the file's md5 checksum is compared against the
    expected value.

    Keyword arguments:
    local_path -- path of the file whose checksum shall be generated
    link -- link where the file shall be downloaded from if it is not found locally
    checksum_reference -- expected MD5 checksum of the file
    """
    if not os.path.exists(local_path):
        print('Downloading from %s, this may take a while...' % link)
        wget.download(link, local_path)
        print()
    if checksum_reference is not None:
        checksum = generate_md5_checksum(local_path)
        if checksum != checksum_reference:
            raise ValueError(
                'The MD5 checksum of local file %s differs from %s, please manually remove \
                 the file and try again.' %
                (local_path, checksum_reference))
    return local_path 

def download(path, url):
    pass

def cleanup(path):
    pass

def process(path):
    pass

def get_from_s3(name, bucket):
    pass

def download_and_process(file, file_path, checksum, bucket, output_directory):
    click.echo(file)
    click.echo(file_path)
    click.echo(checksum)   
    click.echo(bucket) 
    pass