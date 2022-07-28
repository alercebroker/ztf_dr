import pandas as pd
import requests
import re
import os

UNITS = {"B": 2 ** 0, "K": 2 ** 10, "M": 2 ** 20, "G": 2 ** 30, "T": 2 ** 40}
REGEX_STR = r'<img.*> <a href="(ztf.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)'
REGEX_FOLDER = r'<img.*> <a href="(.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)\n'
REGEX_SIZE = r'(\d+\.*\d*)(\w+)'


def parse_size(size: str) -> int:
    """
    Convert `string` size to bytes.

    :param size: Size in string, for example 10M
    :return: Size in bytes representation
    """
    data = re.findall(REGEX_SIZE, size)
    if len(data) == 1:
        data = data[0]
        size, unit = float(data[0]), data[1]
        return size * UNITS[unit]
    return -1


def is_folder(string: str) -> bool:
    """
    Simple function that verify if a URL or path is a folder.

    :param string: URL or path to field/parquet. This function allows to know the edge condition of the recursive
    function `get_data_release`. An example of input can be `https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0384/`
    that return True and `https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/0/field0384/ztf_000384_zr_c01_q1_dr5.parquet`
    that return False.
    :return: Boolean that indicate if the URL/path is a folder.
    """
    return string.endswith("/")


def get_content(url) -> pd.DataFrame:
    response = requests.get(url).content.decode("utf-8")
    files = re.findall(REGEX_FOLDER, response)
    df = pd.DataFrame(files, columns=["file", "date", "size"])
    df["link"] = df["file"].apply(lambda x: os.path.join(url, x))
    return df


def get_metadata(data_release_url: str) -> pd.DataFrame:
    """
    Function that allows you to obtain the links to the different files of the data release, with their respective
    metadata (size, field, date, etc). This function navigate for the folder of CALTECH, obtaining data of all fields.
    The execution can be slow because the performance depends on your bandwith and CALTECH bandwith.

    :param data_release_url: URL to data release. For example: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5
    :return: Dataframe that contain information of data release.
    """

    def _get_metadata(url: str, pages=None):
        print(f"Getting files from {url}", end="\r", flush=True)
        if pages is None:
            pages = []
        df = get_content(url)
        for index, row in df.iterrows():
            file = row["link"]
            if is_folder(file):
                page = _get_metadata(file, pages)
                pages.append(page)
        return df

    output = []
    _get_metadata(data_release_url, output)
    response = pd.concat(output, ignore_index=True)
    response = response[response["size"].map(lambda x: x.strip()) != "-"]  # Remove folders
    response["field"] = response["link"].map(lambda x: x.split("/")[-2])  # Put empirical link
    response["size"] = response["size"].map(parse_size) / 2 ** 20  # in GB
    return response


def get_checksums(checksum_path: str) -> pd.DataFrame:
    """
    Load checksums of all parquets in memory. Also generate urls to download and add a column of field of parquet.

    :return: Dataframe that contain data release information
    """
    print("Getting checksums of each file")
    checksums = pd.read_csv(checksum_path,
                            delimiter="\s+",
                            names=["checksum", "file"])
    checksums["file"] = checksums["file"].map(lambda x: x.split("/")[-1])
    return checksums


def get_data_release_files(data_release_url: str):
    checksum_url = os.path.join(data_release_url, "checksum.md5")
    checksums = get_checksums(checksum_url)
    metadata = get_metadata(data_release_url)
    response = metadata.join(checksums.set_index("file"), on="file")
    return response
