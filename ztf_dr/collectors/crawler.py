import pandas as pd
import requests
import re
import os

REGEX_STR = r'<img.*> <a href="(ztf.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)'
REGEX_FOLDER = r'<img.*> <a href="(.*)">.*</a>\s+(\S+ \S+)\s+(\d*.*)\n'
REGEX_SIZE = r'(\d+)\s*(\w+)'
UNITS = {"B": 1, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}


def parse_size(size: str) -> int:
    """
    Convert `string` size to bytes.

    :param size: Size in string, for example 10M
    :return: Size in bytes representation
    """
    data = re.findall(REGEX_SIZE, size)
    if len(data) == 1:
        data = data[0]
        size, unit = int(data[0]), data[1]
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


def get_content(url):
    return requests.get(url).content.decode("utf-8")


def get_data_release(data_release_url: str) -> pd.DataFrame:
    """
    Function that allows you to obtain the links to the different files of the data release, with their respective
    metadata (size, field, date, etc). This function navigate for the folder of CALTECH, obtaining data of all fields.
    The execution can be slow because the performance depends of your bandwith and CALTECH bandwith.

    :param data_release_url: URL to data release. For example: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5
    :return: Dataframe that contain information of data release.
    """
    response = get_content(data_release_url)
    files = re.findall(REGEX_FOLDER, response)
    df = pd.DataFrame(files, columns=["file", "date", "size"])
    df["file"] = df["file"].apply(lambda x: os.path.join(data_release_url, x))
    for index, row in df.iterrows():
        file = row["file"]
        if is_folder(file):
            page = get_data_release(file)
            df = df.append(page, ignore_index=True)
    return df


def size_by_field(data: pd.DataFrame) -> pd.DataFrame:
    """
    Get size by field.

    :param data: Input data of data release, obtained through `get_data_release`.
    :return: Dataframe with size by field.
    """
    def aux(row):
        total_size = row["size"].sum() / (1024 * 1024)
        files = len(row["field"])
        return pd.Series({
            "files": files,
            "total_size": total_size
        })
    df = data.copy()
    flags = df["file"].map(lambda x: "parquet" in x)
    df = df[flags]
    df["field"] = df["file"].map(lambda x: x.split("/")[-2])
    df["size"] = df["size"].map(parse_size)
    res = df.groupby("field").apply(aux)
    return res
