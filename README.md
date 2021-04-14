[![unittest](https://github.com/alercebroker/ztf_dr/actions/workflows/unittest.yml/badge.svg)](https://github.com/alercebroker/ztf_dr/actions/workflows/unittest.yml)
[![codecov](https://codecov.io/gh/alercebroker/ztf_dr/branch/master/graph/badge.svg)](https://codecov.io/gh/alercebroker/ztf_dr)


# ZTF DR

## Installation

1. Clone the repo

```bash
git clone https://github.com/alercebroker/ztf_dr_downloader.git
```

2. Install the package in your system
```
pip install .   
```

## Usage

1. Locate the data release that you need download. i.e: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/
2. Locate the checksum file. i.e: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5
3. Execute for 5 parallel process:

```
dr download-data-release https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/ \
    https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5 \
    s3://<your-bucket>/<data-release>/<etc> \
    -n 5
```
4. Wait calmly because there is a lot of data!

## Maintenance

- In [data](https://github.com/alercebroker/ztf_dr_downloader/tree/master/data) folder we save some data of data releases (since DR5).
- In `data/DR<X>_by_field.csv` we save total size and amount of files by field.

## External documentation:

- [Table of central coordinates](https://www.oir.caltech.edu/twiki_ptf/pub/ZTF/ZTFFieldGrid/ZTF_Fields.txt)
- [Releases date](http://sites.astro.caltech.edu/ztf/csac/Presentations/masci_Pasadena_10.23.20.pdf)