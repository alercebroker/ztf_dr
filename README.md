[![unittest](https://github.com/alercebroker/ztf_dr/actions/workflows/unittest.yml/badge.svg)](https://github.com/alercebroker/ztf_dr/actions/workflows/unittest.yml)
[![codecov](https://codecov.io/gh/alercebroker/ztf_dr/branch/main/graph/badge.svg)](https://codecov.io/gh/alercebroker/ztf_dr)




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

### Downloading a Data Release of ZTF (>= DR5)
You need an instance in S3 for run this script. The resources that worked without a problem are a `c5a.large` instance with 150 GB of disk. As you increase the number of processes to run, you will need more disks. 

Each process distributes the fields of the data release, each process performs:
1. Verify that the field file is in S3, if it is not there, the file is downloaded, if it is, continue with the next one.
2. Upload the file to S3.
3. Delete the file from disk.
4. Continue with the next file.

To run the code, follow the instructions below: 

1. Locate the data release that you need download. i.e: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/
2. Locate the checksum file. i.e: https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5
3. Execute for 5 parallel process (arg `-n`):

```
dr download-data-release https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/ \
    https://irsa.ipac.caltech.edu/data/ZTF/lc_dr5/checksums.md5 \
    s3://<your-bucket>/<data-release>/<etc> \
    -n 5
```
4. Wait calmly because there is a lot of data!

### Remove light curve of DR (or get object's table)
If you want to get the Data Release metadata without the light curves (to do xmatch or another operation), you can get it using the following command (you must have the data stored somewhere e.g S3):

```
dr get-objects <your-bucket> <data-release>
```

### Get features of data release
You can also obtain characteristics of the light curves from the Data Release (based on [code](https://github.com/alercebroker/lc_classifier) for [Sánchez-Sáez et al. 2020](https://arxiv.org/abs/2008.03311)), This can be very expensive but can be executed on different machines at the same time with slurm (on work).

Run for only one field:

```
dr get-features <input-file> <output-file>
```
 Or compute in your own code:

```python
import pandas as pd

from ztf_dr.extractors import DataReleaseExtractor

input_file = "path_to_pallet_town"
output_file = "path_to_drink_a_beer"

extractor = DataReleaseExtractor()
zone = pd.read_parquet(input_file)
features = extractor.compute_features(zone)
features.to_parquet(output_file)
```

If you have access to a slurm cluster, run this command in your terminal:

```
sbatch --array [0-499]%500 compute_features.slurm <s3-bucket-raw-data> <s3-bucket-output-data>
```

This code distributes 500 jobs in the whole cluster, therefore it distributes all the files of the data release in these jobs. This code is in charge of calculating the features for objects that meet the following conditions: 
- points of the light curve with catflags = 0 and magerr <1
- ndets> 20 in fid 1 and 2, ndets> 5 in fid 3 

### Integration with [ZTF DR API](https://github.com/alercebroker/ztf_dr_api)

Each time you want to update the data in the API database (when ZTF launch a new data release), you must do the following procedure:

1. Launch a machine and install mongodb, the disk should be of a size similar to the total of the data release (e.g DR5 weighs ~ 3.5 TB and the disk of the machine is 3.4 TB).
2. In the same instance run:
   
```
   dr load_mongo <host> <db-name> <collection-name> <input-s3-bucket> --n-cores <number-of-processes> --batch-size <objects-per-batch>
 ```
**NOTE:** If you want drop all elements in database use the flag `-d` in the command.

The code filter objects and converts the light curve to binary and inserts a document of the structure into database: 

```
{
    "_id" : NumberLong(1550215200000003),
    "filterid" : 2,
    "fieldid" : 1550,
    "rcid" : 57,
    "objra" : 35.6168823242188,
    "objdec" : 19.0562019348145,
    "nepochs" : 25,
    "hmjd" : { "$binary" : "avJjR2D1Y0eF/GNHawNkR3AJZEdsCmRHWzdkR0k5ZEdHOmRHTT1kR0w/ZEdNQGRHTUNkR01EZEdWRWRHZFFkRzRYZEcwWWRHYHJlR2ByZUdpc2VHaXNlR3Z7ZUdffGVHX3xlRw==", "$type" : "00" },
    "mag" : { "$binary" : "U/6EQc5ahUGrcYVBcG+FQfKUhUHSZYVBJliFQQBphUEcNIVB1AiFQRAqhUHOOoVBJA6FQUFahUEiMoVB9a2FQQ4UhUHWNoVBw0yFQQyYhUF2jIVBHCGFQWhchUHiQIVBWlyFQQ==", "$type" : "00" },
    "magerr" : { "$binary" : "U65nPEjEazyCz2w8TbVsPMdybjybRGw8dKVrPMppbDzXB2o8UCJoPA6WaTwEVGo8Ql1oPN69azxi8Wk8LKFvPBWfaDzWJmo82SFrPAiYbjwwDW48ODFpPOTWazx0mWo8O9ZrPA==", "$type" : "00" },
    "loc" : {
        "type" : "Point",
        "coordinates" : [ 
            -144.383117675781, 
            19.0562019348145
        ]
    }
}
```

Finally, the script create a spatial-index of the value `loc` and other indexes like by `nepochs`, `filterid`, `fieldid`, among others.

After that, in the AWS console:

1. Go to `lambda`.
2. Click on `ztd-dr-api`
3. Go to `configuration` after that click on `Environment variables`.
4. Change the oldest credentials to the newest credentials.

**NOTE:** If you did some change in the [ztf_dr_api](https://github.com/alercebroker/ztf_dr_api), you must update the lambda function.

## Maintenance

- In [data](https://github.com/alercebroker/ztf_dr_downloader/tree/master/data) folder we save some data of data releases (since DR5).
- In `data/DR<X>_by_field.csv` we save total size and amount of files by field.

## External documentation:

- [Table of central coordinates](https://www.oir.caltech.edu/twiki_ptf/pub/ZTF/ZTFFieldGrid/ZTF_Fields.txt)
- [Releases date](http://sites.astro.caltech.edu/ztf/csac/Presentations/masci_Pasadena_10.23.20.pdf)