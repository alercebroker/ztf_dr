import boto3
import os


def existing_in_bucket(bucket_path: str) -> list:
    bucket_name = bucket_path.split("/")[2]
    prefix = bucket_path.split(bucket_name)[-1][1:]
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = [
        os.path.join("s3://", bucket_name, x.key)
        for x in bucket.objects.filter(Prefix=prefix)
        if x.key.endswith(".parquet")
    ]
    return files


def split_list(seq: list, num: int):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def monitor(outdir, outname, log=True, plot=True):
    pid = os.getpid()
    L = ['psrecord', "%s" % pid, "--interval", "1"]
    if log:
        L = L + ["--log", os.path.join(outdir, f"log_{outname}_{pid}.txt")]
    if plot:
        L = L + ["--plot", os.path.join(outdir, f"plot_{outname}_{pid}.txt")]
    if not log and not plot:
        print("Nothing being monitored")
    else:
        os.spawnvpe(os.P_NOWAIT, 'psrecord', L, os.environ)
