import os
import logging


def split_list(seq: list, num: int):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def monitor(out_dir: str, out_name: str, log=True, plot=True):
    pid = os.getpid()
    L = ['psrecord', "%s" % pid, "--interval", "1"]
    if log:
        L = L + ["--log", os.path.join(out_dir, f"log_{out_name}_{pid}.txt")]
    if plot:
        L = L + ["--plot", os.path.join(out_dir, f"plot_{out_name}_{pid}.txt")]
    if not log and not plot:
        logging.info("Nothing being monitored")
    else:
        os.spawnvpe(os.P_NOWAIT, 'psrecord', L, os.environ)
