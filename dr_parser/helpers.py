from .dr_parser_wrapper import parse_dr3
import logging
import os
import pandas as pd
logging.basicConfig(level=logging.INFO)


def process_dr3_file(input_file, output_dir, nobjects):
    fname = os.path.basename(input_file).split(".")[0]
    out_dir = os.path.join(output_dir, fname)
    os.makedirs(out_dir, exist_ok=True)
    i = 0
    total = 0
    for data in parse_dr3(input_file, nobjects):
        df = data["results"]
        objs = data["objects"]
        if len(df) > 0:
            df = pd.DataFrame(df)
            out_file_path = os.path.join(out_dir, f"partition_{i}.parquet")
            df.to_parquet(out_file_path)
            readed = len(df)
            total += readed
        logging.info(f"Partition {i} - Readed: {readed} - Total {total}")
        i+=1
        if objs:
            out_file_path = os.path.join(out_dir, f"objects.parquet")
            objs = pd.DataFrame(objs)
            objs.to_parquet(out_file_path)
