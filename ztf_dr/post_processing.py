import dask.dataframe as dd
import pyarrow as pa

OBJECT_FIELDS = {
    'objectid': pa.int64(),
    'filterid': pa.int32(),
    'fieldid': pa.int32(),
    'rcid': pa.int32(),
    'objra': pa.float64(),
    'objdec': pa.float64(),
    'nepochs': pa.int32()
}

OBJECT_SCHEMA = pa.schema(OBJECT_FIELDS)

OBJECT_COLUMNS = list(OBJECT_FIELDS.keys())


def get_objects(field_path: str,
                output_path: str) -> None:
    df = dd.read_parquet(field_path, columns=OBJECT_COLUMNS, engine="pyarrow")
    df = df.compute()
    df.to_parquet(output_path, schema=OBJECT_SCHEMA)
    return


def get_objects_table(data_release_path: str) -> None:

    return


if __name__ == "__main__":
    print("a")
    get_objects_table("s3://ztf-data-releases/dr5/field0245/*.parquet", "objects_field0245.parquet")
    print("b")
    pass
