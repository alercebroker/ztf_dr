
def init_results(columns):
    result = {}
    for col in columns:
        result[col] = []
    return result

def parse_dr3(file, nobjs):
    scanned_objects = 0
    partition = 0
    metadata = None
    columns = ["ObjectID (oid)",
               "nDR3epochs",
               "FilterID",
               "FieldID",
               "RcID",
               "ObjRA",
               "ObjDec",
               "HMJD",
               "mag",
               "magerr",
               "clrcoeff",
               "catflags"]
    objects_cols = [
               "ObjectID (oid)",
               "nDR3epochs",
               "FilterID",
               "FieldID",
               "RcID",
               "ObjRA",
               "ObjDec",
               "Partition"
    ]
    results = init_results(columns)
    objects = init_results(objects_cols)
    with open(file) as f:
        for line in f:
            if line.startswith("#"):
                metadata = line[1:].split()
                metadata[0] = int(metadata[0])
                metadata[1:] = [float(d) for d in metadata[1:]]
                for col, d in zip(objects_cols, metadata+[partition]):
                    objects[col].append(d)
                if scanned_objects > nobjs+1:
                    scanned_objects = 0
                    return_results = results
                    results = init_results(columns)
                    partition  += 1
                    yield {"results": return_results, "objects": None}
                scanned_objects+=1
            else:
                data = [float(d) for d in line.split()]
                all_data = metadata + data
                for col, d in zip(columns, all_data):
                    results[col].append(d)
    yield  {"results": results, "objects": objects}
