# convert dataframes into typesense database
# EXPECTS as ctx: a dict of table-to-be dataframes
# RESULTS in: nothing. end of the road.
# SIDE EFFECT: a stuffed typesense db, ideally
import typesense, pandas, numpy, json

def run(ctx, cfg):
    db = typesense.Client({
        "nodes": [cfg["typesense_node"]],
        "api_key": cfg["typesense_apikey"],
        "connection_timeout_seconds": 2
    })

    for table in ctx:
        print(f"processing {table} into typesense...")

        #FIXME this is just for the proto
        if table != "albums": continue

        df = ctx[table]
        if table in [c["name"] for c in db.collections.retrieve()]:
            db.collections[table].delete()

        col_types = {
            col: {
                numpy.dtype("float32"): "float",
                numpy.dtype("float64"): "float",
                numpy.dtype("int32"): "int32",
                numpy.dtype("int64"): "int64",
                pandas.StringDtype: "string",
            }.get(df.dtypes[col], "string")
            for col in df.columns
        }

        db.collections.create({
            "name": table,
            "fields": [
                {
                    "name": col,
                    "type": col_types[col]
                }
                for col in df.columns
            ],
            "default_sorting_field": "mbid"
        })

        for i, row in df.iterrows():
            print(i)
            d = {
                k: str(v) if col_types[k] == "string" else v
                for k, v in row.to_dict().items()
            }
            print(d)
            db.collections[table].documents.create(d)
    
    return None