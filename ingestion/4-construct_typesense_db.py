# convert dataframes into typesense database
# EXPECTS as ctx: a dict of table-to-be dataframes
# RESULTS in: nothing. end of the road.
# SIDE EFFECT: a stuffed typesense db, ideally
import typesense, pandas, numpy, json, time, math

def run(ctx, cfg):
    db = typesense.Client({
        "nodes": [cfg["typesense_node"]],
        "api_key": cfg["typesense_apikey"],
        "connection_timeout_seconds": 2
    })

    # generate the key our frontend will use
    try:
        print(db.keys.create({
            "description": "search-only frontend key",
            "actions": ["documents:search", "documents:get"],
            "collections": ["*"],
            "value": cfg["typesense_searchkey"]
        }))
    except typesense.exceptions.ObjectAlreadyExists:
        pass

    for table in ctx:
        print(f"processing {table} into typesense...")

        #FIXME this is just for the proto
        if table != "albums": continue

        df = ctx[table]
        if table in [c["name"] for c in db.collections.retrieve()]:
            db.collections[table].delete()

        df["row_idx"] = range(1, len(df) + 1)

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
            "default_sorting_field": "row_idx"
        })

        batch = []
        for i, row in df.iterrows():
            print(i)
            d = {
                k: str(v) if col_types[k] == "string" else (0.0 if isinstance(v, float) and math.isnan(v) else v)
                for k, v in row.to_dict().items()
            }
            print(d)
            batch.append(d)
        db.collections[table].documents.import_(
            batch,
            {"action": "upsert"}
        )
    
    return None