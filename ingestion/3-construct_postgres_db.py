# convert dataframes into postgres database
# EXPECTS as ctx: a dict of table-to-be dataframes
# RESULTS in: the same ctx that came in
# SIDE EFFECT: a packed out postgres db, hopefully
import pandas
from sqlalchemy import create_engine


def run(ctx, cfg):
    db = create_engine(cfg["db_endpoint"])
    for k in ctx:
        print(f"processing {k} into postgres...")
        ctx[k].to_sql(
            k,
            db,
            if_exists = "replace",
            index = False
        )
    return ctx