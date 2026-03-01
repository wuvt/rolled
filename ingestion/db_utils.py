# Util function for rolled postgreSQL DB.

import pandas, sqlalchemy

# Import sql table as a pandas dataframe
def postgres_to_df(engine: sqlalchemy.engine.Engine, table_name: str,
                   schema: str = "public") -> pandas.DataFrame | None:
    inspector = sqlalchemy.inspect(engine)

    # Check if table exists
    if not inspector.has_table(table_name, schema=schema):
        print(f"Table '{table_name}' does not exist.")
        return None

    # Load table into DataFrame
    with engine.connect() as conn:
        df = pandas.read_sql_table(table_name, conn, schema=schema)

    return df
