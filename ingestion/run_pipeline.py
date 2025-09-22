import os, pathlib

print("running pipeline")

pipeline = {
    file.split("-")[0]: __import__(file.split(".py")[0])
    for file in os.listdir(
        pathlib.Path(__file__).parent.absolute()
    )
    if file[0] in "0123456789" and file.endswith(".py")
}

ctx = None
for key in sorted(pipeline):
    print("=-"*4+f"running {key}"+"-="*4)
    ctx = pipeline[key].run(ctx, {
        "postgres": {
            "host": os.getenv("ING_POSTGRES_HOST"),
            "port": os.getenv("ING_POSTGRES_PORT"),
            "username": os.getenv("ING_POSTGRES_USERNAME"),
            "password": os.getenv("ING_POSTGRES_PASSWORD"),
            "database": os.getenv("ING_POSTGRES_DATABASE")
        },
        "typesense_node": {
            "host": os.getenv("ING_TYPESENSE_HOST"),
            "port": os.getenv("ING_TYPESENSE_PORT"),
            "protocol": os.getenv("ING_TYPESENSE_PROTO"),
        },
        "typesense_apikey": os.getenv("ING_TYPESENSE_BOOTSTRAP_API_KEY"),
        "typesense_searchkey": os.getenv("ING_TYPESENSE_SEARCH_API_KEY")
    })