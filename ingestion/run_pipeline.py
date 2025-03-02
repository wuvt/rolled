import os, pathlib

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
        "db_endpoint": "postgresql://root:aids@localhost:5432/rolled",
        "typesense_node": {
            "host": "localhost",
            "port": "8108",
            "protocol": "http"
        },
        "typesense_apikey": "aaachangethis"
    })