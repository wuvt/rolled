import typesense, json

cfg = {
        "db_endpoint": "postgresql://root:aids@localhost:5432/rolled",
        "typesense_node": {
            "host": "localhost",
            "port": "8108",
            "protocol": "http"
        },
        "typesense_apikey": "c940bfc2d4664bc008ae3a37bf8ca160c8ce3ce1768454160ba0bd6141a65c84"
}
db = typesense.Client({
    "nodes": [cfg["typesense_node"]],
    "api_key": cfg["typesense_apikey"],
    "connection_timeout_seconds": 2
})
print(json.dumps(db.keys.retrieve(), indent=2))