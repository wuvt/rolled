import typesense, json

cfg = {
        "db_endpoint": "postgresql://root:aids@localhost:5432/rolled",
        "typesense_node": {
            "host": "localhost",
            "port": "8108",
            "protocol": "http"
        },
        "typesense_apikey": "aaachangethis"
}
db = typesense.Client({
    "nodes": [cfg["typesense_node"]],
    "api_key": cfg["typesense_apikey"],
    "connection_timeout_seconds": 2
})
print(json.dumps(db.collections.retrieve(), indent=2))
print(db.collections["albums"])
print(db.collections["albums"].documents.search({
    "q": "young",
    "query_by": "album_title"
}))