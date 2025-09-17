FROM docker.io/library/python:3.13-slim

RUN pip install pandas sqlalchemy typesense openpyxl psycopg2-binary xlrd orjson

WORKDIR /ingestion
COPY . /ingestion

CMD sleep 5 && python3 run_pipeline.py && sleep 68400