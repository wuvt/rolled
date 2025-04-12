FROM python:3.13-slim

WORKDIR /ingestion
COPY . /ingestion

RUN pip install pandas sqlalchemy typesense openpyxl psycopg2-binary xlrd orjson

CMD sleep 5 && python3 run_pipeline.py && sleep 68400