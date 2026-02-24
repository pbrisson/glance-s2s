#!/usr/bin/env python3
"""Create BigQuery dataset and table for Glance-S2S."""

import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

BQ_PROJECT = os.getenv("BQ_PROJECT", "inmobi-dev-488419")
BQ_DATASET = os.getenv("BQ_DATASET", "glance_s2s")
BQ_TABLE = os.getenv("BQ_TABLE", "clicks")

client = bigquery.Client(project=BQ_PROJECT)

# Create dataset
dataset_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
dataset_ref.location = "US"
try:
    client.create_dataset(dataset_ref)
    print(f"Created dataset {BQ_DATASET}")
except Conflict:
    print(f"Dataset {BQ_DATASET} already exists")

# Create table
schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("uniqueId", "STRING"),
    bigquery.SchemaField("sub1", "STRING"),
    bigquery.SchemaField("sub2", "STRING"),
    bigquery.SchemaField("sub3", "STRING"),
    bigquery.SchemaField("sub4", "STRING"),
    bigquery.SchemaField("sub5", "STRING"),
    bigquery.SchemaField("sub6", "STRING"),
    bigquery.SchemaField("sub7", "STRING"),
    bigquery.SchemaField("sub8", "STRING"),
    bigquery.SchemaField("sub9", "STRING"),
    bigquery.SchemaField("sub10", "STRING"),
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("user_agent", "STRING"),
]

table_ref = bigquery.Table(f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}", schema=schema)
try:
    client.create_table(table_ref)
    print(f"Created table {BQ_TABLE}")
except Conflict:
    print(f"Table {BQ_TABLE} already exists")

print("Done!")
