#!/usr/bin/env python3
"""Add 'platform' column to existing BQ clicks table (non-destructive ALTER)."""

import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "glance_s2s")
BQ_TABLE = os.getenv("BQ_TABLE", "clicks")

client = bigquery.Client(project=BQ_PROJECT)
table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

table = client.get_table(table_ref)
original_schema = list(table.schema)

# Check if platform already exists
existing_fields = [f.name for f in original_schema]
if "platform" in existing_fields:
    print("Column 'platform' already exists. Nothing to do.")
else:
    new_schema = original_schema + [
        bigquery.SchemaField("platform", "STRING"),
    ]
    table.schema = new_schema
    client.update_table(table, ["schema"])
    print(f"Added 'platform' column to {table_ref}")

print("Done!")
