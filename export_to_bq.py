#!/usr/bin/env python3
"""Export pending click records from Redis to BigQuery."""

import json
import logging
import os
import sys

import redis
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("glance-s2s-export")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY = os.getenv("REDIS_KEY", "glance_s2s:pending")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "glance_s2s")
BQ_TABLE = os.getenv("BQ_TABLE", "clicks")

BATCH_SIZE = 500


def main():
    r = redis.from_url(REDIS_URL)
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    total = 0
    while True:
        pipe = r.pipeline()
        for _ in range(BATCH_SIZE):
            pipe.lpop(REDIS_KEY)
        results = pipe.execute()

        rows = []
        for raw in results:
            if raw is None:
                continue
            try:
                record = json.loads(raw)
                rows.append(record)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Skipping malformed record: %s", raw)

        if not rows:
            break

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        try:
            job = client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()  # Wait for completion
        except Exception as e:
            logger.error("BigQuery load error: %s", e)
            # Push failed rows back to Redis for retry
            for row in rows:
                r.rpush(REDIS_KEY, json.dumps(row))
            sys.exit(1)

        total += len(rows)
        logger.info("Inserted %d rows", len(rows))

        if len(rows) < BATCH_SIZE:
            break

    logger.info("Export complete. Total rows: %d", total)


if __name__ == "__main__":
    main()
