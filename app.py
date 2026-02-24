"""Glance-S2S Click Tracker — FastAPI app with Redis buffering."""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import redis
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request
from fastapi.responses import Response

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("glance-s2s")

app = FastAPI(title="Glance-S2S Click Tracker", docs_url=None, redoc_url=None)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY = os.getenv("REDIS_KEY", "glance_s2s:pending")

pool = redis.ConnectionPool.from_url(REDIS_URL)


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


# 1x1 transparent GIF
PIXEL = (
    b"\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00"
    b"\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21"
    b"\xf9\x04\x00\x00\x00\x00\x00\x2c\x00\x00"
    b"\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44"
    b"\x01\x00\x3b"
)


@app.get("/track")
async def track(
    request: Request,
    uniqueId: Optional[str] = Query(None),
    sub1: Optional[str] = Query(None),
    sub2: Optional[str] = Query(None),
    sub3: Optional[str] = Query(None),
    sub4: Optional[str] = Query(None),
    sub5: Optional[str] = Query(None),
    sub6: Optional[str] = Query(None),
    sub7: Optional[str] = Query(None),
    sub8: Optional[str] = Query(None),
    sub9: Optional[str] = Query(None),
    sub10: Optional[str] = Query(None),
):
    """Record a click event and return a tracking pixel."""
    record = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uniqueId": uniqueId or "",
        "sub1": sub1 or "",
        "sub2": sub2 or "",
        "sub3": sub3 or "",
        "sub4": sub4 or "",
        "sub5": sub5 or "",
        "sub6": sub6 or "",
        "sub7": sub7 or "",
        "sub8": sub8 or "",
        "sub9": sub9 or "",
        "sub10": sub10 or "",
        "ip": request.headers.get("x-forwarded-for", request.client.host if request.client else ""),
        "user_agent": request.headers.get("user-agent", ""),
    }

    try:
        r = get_redis()
        r.rpush(REDIS_KEY, json.dumps(record))
    except Exception:
        logger.exception("Failed to write to Redis")

    return Response(content=PIXEL, media_type="image/gif")


@app.get("/health")
async def health():
    return {"status": "ok"}
