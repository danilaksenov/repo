import os
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r        = redis.from_url(REDIS_URL, decode_responses=True)
JOB      = lambda jid: f"job:{jid}"
BUSY_KEY  = lambda cid: f"busy:{cid}"

BUSY_TTL  = 2 * 3600