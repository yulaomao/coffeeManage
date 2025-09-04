from flask import Flask
from flask_cors import CORS
from redis import Redis
from apscheduler.schedulers.background import BackgroundScheduler
import json


class RedisClient:
    def __init__(self):
        self._client = None

    def init_app(self, app: Flask):
        url = app.config.get("REDIS_URL")
        self._client = Redis.from_url(url, decode_responses=True)

    @property
    def r(self):
        if not self._client:
            raise RuntimeError("Redis not initialized")
        return self._client


class SchedulerExt:
    def __init__(self):
        self._sched = None

    def init_app(self, app: Flask):
        sched = BackgroundScheduler(daemon=True)
        sched.start()
        self._sched = sched

    @property
    def instance(self):
        return self._sched


redis_cli = RedisClient()
scheduler = SchedulerExt()
cors = CORS()


def jget(s: str, default=None):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default


def jset(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
