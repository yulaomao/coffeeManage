from typing import Dict, Any, List
from ..utils.extensions import redis_cli
from ..utils.keys import ts

from ..utils.keys import (
    k_alarm, k_alarms_by_ts, k_alarms_status
)


class AlarmService:
    @staticmethod
    def list(device_id: str, limit: int = 100):
        r = redis_cli.r
        ids = r.zrevrange(k_alarms_by_ts(device_id), 0, limit-1)
        res = []
        for aid in ids:
            res.append(r.hgetall(k_alarm(device_id, aid)))
        return res

    @staticmethod
    def create(device_id: str, alarm_id: str, data: Dict[str, Any]):
        r = redis_cli.r
        h = {
            "id": alarm_id,
            "type": data.get("type", "generic"),
            "severity": data.get("severity", "info"),
            "title": data.get("title", ""),
            "description": data.get("description", ""),
            "status": data.get("status", "open"),
            "context_json": data.get("context_json", "{}"),
            "created_ts": str(ts()),
            "updated_ts": str(ts()),
        }
        r.hset(k_alarm(device_id, alarm_id), mapping=h)
        r.zadd(k_alarms_by_ts(device_id), {alarm_id: ts()})
        r.sadd(k_alarms_status(device_id, h["status"]), alarm_id)
        return h

    @staticmethod
    def set_status(device_id: str, alarm_id: str, status: str):
        r = redis_cli.r
        key = k_alarm(device_id, alarm_id)
        if not r.exists(key):
            return None
        old = r.hget(key, "status") or "open"
        if old != status:
            r.srem(k_alarms_status(device_id, old), alarm_id)
            r.sadd(k_alarms_status(device_id, status), alarm_id)
        r.hset(key, mapping={"status": status, "updated_ts": str(ts())})
        return r.hgetall(key)
