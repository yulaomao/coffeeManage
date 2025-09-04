from typing import Dict, Any, List
from ..utils.extensions import redis_cli
from ..utils.keys import k_order, k_orders_by_ts, ts


class OrderService:
    @staticmethod
    def list_device_orders(device_id: str, limit: int = 50, start_ts: int | None = None, end_ts: int | None = None, offset: int = 0):
        r = redis_cli.r
        key = k_orders_by_ts(device_id)
        start = "-inf" if not start_ts else start_ts
        end = "+inf" if not end_ts else end_ts
        try:
            off = max(0, int(offset or 0))
        except Exception:
            off = 0
        ids = r.zrevrangebyscore(key, end, start, start=off, num=limit)
        res = []
        for oid in ids:
            res.append(r.hgetall(k_order(device_id, oid)))
        return res

    @staticmethod
    def create_order(device_id: str, order_id: str, h: Dict[str, Any]):
        r = redis_cli.r
        r.hset(k_order(device_id, order_id), mapping=h)
        ts_val = int(h.get("server_ts") or ts())
        r.zadd(k_orders_by_ts(device_id), {order_id: ts_val})
        return True
