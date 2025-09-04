from typing import Dict, Any, List, Tuple
from ..utils.extensions import redis_cli, jget, jset
from ..utils.keys import (
    k_order, k_orders_by_ts, ts,
    k_orders_global_by_ts, k_order_index, k_audit_stream
)
from datetime import datetime
import csv, io, json


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
        # global indices (best-effort)
        try:
            r.zadd(k_orders_global_by_ts(), {f"{device_id}:{order_id}": ts_val})
            r.set(k_order_index(order_id), device_id)
        except Exception:
            pass
        return True

    # Global querying and utilities
    @staticmethod
    def _match_filters(h: Dict[str, Any], q: Dict[str, Any]) -> bool:
        # h: order hash
        # q: filters: device_id, order_id, pay_status, status, channel, recipe_id, q (keyword), min_amount, max_amount
        if not h:
            return False
        if q.get('device_id') and (h.get('device_id') != q['device_id']):
            return False
        if q.get('order_id') and (h.get('order_id') != q['order_id']):
            return False
        if q.get('pay_status') and (h.get('pay_status') != q['pay_status']):
            return False
        if q.get('status') and (h.get('status') != q['status']):
            return False
        if q.get('channel') and (h.get('channel') != q['channel']):
            return False
        if q.get('recipe_id') and (h.get('recipe_id') != q['recipe_id']):
            return False
        minv = q.get('min_amount')
        if minv is not None and str(minv).strip() != '':
            try:
                if int(h.get('amount_cents') or 0) < int(minv):
                    return False
            except Exception:
                pass  # ignore invalid filter value
        maxv = q.get('max_amount')
        if maxv is not None and str(maxv).strip() != '':
            try:
                if int(h.get('amount_cents') or 0) > int(maxv):
                    return False
            except Exception:
                pass  # ignore invalid filter value
        kw = (q.get('q') or '').strip().lower()
        if kw:
            blob = json.dumps(h, ensure_ascii=False).lower()
            if kw not in blob:
                return False
        return True

    @staticmethod
    def list_orders(filters: Dict[str, Any], page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        r = redis_cli.r
        page = max(1, int(page or 1))
        page_size = max(1, min(200, int(page_size or 50)))
        start_ts = filters.get('from')
        end_ts = filters.get('to')
        start = "-inf" if not start_ts else int(start_ts)
        end = "+inf" if not end_ts else int(end_ts)
        # Use global index if present, fallback scan by devices if missing
        items: List[Dict[str, Any]] = []
        total = 0
        try:
            members = r.zrevrangebyscore(k_orders_global_by_ts(), end, start, start=0, num=5000)
            if not members:
                raise RuntimeError("empty-global-index")
            # paginate after filtering
            for m in members:
                try:
                    device_id, order_id = m.split(":", 1)
                except ValueError:
                    continue
                h = r.hgetall(k_order(device_id, order_id))
                if not h:
                    continue
                # enrich
                h.setdefault('device_id', device_id)
                h.setdefault('order_id', order_id)
                if OrderService._match_filters(h, filters):
                    items.append(h)
            total = len(items)
            s = (page-1)*page_size
            e = s + page_size
            items = items[s:e]
        except Exception:
            # fallback: naive scan per device (may be slow)
            for key in r.scan_iter(match="cm:dev:*:orders:by_ts"):
                try:
                    device_id = key.split(":")[2]
                    ids = r.zrevrangebyscore(key, end, start, start=0, num=1000)
                    for oid in ids:
                        h = r.hgetall(k_order(device_id, oid))
                        if not h:
                            continue
                        h.setdefault('device_id', device_id)
                        h.setdefault('order_id', oid)
                        if OrderService._match_filters(h, filters):
                            items.append(h)
                except Exception:
                    pass
            total = len(items)
            s = (page-1)*page_size
            e = s + page_size
            items = items[s:e]
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def stats(filters: Dict[str, Any]) -> Dict[str, Any]:
        # Aggregate totals and simple hourly/daily buckets
        r = redis_cli.r
        start_ts = int(filters.get('from') or 0)
        end_ts = int(filters.get('to') or ts())
        # Collect candidate members similar to list_orders but without pagination
        rows: List[Dict[str, Any]] = []
        try:
            members = r.zrevrangebyscore(k_orders_global_by_ts(), end_ts, start_ts, start=0, num=10000)
            if not members:
                raise RuntimeError("empty-global-index")
            for m in members:
                try:
                    device_id, order_id = m.split(":", 1)
                except ValueError:
                    continue
                h = r.hgetall(k_order(device_id, order_id))
                if h:
                    h.setdefault('device_id', device_id)
                    h.setdefault('order_id', order_id)
                    if OrderService._match_filters(h, filters):
                        rows.append(h)
        except Exception:
            for key in r.scan_iter(match="cm:dev:*:orders:by_ts"):
                device_id = key.split(":")[2]
                ids = r.zrevrangebyscore(key, end_ts, start_ts, start=0, num=1000)
                for oid in ids:
                    h = r.hgetall(k_order(device_id, oid))
                    if h:
                        h.setdefault('device_id', device_id)
                        h.setdefault('order_id', oid)
                        if OrderService._match_filters(h, filters):
                            rows.append(h)
        total = len(rows)
        revenue_cents = 0
        success = 0
        refunded = 0
        for h in rows:
            try:
                revenue_cents += int(h.get('amount_cents') or 0)
            except Exception:
                pass
            if (h.get('status') or '') == 'success':
                success += 1
            if (h.get('pay_status') or '') == 'refunded':
                refunded += 1
        success_rate = (success/total*100.0) if total else 0.0
        refund_rate = (refunded/total*100.0) if total else 0.0
        arpu = (revenue_cents/total/100.0) if total else 0.0
        # timeseries (hourly buckets if <= 2 days, else daily)
        span = max(1, end_ts - start_ts)
        hourly = span <= 2*86400
        buckets = {}
        for h in rows:
            t = int(h.get('server_ts') or h.get('device_ts') or 0)
            if t <= 0:
                continue
            dt = datetime.utcfromtimestamp(t)
            label = dt.strftime('%Y-%m-%d %H:00') if hourly else dt.strftime('%Y-%m-%d')
            b = buckets.setdefault(label, {"orders": 0, "revenue_cents": 0})
            b["orders"] += 1
            try:
                b["revenue_cents"] += int(h.get('amount_cents') or 0)
            except Exception:
                pass
        labels = sorted(buckets.keys())
        trend_orders = [buckets[l]["orders"] for l in labels]
        trend_revenue = [round(buckets[l]["revenue_cents"] / 100.0, 2) for l in labels]
        return {
            "total": total,
            "revenue": round(revenue_cents/100.0, 2),
            "success_rate": round(success_rate, 2),
            "refund_rate": round(refund_rate, 2),
            "arpu": round(arpu, 2),
            "labels": labels,
            "trend_orders": trend_orders,
            "trend_revenue": trend_revenue,
        }

    @staticmethod
    def export(filters: Dict[str, Any], fmt: str = 'csv') -> Tuple[str, str, str]:
        # returns (content, mime, filename)
        data = OrderService.list_orders(filters, page=1, page_size=10000)
        rows = data.get('items') or []
        if fmt == 'json':
            payload = json.dumps(rows, ensure_ascii=False)
            return payload, 'application/json', 'orders.json'
        # CSV
        headers = set()
        for r in rows:
            for k in (r or {}).keys():
                headers.add(k)
        preferred = ["order_id","device_id","status","pay_status","channel","amount_cents","server_ts","device_ts","recipe_id","item","duration_ms","err_code","err_msg"]
        cols = [c for c in preferred if c in headers] + sorted([h for h in headers if h not in preferred])
        sio = io.StringIO()
        w = csv.writer(sio)
        w.writerow(cols)
        for r in rows:
            w.writerow([ (r.get(c) if isinstance(r, dict) else "") for c in cols ])
        return sio.getvalue(), 'text/csv', 'orders.csv'

    @staticmethod
    def get(order_id: str) -> Dict[str, Any]:
        r = redis_cli.r
        device_id = r.get(k_order_index(order_id))
        if device_id:
            h = r.hgetall(k_order(device_id, order_id))
            if h is None:
                h = {}
            if h:
                h.setdefault('device_id', device_id)
                h.setdefault('order_id', order_id)
            return h
        # fallback: scan devices
        for key in r.scan_iter(match="cm:dev:*:order:*"):
            try:
                parts = key.split(":")
                if parts[-2] == 'order' and parts[-1] == order_id:
                    device_id = parts[2]
                    h = r.hgetall(k_order(device_id, order_id))
                    if h:
                        h.setdefault('device_id', device_id)
                        h.setdefault('order_id', order_id)
                    return h
            except Exception:
                pass
        return {}

    @staticmethod
    def refund(order_id: str, actor: str) -> Dict[str, Any]:
        # Placeholder demo: mark pay_status=refunded and audit; real life would call PSP APIs.
        r = redis_cli.r
        h = OrderService.get(order_id)
        if not h:
            raise KeyError(order_id)
        device_id = h.get('device_id')
        # mark refunded
        try:
            r.hset(k_order(device_id, order_id), mapping={"pay_status": "refunded", "refund_ts": str(ts())})
            r.xadd(k_audit_stream(), {"action": "order_refund", "actor": actor, "target_id": order_id, "ts": ts(), "summary": device_id or ''})
        except Exception:
            pass
        return r.hgetall(k_order(device_id, order_id))
