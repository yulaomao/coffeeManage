from ..utils.extensions import redis_cli, jget, jset
from ..utils.keys import (
    k_device, ts, k_audit_stream, k_orders_by_ts, k_alarms_status, k_dict_material,
    k_menu_meta, k_menu_cats, k_menu_available
)
from datetime import datetime, timedelta
import json


class DeviceService:
    @staticmethod
    def touch_device(device_id: str, ip: str):
        r = redis_cli.r
        k = k_device(device_id)
        r.hset(k, mapping={
            "device_id": device_id,
            "status": "online",
            "last_seen_ts": ts(),
            "ip": ip,
        })
        r.xadd(k_audit_stream(), {"action": "device_touch", "actor": "device", "target_id": device_id, "ts": ts()})

    @staticmethod
    def get_summary(device_id: str):
        r = redis_cli.r
        k = k_device(device_id)
        h = r.hgetall(k)
        if not h:
            # init minimal
            h = {"device_id": device_id, "status": "registered"}
            r.hset(k, mapping=h)
        # sales today
        from datetime import datetime
        now = datetime.utcnow()
        day_start = int(datetime(now.year, now.month, now.day).timestamp())
        try:
            sales_today = int(r.zcount(k_orders_by_ts(device_id), day_start, "+inf") or 0)
        except Exception:
            sales_today = 0
        # menu meta and counts
        meta = r.hgetall(k_menu_meta(device_id)) or {}
        try:
            cat_cnt = int(r.zcard(k_menu_cats(device_id)) or 0)
        except Exception:
            cat_cnt = 0
        try:
            avail_cnt = int(r.scard(k_menu_available(device_id)) or 0)
        except Exception:
            avail_cnt = 0
        # bins low and alarms open
        try:
            low_bins = int(r.scard(f"cm:dev:{device_id}:bins:low") or 0)
        except Exception:
            low_bins = 0
        try:
            alarms_open = int(r.scard(k_alarms_status(device_id, "open")) or 0)
        except Exception:
            alarms_open = 0
        return {
            "device": h,
            "sales_today": sales_today,
            "menu": {"meta": meta, "category_count": cat_cnt, "available_count": avail_cnt},
            "bins_low_count": low_bins,
            "alarms_open": alarms_open,
        }

    @staticmethod
    def dashboard_summary():
        r = redis_cli.r
        # time windows
        now = datetime.utcnow()
        day_start = int(datetime(now.year, now.month, now.day).timestamp())
        week_start = day_start - 6 * 86400

        total = 0
        online = 0
        sales_today = 0
        sales_week = 0
        alarms_open = 0
        low_material_devices = 0
        pending_commands = 0

        # Iterate root device hashes only: cm:dev:{id}
        for key in r.scan_iter(match="cm:dev:*"):
            if key.count(":") != 2:
                continue
            h = r.hgetall(key)
            if not h:
                continue
            total += 1
            if (h.get("status") or "") in ("online",):
                online += 1
            device_id = h.get("device_id") or key.split(":")[2]
            # orders counts via ZCOUNT
            try:
                sales_today += int(r.zcount(k_orders_by_ts(device_id), day_start, "+inf") or 0)
                sales_week += int(r.zcount(k_orders_by_ts(device_id), week_start, "+inf") or 0)
            except Exception:
                pass
            # alarms open
            try:
                alarms_open += int(r.scard(k_alarms_status(device_id, "open")) or 0)
            except Exception:
                pass
            # low materials
            try:
                if int(r.scard(f"cm:dev:{device_id}:bins:low") or 0) > 0:
                    low_material_devices += 1
            except Exception:
                pass
            # pending commands
            try:
                pending_commands += int(r.llen(f"cm:dev:{device_id}:q:cmd:pending") or 0)
            except Exception:
                pass

        online_rate = round((online / total) * 100, 1) if total else 0.0

        # menu publishes in last 24h from audit stream (best-effort scan last 500)
        publishes_24h = 0
        try:
            cutoff = int(now.timestamp()) - 86400
            # prefer xrevrange if available
            try:
                entries = r.xrevrange(k_audit_stream(), count=500)
                for _id, fields in entries:
                    act = fields.get("action") if isinstance(fields, dict) else None
                    ts_field = fields.get("ts") if isinstance(fields, dict) else None
                    if act == "menu_publish" and ts_field and int(ts_field) >= cutoff:
                        publishes_24h += 1
            except Exception:
                raw = r.execute_command('XREVRANGE', k_audit_stream(), '+', '-', 'COUNT', '500') or []
                for sid, f in raw:
                    d = {}
                    for i in range(0, len(f), 2):
                        d[f[i]] = f[i+1]
                    if d.get("action") == "menu_publish" and d.get("ts") and int(d["ts"]) >= cutoff:
                        publishes_24h += 1
        except Exception:
            publishes_24h = 0

        return {
            "device_total": total,
            "online_rate": online_rate,
            "sales_today": sales_today,
            "sales_week": sales_week,
            "alarms_open": alarms_open,
            "low_material_devices": low_material_devices,
            "pending_commands_count": pending_commands,
            "menu_published_today": publishes_24h,
        }

    @staticmethod
    def list_devices(status: str | None = None, query: str | None = None, page: int = 1, page_size: int = 20):
        r = redis_cli.r
        items = []
        for key in r.scan_iter(match="cm:dev:*"):
            if key.count(":") != 2:
                continue
            h = r.hgetall(key)
            if not h:
                continue
            obj = {
                "device_id": h.get("device_id") or key.split(":")[2],
                "alias": h.get("alias", ""),
                "status": h.get("status", ""),
                "fw_version": h.get("fw_version", ""),
                "last_seen_ts": h.get("last_seen_ts", ""),
            }
            # filters
            if status and obj["status"] != status:
                continue
            if query:
                q = query.lower()
                if q not in (obj["device_id"] or "").lower() and q not in (obj["alias"] or "").lower():
                    continue
            items.append(obj)
        # sort by last_seen_ts desc
        def _score(o):
            try:
                return int(o.get("last_seen_ts") or 0)
            except Exception:
                return 0
        items.sort(key=_score, reverse=True)
        total = len(items)
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        start = (page - 1) * page_size
        end = start + page_size
        return {"items": items[start:end], "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def dashboard_trends(days: int = 7):
        r = redis_cli.r
        days = max(1, min(days, 30))
        now = datetime.utcnow()
        # build day buckets (UTC)
        labels = []
        starts = []
        for i in range(days-1, -1, -1):
            d = now - timedelta(days=i)
            start = datetime(d.year, d.month, d.day)
            labels.append(start.strftime("%Y-%m-%d"))
            starts.append(int(start.timestamp()))
        # sales and active devices per day
        sales = [0]*days
        active = [0]*days
        device_total = 0
        # Collect root devices
        devices = []
        for key in r.scan_iter(match="cm:dev:*"):
            if key.count(":") != 2:
                continue
            h = r.hgetall(key)
            if not h:
                continue
            device_total += 1
            devices.append(h.get("device_id") or key.split(":")[2])
        # For each device, aggregate orders per day and mark last_seen day as active
        for did in devices:
            # sales zset counts per day
            for idx, start in enumerate(starts):
                end = start + 86400 - 1
                try:
                    sales[idx] += int(r.zcount(k_orders_by_ts(did), start, end) or 0)
                except Exception:
                    pass
            # active by last_seen_ts falling into day bucket
            try:
                ls = int(r.hget(k_device(did), "last_seen_ts") or 0)
                if ls > 0:
                    # find bucket index
                    for idx, start in enumerate(starts):
                        if start <= ls < start + 86400:
                            active[idx] += 1
                            break
            except Exception:
                pass
        return {"days": labels, "sales": sales, "active_devices": active, "device_total": device_total}

    @staticmethod
    def list_low_materials(limit_devices: int = 10, max_bins: int = 5):
        r = redis_cli.r
        result = []
        # iterate root devices
        for key in r.scan_iter(match="cm:dev:*"):
            if key.count(":") != 2:
                continue
            h = r.hgetall(key)
            if not h:
                continue
            device_id = h.get("device_id") or key.split(":")[2]
            low_bins = list(r.smembers(f"cm:dev:{device_id}:bins:low"))
            if not low_bins:
                continue
            bins = []
            for bi in low_bins[:max_bins]:
                bh = r.hgetall(f"cm:dev:{device_id}:bin:{bi}")
                code = bh.get("material_code", "")
                md = r.hgetall(k_dict_material(code)) if code else {}
                name = md.get("name_i18n_json")
                try:
                    name_zh = (json.loads(name).get("zh") if name else None)
                except Exception:
                    name_zh = None
                try:
                    remaining = float(bh.get("remaining") or 0)
                    capacity = float(bh.get("capacity") or 0)
                    pct = int(round((remaining / capacity) * 100)) if capacity > 0 else 0
                except Exception:
                    remaining, capacity, pct = 0, 0, 0
                bins.append({
                    "bin_index": bi,
                    "material_code": code,
                    "material_name": name_zh or code,
                    "remaining": remaining,
                    "capacity": capacity,
                    "unit": bh.get("unit", ""),
                    "pct": pct,
                    "threshold_low_pct": bh.get("threshold_low_pct", "")
                })
            result.append({
                "device_id": device_id,
                "alias": h.get("alias", ""),
                "low_count": len(low_bins),
                "bins": bins,
            })
        # sort by low_count desc then device_id
        result.sort(key=lambda x: (-x.get("low_count", 0), x.get("device_id")))
        return result[:limit_devices]

    @staticmethod
    def list_bins(device_id: str):
        r = redis_cli.r
        bins = []
        # discover bin keys by scan
        prefix = f"cm:dev:{device_id}:bin:"
        for key in r.scan_iter(match=prefix+"*"):
            try:
                idx = key.split(":")[-1]
                bh = r.hgetall(key)
                code = bh.get("material_code", "")
                md = r.hgetall(k_dict_material(code)) if code else {}
                name = md.get("name_i18n_json")
                try:
                    name_zh = (json.loads(name).get("zh") if name else None)
                except Exception:
                    name_zh = None
                remaining = float(bh.get("remaining") or 0)
                capacity = float(bh.get("capacity") or 0)
                pct = int(round((remaining / capacity) * 100)) if capacity > 0 else 0
                low_set = r.sismember(f"cm:dev:{device_id}:bins:low", idx)
                bins.append({
                    "bin_index": idx,
                    "material_code": code,
                    "material_name": name_zh or code,
                    "remaining": remaining,
                    "capacity": capacity,
                    "unit": bh.get("unit", ""),
                    "pct": pct,
                    "threshold_low_pct": bh.get("threshold_low_pct", ""),
                    "is_low": bool(low_set),
                })
            except Exception:
                continue
        # sort by index numeric if possible
        def _key(b):
            try: return int(b.get("bin_index") or 0)
            except: return 0
        bins.sort(key=_key)
        return bins
