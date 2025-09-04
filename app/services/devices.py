from ..utils.extensions import redis_cli, jget, jset
from ..utils.keys import (
    k_device, ts, k_audit_stream, k_orders_by_ts, k_alarms_status, k_dict_material
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
        return {
            "device": h,
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
    def list_devices(limit: int = 200):
        r = redis_cli.r
        arr = []
        for key in r.scan_iter(match="cm:dev:*"):
            if ":" in key and key.count(":") > 2:
                continue
            h = r.hgetall(key)
            if not h:
                continue
            arr.append({
                "device_id": h.get("device_id"),
                "alias": h.get("alias", ""),
                "status": h.get("status", ""),
                "fw_version": h.get("fw_version", ""),
                "last_seen_ts": h.get("last_seen_ts", ""),
            })
            if len(arr) >= limit:
                break
        return arr

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
