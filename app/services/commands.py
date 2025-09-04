import uuid
from typing import Dict, Any, List, Tuple
from ..utils.extensions import redis_cli, jset, jget
from ..utils.keys import (
    k_cmd_hash, k_cmd_pending_q, k_cmd_inflight, ts,
    k_audit_stream, k_batch, k_batch_cmds
)
import json, io, csv


class CommandService:
    @staticmethod
    def enqueue(device_id: str, cmd_type: str, payload: Dict[str, Any] | None = None, note: str | None = None, batch_id: str | None = None) -> str:
        r = redis_cli.r
        cmd_id = str(uuid.uuid4())
        h = {
            "type": cmd_type,
            "payload_json": jset(payload or {}),
            "status": "pending",
            "issued_ts": str(ts()),
            "sent_ts": "",
            "result_ts": "",
            "result_payload_json": "",
            "attempts": "0",
            "max_attempts": "3",
            "last_error": "",
            "batch_id": batch_id or "",
        }
        r.hset(k_cmd_hash(device_id, cmd_id), mapping=h)
        r.lpush(k_cmd_pending_q(device_id), cmd_id)
        return cmd_id

    @staticmethod
    def claim(device_id: str, limit: int = 1) -> List[Dict[str, Any]]:
        r = redis_cli.r
        result = []
        tries = 0
        for _ in range(max(1, min(limit, 20))):
            cmd_id = r.rpop(k_cmd_pending_q(device_id))
            if not cmd_id:
                break
            key = k_cmd_hash(device_id, cmd_id)
            ch = r.hgetall(key)
            # skip canceled
            if ch and (ch.get("status") == "canceled"):
                continue
            # paused batch? push back and skip
            bid = ch.get("batch_id") if ch else ""
            if bid:
                b = r.hgetall(k_batch(bid)) or {}
                if (b.get("paused") == "1"):
                    r.lpush(k_cmd_pending_q(device_id), cmd_id)
                    tries += 1
                    if tries > 5:
                        break
                    continue
            r.hset(key, mapping={"status": "sent", "sent_ts": str(ts())})
            r.zadd(k_cmd_inflight(device_id), {cmd_id: ts()})
            h = ch or r.hgetall(key)
            h["id"] = cmd_id
            result.append(h)
        return result

    @staticmethod
    def ack(device_id: str, cmd_id: str, status: str, result_payload: Dict[str, Any] | None = None, error: str | None = None):
        r = redis_cli.r
        key = k_cmd_hash(device_id, cmd_id)
        if not r.exists(key):
            return False
        mapping = {
            "status": status,
            "result_ts": str(ts()),
            "result_payload_json": jset(result_payload or {}),
            "last_error": error or "",
        }
        r.hset(key, mapping=mapping)
        # update batch counters best-effort
        try:
            bid = r.hget(key, "batch_id")
            if bid:
                r.hincrby(k_batch(bid), f"count_{status}", 1)
        except Exception:
            pass
        r.zrem(k_cmd_inflight(device_id), cmd_id)
        r.xadd(k_audit_stream(), {"action": "cmd_ack", "actor": device_id, "target_id": cmd_id, "summary": status, "ts": ts()})
        return True

    @staticmethod
    def list_by_device(device_id: str, limit: int = 50):
        r = redis_cli.r
        arr = []
        # naive scan on device scope
        prefix = f"cm:dev:{device_id}:cmd:"
        for key in r.scan_iter(match=prefix+"*"):
            if key.endswith(":inflight"):
                continue
            if key.endswith(":pending"):
                continue
            h = r.hgetall(key)
            if not h:
                continue
            h["id"] = key.split(":")[-1]
            arr.append(h)
            if len(arr) >= limit:
                break
        return arr

    @staticmethod
    def recycle_inflight(max_age_sec: int = 60):
        r = redis_cli.r
        # For all devices: naive scan inflight keys
        for key in r.scan_iter(match="cm:dev:*:cmd:inflight"):
            now = ts()
            stale = []
            for member, score in r.zrange(key, 0, -1, withscores=True):
                if now - int(score) >= max_age_sec:
                    stale.append(member)
            if not stale:
                continue
            # requeue
            parts = key.split(":")
            device_id = parts[2]
            for cmd_id in stale:
                ch = r.hgetall(k_cmd_hash(device_id, cmd_id))
                attempts = int(ch.get("attempts", "0")) + 1
                max_attempts = int(ch.get("max_attempts", "3"))
                if attempts < max_attempts:
                    r.hset(k_cmd_hash(device_id, cmd_id), mapping={"status": "pending", "attempts": str(attempts)})
                    r.lpush(k_cmd_pending_q(device_id), cmd_id)
                else:
                    r.hset(k_cmd_hash(device_id, cmd_id), mapping={"status": "fail", "attempts": str(attempts), "last_error": "timeout"})
                r.zrem(key, cmd_id)

    @staticmethod
    def dispatch_batch(device_ids: List[str], command_type: str, payload: Dict[str, Any] | None, note: str | None) -> Dict[str, Any]:
        import uuid
        batch_id = str(uuid.uuid4())
        r = redis_cli.r
        now = ts()
        meta = {"id": batch_id, "type": command_type, "note": note or "", "created_ts": str(now), "status": "queued", "creator": "admin", "tag": "", "paused": "0", "max_concurrency": "0", "count_total": str(len(device_ids))}
        r.hset(k_batch(batch_id), mapping=meta)
        created = 0
        for d in device_ids:
            cmd_id = CommandService.enqueue(d, command_type, payload, note, batch_id=batch_id)
            r.hset(k_batch_cmds(batch_id), mapping={cmd_id: d})
            created += 1
        r.xadd(k_audit_stream(), {"action": "dispatch_create", "actor": "admin", "target_id": batch_id, "ts": now, "summary": command_type})
        return {"batch_id": batch_id, "count": created}

    @staticmethod
    def list_batches(from_ts: int | None = None, to_ts: int | None = None, type: str | None = None, status: str | None = None, creator: str | None = None, tag: str | None = None, q: str | None = None, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        r = redis_cli.r
        page = max(1, int(page or 1)); page_size = max(1, min(100, int(page_size or 20)))
        arr = []
        for key in r.scan_iter(match="cm:batch:*"):
            if key.endswith(":cmds"):
                continue
            h = r.hgetall(key)
            if not h:
                continue
            try:
                cts = int(h.get("created_ts") or 0)
            except Exception:
                cts = 0
            if from_ts and cts < int(from_ts):
                continue
            if to_ts and cts > int(to_ts):
                continue
            if type and (h.get("type") != type):
                continue
            if status and (h.get("status") != status):
                continue
            if creator and (h.get("creator") != creator):
                continue
            if tag and (h.get("tag") != tag):
                continue
            if q:
                blob = json.dumps(h, ensure_ascii=False).lower()
                if q.lower() not in blob:
                    continue
            arr.append(h)
        # sort by created_ts desc
        def _score(o):
            try:
                return int(o.get("created_ts") or 0)
            except Exception:
                return 0
        arr.sort(key=_score, reverse=True)
        total = len(arr)
        s=(page-1)*page_size; e=s+page_size
        return {"items": arr[s:e], "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def get_batch(batch_id: str) -> Dict[str, Any]:
        r = redis_cli.r
        info = r.hgetall(k_batch(batch_id))
        cmds = r.hgetall(k_batch_cmds(batch_id))
        # derive counters
        counts = {"success": 0, "fail": 0, "sent": 0, "pending": 0, "canceled": 0}
        for cmd_id, did in (cmds or {}).items():
            ch = r.hgetall(k_cmd_hash(did, cmd_id))
            st = (ch.get("status") or "pending") if ch else "pending"
            if st in counts:
                counts[st] += 1
        info = info or {}
        info.setdefault("count_total", str(len(cmds or {})))
        return {"info": info, "counts": counts, "cmds": cmds}

    @staticmethod
    def list_batch_items(batch_id: str, status: str | None = None, device_id: str | None = None, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        r = redis_cli.r
        page = max(1, int(page or 1)); page_size = max(1, min(200, int(page_size or 50)))
        cmds = r.hgetall(k_batch_cmds(batch_id)) or {}
        rows = []
        for cmd_id, did in cmds.items():
            if device_id and did != device_id:
                continue
            ch = r.hgetall(k_cmd_hash(did, cmd_id))
            if not ch:
                continue
            st = ch.get("status")
            if status and st != status:
                continue
            rows.append({
                "item_id": cmd_id,
                "device_id": did,
                "type": ch.get("type"),
                "status": st,
                "attempts": ch.get("attempts"),
                "issued_ts": ch.get("issued_ts"),
                "sent_ts": ch.get("sent_ts"),
                "result_ts": ch.get("result_ts"),
                "last_error": ch.get("last_error"),
            })
        # sort desc by issued_ts
        def _score(o):
            try:
                return int(o.get("issued_ts") or 0)
            except Exception:
                return 0
        rows.sort(key=_score, reverse=True)
        total = len(rows)
        s=(page-1)*page_size; e=s+page_size
        return {"items": rows[s:e], "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def export_batch(batch_id: str, fmt: str = 'csv') -> Tuple[str, str, str]:
        data = CommandService.list_batch_items(batch_id, page=1, page_size=10000)
        rows = data.get('items') or []
        if fmt == 'json':
            return json.dumps(rows, ensure_ascii=False), 'application/json', f'batch-{batch_id}.json'
        headers = set()
        for r in rows:
            for k in (r or {}).keys():
                headers.add(k)
        preferred = ["item_id","device_id","type","status","attempts","issued_ts","sent_ts","result_ts","last_error"]
        cols = [c for c in preferred if c in headers] + sorted([h for h in headers if h not in preferred])
        sio = io.StringIO(); w=csv.writer(sio); w.writerow(cols)
        for r in rows:
            w.writerow([ (r.get(c) if isinstance(r, dict) else "") for c in cols ])
        return sio.getvalue(), 'text/csv', f'batch-{batch_id}.csv'

    @staticmethod
    def create_batch(batch_type: str, device_ids: List[str], payload: Dict[str, Any] | None, options: Dict[str, Any] | None, tag: str | None, note: str | None, dedup_key: str | None, creator: str) -> Dict[str, Any]:
        import uuid
        batch_id = str(uuid.uuid4())
        r = redis_cli.r
        now = ts()
        meta = {
            "id": batch_id,
            "type": batch_type,
            "note": note or "",
            "tag": tag or "",
            "created_ts": str(now),
            "status": "queued",
            "creator": creator or "admin",
            "paused": "0",
            "max_concurrency": str((options or {}).get('max_concurrency') or 0),
            "retry": jset({k:v for k,v in (options or {}).items() if k in ('retry','max_attempts','timeout_s')}),
            "dedup_key": dedup_key or "",
            "count_total": str(len(device_ids))
        }
        r.hset(k_batch(batch_id), mapping=meta)
        for d in device_ids:
            cmd_id = CommandService.enqueue(d, batch_type, payload, note, batch_id=batch_id)
            r.hset(k_batch_cmds(batch_id), mapping={cmd_id: d})
        r.xadd(k_audit_stream(), {"action": "dispatch_create", "actor": creator or 'admin', "target_id": batch_id, "ts": now, "summary": batch_type})
        return {"batch_id": batch_id, "count": len(device_ids)}

    @staticmethod
    def batch_retry_failed(batch_id: str) -> int:
        r = redis_cli.r
        cmds = r.hgetall(k_batch_cmds(batch_id)) or {}
        n=0
        for cmd_id, did in cmds.items():
            ch = r.hgetall(k_cmd_hash(did, cmd_id))
            if not ch or ch.get('status') != 'fail':
                continue
            r.hset(k_cmd_hash(did, cmd_id), mapping={"status":"pending"})
            r.lpush(k_cmd_pending_q(did), cmd_id)
            n+=1
        r.xadd(k_audit_stream(), {"action": "dispatch_retry", "actor": "admin", "target_id": batch_id, "ts": ts(), "summary": str(n)})
        return n

    @staticmethod
    def batch_cancel(batch_id: str) -> int:
        r = redis_cli.r
        r.hset(k_batch(batch_id), mapping={"status":"canceled"})
        cmds = r.hgetall(k_batch_cmds(batch_id)) or {}
        n=0
        for cmd_id, did in cmds.items():
            ch = r.hgetall(k_cmd_hash(did, cmd_id))
            if not ch:
                continue
            if ch.get('status') in ('success','fail','canceled'):
                continue
            r.hset(k_cmd_hash(did, cmd_id), mapping={"status":"canceled"})
            n+=1
        r.xadd(k_audit_stream(), {"action": "dispatch_cancel", "actor": "admin", "target_id": batch_id, "ts": ts(), "summary": str(n)})
        return n

    @staticmethod
    def batch_pause(batch_id: str):
        redis_cli.r.hset(k_batch(batch_id), mapping={"paused":"1"})
        redis_cli.r.xadd(k_audit_stream(), {"action": "dispatch_pause", "actor": "admin", "target_id": batch_id, "ts": ts()})

    @staticmethod
    def batch_resume(batch_id: str):
        redis_cli.r.hset(k_batch(batch_id), mapping={"paused":"0"})
        redis_cli.r.xadd(k_audit_stream(), {"action": "dispatch_resume", "actor": "admin", "target_id": batch_id, "ts": ts()})

    @staticmethod
    def batch_set_concurrency(batch_id: str, max_concurrency: int):
        redis_cli.r.hset(k_batch(batch_id), mapping={"max_concurrency": str(max_concurrency)})
        redis_cli.r.xadd(k_audit_stream(), {"action": "dispatch_update", "actor": "admin", "target_id": batch_id, "ts": ts(), "summary": f"concurrency={max_concurrency}"})

    @staticmethod
    def retry_item(batch_id: str, item_id: str) -> bool:
        r = redis_cli.r
        did = redis_cli.r.hget(k_batch_cmds(batch_id), item_id)
        if not did:
            return False
        ch = r.hgetall(k_cmd_hash(did, item_id))
        if not ch:
            return False
        r.hset(k_cmd_hash(did, item_id), mapping={"status":"pending"})
        r.lpush(k_cmd_pending_q(did), item_id)
        redis_cli.r.xadd(k_audit_stream(), {"action": "dispatch_retry", "actor": "admin", "target_id": batch_id, "ts": ts(), "summary": item_id})
        return True
