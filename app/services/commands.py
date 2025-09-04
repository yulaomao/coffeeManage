import uuid
from typing import Dict, Any, List
from ..utils.extensions import redis_cli, jset
from ..utils.keys import (
    k_cmd_hash, k_cmd_pending_q, k_cmd_inflight, ts,
    k_audit_stream, k_batch, k_batch_cmds
)


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
        for _ in range(max(1, min(limit, 20))):
            cmd_id = r.rpop(k_cmd_pending_q(device_id))
            if not cmd_id:
                break
            key = k_cmd_hash(device_id, cmd_id)
            r.hset(key, mapping={"status": "sent", "sent_ts": str(ts())})
            r.zadd(k_cmd_inflight(device_id), {cmd_id: ts()})
            h = r.hgetall(key)
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
        r.hset(k_batch(batch_id), mapping={"id": batch_id, "type": command_type, "note": note or "", "created_ts": ts()})
        for d in device_ids:
            cmd_id = CommandService.enqueue(d, command_type, payload, note, batch_id=batch_id)
            r.hset(k_batch_cmds(batch_id), mapping={cmd_id: d})
        return {"batch_id": batch_id, "count": len(device_ids)}

    @staticmethod
    def list_batches(limit: int = 50):
        r = redis_cli.r
        # naive scan
        arr = []
        for key in r.scan_iter(match="cm:batch:*"):
            if key.endswith(":cmds"):
                continue
            arr.append(r.hgetall(key))
            if len(arr) >= limit:
                break
        return arr

    @staticmethod
    def get_batch(batch_id: str):
        r = redis_cli.r
        info = r.hgetall(k_batch(batch_id))
        cmds = r.hgetall(k_batch_cmds(batch_id))
        return {"info": info, "cmds": cmds}
