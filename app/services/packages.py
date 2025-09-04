from typing import Dict, Any, List
from ..utils.extensions import redis_cli, jset
from ..utils.keys import ts


def k_pkg(pkg_id: str) -> str:
    return f"cm:dict:package:{pkg_id}"

def k_pkg_all() -> str:
    return "cm:dict:package:all"


class PackageService:
    @staticmethod
    def upsert(pkg_id: str, data: Dict[str, Any]):
        r = redis_cli.r
        h = {
            "id": pkg_id,
            "name": data.get("name", pkg_id),
            "md5": data.get("md5", ""),
            "size": str(data.get("size") or 0),
            "uploaded_ts": str(ts()),
            "meta_json": jset(data.get("meta") or {}),
        }
        r.hset(k_pkg(pkg_id), mapping=h)
        r.sadd(k_pkg_all(), pkg_id)
        return h

    @staticmethod
    def list_all():
        r = redis_cli.r
        res = []
        for pid in r.smembers(k_pkg_all()):
            res.append(r.hgetall(k_pkg(pid)))
        return res

    @staticmethod
    def dispatch(pkg_id: str, device_ids: List[str]):
        from .commands import CommandService
        payload = {"package_id": pkg_id}
        return CommandService.dispatch_batch(device_ids, "upgrade", payload, note=f"Dispatch package {pkg_id}")
