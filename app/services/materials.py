from typing import Dict, Any
from ..utils.extensions import redis_cli, jset, jget
from ..utils.keys import k_dict_material, k_dict_material_all


class MaterialService:
    @staticmethod
    def upsert(code: str, data: Dict[str, Any]):
        r = redis_cli.r
        key = k_dict_material(code)
        r.hset(key, mapping={
            "code": code,
            "name_i18n_json": jset(data.get("name_i18n") or {}),
            "unit": data.get("unit", "g"),
            "meta_json": jset(data.get("meta") or {}),
        })
        r.sadd(k_dict_material_all(), code)
        return r.hgetall(key)

    @staticmethod
    def list_all():
        r = redis_cli.r
        arr = []
        for code in r.smembers(k_dict_material_all()):
            arr.append(r.hgetall(k_dict_material(code)))
        return arr
