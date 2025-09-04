from typing import Dict, Any
from ..utils.extensions import redis_cli, jset
from ..utils.keys import k_dict_recipe, k_dict_recipe_enabled


class RecipeService:
    @staticmethod
    def upsert(recipe_id: str, data: Dict[str, Any]):
        r = redis_cli.r
        key = k_dict_recipe(recipe_id)
        r.hset(key, mapping={
            "id": recipe_id,
            "name": data.get("name", recipe_id),
            "enabled": "1" if data.get("enabled", True) else "0",
            "schema_json": jset(data.get("schema") or {}),
        })
        if data.get("enabled", True):
            r.sadd(k_dict_recipe_enabled(), recipe_id)
        else:
            r.srem(k_dict_recipe_enabled(), recipe_id)
        return r.hgetall(key)

    @staticmethod
    def publish(recipe_id: str):
        # placeholder for package/publish pipeline
        return {"id": recipe_id, "published": True}

    @staticmethod
    def list_enabled():
        r = redis_cli.r
        return list(r.smembers(k_dict_recipe_enabled()))
