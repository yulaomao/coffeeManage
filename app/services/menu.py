import re
from typing import Dict, Any, List
from ..utils.extensions import redis_cli, jget, jset
from ..utils.keys import (
    k_menu_meta, k_menu_cats, k_menu_cat, k_menu_cat_items,
    k_menu_item, k_menu_available, k_menu_seq_cat, k_menu_seq_item,
    ts, k_audit_stream, k_dict_recipe,
)
from ..utils.rate_limit import check_rate, RateLimited
from flask import current_app

TIME_RE = re.compile(r"^\d{2}:\d{2}$")


class MenuService:
    @staticmethod
    def _ensure_meta(r, device_id: str) -> Dict[str, Any]:
        kmeta = k_menu_meta(device_id)
        meta = r.hgetall(kmeta)
        if not meta:
            meta = {"version": "1", "status": "draft", "updated_ts": str(ts())}
            r.hset(kmeta, mapping=meta)
        return meta

    @staticmethod
    def _next_cat_id(r, device_id: str) -> str:
        return str(r.incr(k_menu_seq_cat(device_id)))

    @staticmethod
    def _next_item_id(r, device_id: str) -> str:
        return str(r.incr(k_menu_seq_item(device_id)))

    @staticmethod
    def get_full_menu(device_id: str) -> Dict[str, Any]:
        r = redis_cli.r
        meta = MenuService._ensure_meta(r, device_id)
        cats = []
        for cat_id, score in r.zrange(k_menu_cats(device_id), 0, -1, withscores=True):
            cat_h = r.hgetall(k_menu_cat(device_id, cat_id))
            items = []
            for item_id, _ in r.zrange(k_menu_cat_items(device_id, cat_id), 0, -1, withscores=True):
                items.append(r.hgetall(k_menu_item(device_id, item_id)))
            cats.append({"id": cat_id, **cat_h, "items": items, "sort_order": int(score)})
        return {"meta": meta, "categories": cats}

    @staticmethod
    def get_available_items(device_id: str) -> List[str]:
        r = redis_cli.r
        # 简化：直接读派生集合，如为空则现算一遍
        key = k_menu_available(device_id)
        ids = list(r.smembers(key))
        if ids:
            return ids
        MenuService._recompute_availability(device_id)
        return list(r.smembers(key))

    @staticmethod
    def _recompute_availability(device_id: str):
        r = redis_cli.r
        key = k_menu_available(device_id)
        r.delete(key)
        # 条件：visibility=visible 且 schedule 命中（简化）
        for cat_id in r.zrange(k_menu_cats(device_id), 0, -1):
            for item_id in r.zrange(k_menu_cat_items(device_id, cat_id), 0, -1):
                h = r.hgetall(k_menu_item(device_id, item_id))
                if not h:
                    continue
                if h.get("visibility", "visible") != "visible":
                    continue
                sch = jget(h.get("schedule_json"), None)
                if sch and not MenuService._schedule_hit(sch):
                    continue
                r.sadd(key, item_id)

    @staticmethod
    def _schedule_hit(sch: Dict[str, Any]) -> bool:
        # 仅校验格式并总是返回 True；详细命中逻辑可拓展
        if not sch:
            return True
        if "ranges" in sch:
            for t1, t2 in sch.get("ranges", []):
                if not (TIME_RE.match(t1) and TIME_RE.match(t2)):
                    return False
        return True

    @staticmethod
    def create_category(device_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = redis_cli.r
        limit = current_app.config["RATE_LIMITS"]["menu_write_per_min"]
        try:
            check_rate(r, f"rl:menu:cat:create:{device_id}", limit)
        except RateLimited:
            raise ValueError("RATE_LIMITED")
        name_i18n = payload.get("name_i18n")
        if not isinstance(name_i18n, dict):
            raise ValueError("INVALID_ARGUMENT:name_i18n")
        sort_order = int(payload.get("sort_order") or (r.zcard(k_menu_cats(device_id)) + 1))
        cat_id = MenuService._next_cat_id(r, device_id)
        ch = {
            "id": cat_id,
            "name_i18n_json": jset(name_i18n),
            "icon": payload.get("icon", ""),
            "sort_order": str(sort_order),
            "visible": "1",
            "updated_ts": str(ts()),
        }
        r.hset(k_menu_cat(device_id, cat_id), mapping=ch)
        r.zadd(k_menu_cats(device_id), {cat_id: sort_order})
        r.xadd(k_audit_stream(), {"action": "menu_cat_create", "actor": "admin", "target_id": device_id, "summary": cat_id, "ts": ts()})
        return ch

    @staticmethod
    def update_category(device_id: str, cat_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = redis_cli.r
        key = k_menu_cat(device_id, cat_id)
        if not r.exists(key):
            raise KeyError(cat_id)
        mapping = {"updated_ts": str(ts())}
        if "name_i18n" in payload:
            if not isinstance(payload["name_i18n"], dict):
                raise ValueError("INVALID_ARGUMENT:name_i18n")
            mapping["name_i18n_json"] = jset(payload["name_i18n"])
        if "icon" in payload:
            mapping["icon"] = payload.get("icon", "")
        if "visible" in payload:
            mapping["visible"] = "1" if payload.get("visible") else "0"
        if "sort_order" in payload:
            so = int(payload["sort_order"])
            mapping["sort_order"] = str(so)
            r.zadd(k_menu_cats(device_id), {cat_id: so})
        r.hset(key, mapping=mapping)
        return r.hgetall(key)

    @staticmethod
    def delete_category(device_id: str, cat_id: str, move_to: str | None):
        r = redis_cli.r
        items_key = k_menu_cat_items(device_id, cat_id)
        items = list(r.zrange(items_key, 0, -1))
        if items and not move_to:
            raise ValueError("MENU_CATEGORY_NOT_EMPTY")
        if items and move_to:
            for it in items:
                # move item to new cat end
                so = r.zcard(k_menu_cat_items(device_id, move_to)) + 1
                r.hset(k_menu_item(device_id, it), mapping={"cat_id": move_to, "sort_order": str(so)})
                r.zrem(items_key, it)
                r.zadd(k_menu_cat_items(device_id, move_to), {it: so})
        # remove category
        r.delete(k_menu_cat(device_id, cat_id))
        r.zrem(k_menu_cats(device_id), cat_id)

    @staticmethod
    def create_item(device_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = redis_cli.r
        limit = current_app.config["RATE_LIMITS"]["menu_write_per_min"]
        try:
            check_rate(r, f"rl:menu:item:create:{device_id}", limit)
        except RateLimited:
            raise ValueError("RATE_LIMITED")
        cat_id = str(payload.get("cat_id"))
        recipe_id = str(payload.get("recipe_id"))
        if not cat_id or not r.exists(k_menu_cat(device_id, cat_id)):
            raise ValueError("MENU_CATEGORY_NOT_FOUND")
        if not recipe_id:
            raise ValueError("INVALID_ARGUMENT:recipe_id")
        if not r.exists(k_dict_recipe(recipe_id)):
            raise ValueError("RECIPE_NOT_FOUND")
        item_id = MenuService._next_item_id(r, device_id)
        so = int(payload.get("sort_order") or (r.zcard(k_menu_cat_items(device_id, cat_id)) + 1))
        h = {
            "id": item_id,
            "cat_id": cat_id,
            "recipe_id": recipe_id,
            "name_i18n_json": jset(payload.get("name_i18n") or {}),
            "image_url": payload.get("image_url", ""),
            "price_cents_override": "" if payload.get("price_cents_override") is None else str(payload.get("price_cents_override")),
            "options_schema_json": jset(payload.get("options_schema") or {}),
            "badges_csv": ",".join(payload.get("badges", [])) if payload.get("badges") else "",
            "visibility": payload.get("visibility", "visible"),
            "schedule_json": jset(payload.get("schedule") or {}),
            "tags_csv": ",".join(payload.get("tags", [])) if payload.get("tags") else "",
            "sort_order": str(so),
            "updated_ts": str(ts()),
        }
        r.hset(k_menu_item(device_id, item_id), mapping=h)
        r.zadd(k_menu_cat_items(device_id, cat_id), {item_id: so})
        r.xadd(k_audit_stream(), {"action": "menu_item_create", "actor": "admin", "target_id": device_id, "summary": item_id, "ts": ts()})
        return h

    @staticmethod
    def update_item(device_id: str, item_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = redis_cli.r
        key = k_menu_item(device_id, item_id)
        if not r.exists(key):
            raise KeyError(item_id)
        h = r.hgetall(key)
        mapping = {"updated_ts": str(ts())}
        if "cat_id" in payload and payload["cat_id"] != h.get("cat_id"):
            old_cat = h.get("cat_id")
            new_cat = str(payload["cat_id"])
            if not r.exists(k_menu_cat(device_id, new_cat)):
                raise ValueError("MENU_CATEGORY_NOT_FOUND")
            # move
            r.zrem(k_menu_cat_items(device_id, old_cat), item_id)
            so = r.zcard(k_menu_cat_items(device_id, new_cat)) + 1
            r.zadd(k_menu_cat_items(device_id, new_cat), {item_id: so})
            mapping["cat_id"] = new_cat
            mapping["sort_order"] = str(so)
        # optional change: recipe_id
        if "recipe_id" in payload and payload["recipe_id"] != h.get("recipe_id"):
            new_rid = str(payload["recipe_id"])
            if not new_rid:
                raise ValueError("INVALID_ARGUMENT:recipe_id")
            if not r.exists(k_dict_recipe(new_rid)):
                raise ValueError("RECIPE_NOT_FOUND")
            mapping["recipe_id"] = new_rid
        for f in ["name_i18n", "options_schema", "schedule"]:
            if f in payload:
                mapping[f"{f}_json"] = jset(payload.get(f) or {})
        for f in ["image_url", "visibility"]:
            if f in payload:
                mapping[f] = payload.get(f, "")
        if "price_cents_override" in payload:
            v = payload["price_cents_override"]
            mapping["price_cents_override"] = "" if v is None else str(v)
        if "badges" in payload:
            mapping["badges_csv"] = ",".join(payload.get("badges") or [])
        if "tags" in payload:
            mapping["tags_csv"] = ",".join(payload.get("tags") or [])
        if "sort_order" in payload:
            so = int(payload["sort_order"])
            mapping["sort_order"] = str(so)
            r.zadd(k_menu_cat_items(device_id, mapping.get("cat_id") or h.get("cat_id")), {item_id: so})
        r.hset(key, mapping=mapping)
        return r.hgetall(key)

    @staticmethod
    def delete_item(device_id: str, item_id: str):
        r = redis_cli.r
        key = k_menu_item(device_id, item_id)
        h = r.hgetall(key)
        if not h:
            return
        r.zrem(k_menu_cat_items(device_id, h.get("cat_id")), item_id)
        r.delete(key)

    @staticmethod
    def set_visibility(device_id: str, item_id: str, vis: str):
        if vis not in ("visible", "hidden", "archived"):
            raise ValueError("INVALID_ARGUMENT:visibility")
        r = redis_cli.r
        key = k_menu_item(device_id, item_id)
        if not r.exists(key):
            raise ValueError("MENU_ITEM_NOT_FOUND")
        r.hset(key, mapping={"visibility": vis, "updated_ts": str(ts())})
        MenuService._recompute_availability(device_id)
        return r.hgetall(key)

    @staticmethod
    def set_schedule(device_id: str, item_id: str, sch: Dict[str, Any]):
        # 校验基础格式
        if not MenuService._schedule_hit(sch):
            raise ValueError("INVALID_SCHEDULE")
        r = redis_cli.r
        key = k_menu_item(device_id, item_id)
        if not r.exists(key):
            raise ValueError("MENU_ITEM_NOT_FOUND")
        r.hset(key, mapping={"schedule_json": jset(sch), "updated_ts": str(ts())})
        MenuService._recompute_availability(device_id)
        return r.hgetall(key)

    @staticmethod
    def set_price(device_id: str, item_id: str, price):
        r = redis_cli.r
        key = k_menu_item(device_id, item_id)
        if not r.exists(key):
            raise ValueError("MENU_ITEM_NOT_FOUND")
        v = "" if price is None else str(int(price))
        r.hset(key, mapping={"price_cents_override": v, "updated_ts": str(ts())})
        return r.hgetall(key)

    @staticmethod
    def publish(device_id: str):
        r = redis_cli.r
        meta = MenuService._ensure_meta(r, device_id)
        new_ver = str(int(meta.get("version", "1")) + 1)
        meta_update = {"version": new_ver, "status": "published", "updated_ts": str(ts())}
        r.hset(k_menu_meta(device_id), mapping=meta_update)
        MenuService._recompute_availability(device_id)
        r.xadd(k_audit_stream(), {"action": "menu_publish", "actor": "admin", "target_id": device_id, "summary": new_ver, "ts": ts()})
        return r.hgetall(k_menu_meta(device_id))

    @staticmethod
    def export_menu(device_id: str) -> Dict[str, Any]:
        menu = MenuService.get_full_menu(device_id)
        return menu

    @staticmethod
    def import_menu(device_id: str, menu_json: Dict[str, Any], strategy: str):
        if strategy not in ("overwrite", "merge"):
            raise ValueError("INVALID_ARGUMENT:strategy")
        if not isinstance(menu_json, dict):
            raise ValueError("INVALID_ARGUMENT:menu_json")
        r = redis_cli.r
        if strategy == "overwrite":
            # 清理现有结构
            for cat_id in r.zrange(k_menu_cats(device_id), 0, -1):
                for item_id in r.zrange(k_menu_cat_items(device_id, cat_id), 0, -1):
                    r.delete(k_menu_item(device_id, item_id))
                r.delete(k_menu_cat_items(device_id, cat_id))
                r.delete(k_menu_cat(device_id, cat_id))
            r.delete(k_menu_cats(device_id))
        # 重建
        cats = menu_json.get("categories", [])
        for idx, cat in enumerate(cats, start=1):
            cat_payload = {
                "name_i18n": jget(cat.get("name_i18n_json")) or cat.get("name_i18n") or {"zh": cat.get("name", "分类")},
                "icon": cat.get("icon", ""),
                "sort_order": cat.get("sort_order", idx),
            }
            c = MenuService.create_category(device_id, cat_payload)
            # items
            for jdx, it in enumerate(cat.get("items", []), start=1):
                it_payload = {
                    "cat_id": c["id"],
                    "recipe_id": it.get("recipe_id", ""),
                    "name_i18n": jget(it.get("name_i18n_json")) or {},
                    "image_url": it.get("image_url", ""),
                    "price_cents_override": int(it.get("price_cents_override")) if it.get("price_cents_override") else None,
                    "options_schema": jget(it.get("options_schema_json")) or {},
                    "badges": [b for b in (it.get("badges_csv", "").split(",") if it.get("badges_csv") else []) if b],
                    "visibility": it.get("visibility", "visible"),
                    "schedule": jget(it.get("schedule_json")) or {},
                    "tags": [t for t in (it.get("tags_csv", "").split(",") if it.get("tags_csv") else []) if t],
                    "sort_order": it.get("sort_order", jdx),
                }
                MenuService.create_item(device_id, it_payload)
        return MenuService.publish(device_id)

    @staticmethod
    def reorder_categories(device_id: str, arr: List[Dict[str, Any]]):
        r = redis_cli.r
        mapping = {}
        for obj in arr:
            cid = str(obj.get("cat_id"))
            so = int(obj.get("sort_order"))
            mapping[cid] = so
            r.hset(k_menu_cat(device_id, cid), mapping={"sort_order": str(so), "updated_ts": str(ts())})
        if mapping:
            r.zadd(k_menu_cats(device_id), mapping)

    @staticmethod
    def reorder_items(device_id: str, cat_id: str, arr: List[Dict[str, Any]]):
        r = redis_cli.r
        mapping = {}
        for obj in arr:
            iid = str(obj.get("item_id"))
            so = int(obj.get("sort_order"))
            mapping[iid] = so
            r.hset(k_menu_item(device_id, iid), mapping={"sort_order": str(so), "updated_ts": str(ts())})
        if mapping:
            r.zadd(k_menu_cat_items(device_id, cat_id), mapping)
