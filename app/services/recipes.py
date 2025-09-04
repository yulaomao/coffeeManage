from typing import Dict, Any, List, Tuple
import json
from ..utils.extensions import redis_cli, jget, jset
from ..utils.keys import (
    k_dict_recipe, k_dict_recipe_enabled, k_dict_recipe_all,
    k_dev, k_menu_cat, k_menu_cat_items, k_menu_item,
    k_audit_stream, ts, k_recipe_pkg, k_dev_recipes_active,
    k_dict_material,
)
from ..utils.rate_limit import check_rate, RateLimited
from .materials import MaterialService
from .commands import CommandService


class RecipeService:
    MAX_STEP_MS = 120_000

    @staticmethod
    def _validate_schema(schema: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        # Normalize
        name_i18n = schema.get("name_i18n") or jget(schema.get("name_i18n_json")) or {}
        if not isinstance(name_i18n, dict):
            name_i18n = {}
        ingredients = schema.get("ingredients") or []
        steps = schema.get("steps") or []
        options_schema = schema.get("options_schema") or jget(schema.get("options_schema_json")) or {}
        allergens_csv = schema.get("allergens_csv") or ""
        default_price_cents = schema.get("default_price_cents")
        yield_ml = schema.get("yield_ml")
        extra_json = schema.get("extra_json") or jget(schema.get("extra_json_json")) or {}

        # Validate ingredients
        for ing in ingredients:
            code = (ing.get("material") or ing.get("material_code") or "").strip()
            if not code:
                return False, "INVALID_ING:material_code", {}
            amt = float(ing.get("amount") or 0)
            if amt <= 0:
                return False, "INVALID_ING:amount", {}
            unit = (ing.get("unit") or "").strip()
            if unit not in ("g","ml","pcs"):
                return False, "INVALID_ING:unit", {}
        # Validate steps
        total_ms = 0
        seq_expected = 1
        for st in steps:
            seq = int(st.get("seq") or seq_expected)
            if seq != seq_expected:
                return False, "INVALID_STEP:seq", {}
            seq_expected += 1
            stype = st.get("type")
            if stype not in ("brew","grind","steam","mix","wait"):
                return False, "INVALID_STEP:type", {}
            params = st.get("params") or {}
            t = int(params.get("time_ms") or 0)
            if t <= 0:
                return False, "INVALID_STEP:time_ms", {}
            total_ms += t
        if total_ms > RecipeService.MAX_STEP_MS:
            return False, "STEP_TIME_EXCEEDED", {}
        normalized = {
            "name_i18n": name_i18n,
            "ingredients": ingredients,
            "steps": steps,
            "options_schema": options_schema,
            "default_price_cents": default_price_cents,
            "yield_ml": yield_ml,
            "allergens_csv": allergens_csv,
            "extra_json": extra_json,
        }
        return True, "", normalized

    @staticmethod
    def upsert(recipe_id: str, data: Dict[str, Any]):
        r = redis_cli.r
        exists = r.exists(k_dict_recipe(recipe_id))
        enabled = bool(data.get("enabled", True))
        schema = data.get("schema") or {}
        ok, msg, norm = RecipeService._validate_schema(schema)
        if not ok:
            raise ValueError(msg)
        # check materials existence (soft check; collect missing)
        missing = []
        for ing in norm.get("ingredients", []):
            code = ing.get("material") or ing.get("material_code")
            if code and not redis_cli.r.exists(k_dict_material(code)):
                missing.append(code)
        # strict validation when enabled flag is true (to避免上架无物料)
        if enabled and missing:
            raise ValueError(f"MATERIALS_MISSING:{','.join(sorted(set(missing)))}")
        key = k_dict_recipe(recipe_id)
        prev = r.hgetall(key) if exists else {}
        version = str(int(prev.get("version", "0")) + 1)
        h = {
            "id": recipe_id,
            "enabled": "1" if enabled else "0",
            "schema_json": jset(norm),
            "name": (norm.get("name_i18n") or {}).get("zh") or data.get("name") or recipe_id,
            "default_price_cents": str(norm.get("default_price_cents") or ""),
            "version": version,
            "updated_ts": str(ts()),
        }
        r.hset(key, mapping=h)
        r.sadd(k_dict_recipe_all(), recipe_id)
        if enabled:
            r.sadd(k_dict_recipe_enabled(), recipe_id)
        else:
            r.srem(k_dict_recipe_enabled(), recipe_id)
        r.xadd(k_audit_stream(), {"action": ("recipe_update" if exists else "recipe_create"), "target_id": recipe_id, "ts": ts(), "summary": h["name"]})
        return {**h, "missing_materials": missing}

    @staticmethod
    def get(recipe_id: str) -> Dict[str, Any]:
        return redis_cli.r.hgetall(k_dict_recipe(recipe_id))

    @staticmethod
    def list(query: str | None = None, enabled: str | None = None, tags: str | None = None, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        r = redis_cli.r
        q = (query or "").lower().strip()
        tagset = set([t.strip().lower() for t in (tags or "").split(',') if t.strip()])
        rows: List[Dict[str, Any]] = []
        for rid in r.smembers(k_dict_recipe_all()):
            h = r.hgetall(k_dict_recipe(rid))
            if not h:
                continue
            if enabled in ("1","0","true","false","enabled","disabled"):
                want = "1" if enabled in ("1","true","enabled") else "0"
                if (h.get("enabled") or "1") != want:
                    continue
            if q:
                name_zh = (jget(h.get("schema_json")) or {}).get("name_i18n", {}).get("zh", "")
                if q not in (h.get("id") or '').lower() and q not in (name_zh or '').lower() and q not in (h.get("name") or '').lower():
                    continue
            if tagset:
                sj = jget(h.get("schema_json")) or {}
                tags_csv = ",".join(sj.get("tags", [])) if isinstance(sj.get("tags"), list) else (sj.get("tags_csv") or "")
                ht = set([t.strip().lower() for t in tags_csv.split(',') if t.strip()])
                if not tagset.issubset(ht):
                    continue
            rows.append(h)
        def score(o):
            try: return int(o.get("updated_ts") or 0)
            except: return 0
        rows.sort(key=score, reverse=True)
        total = len(rows)
        page = max(1, int(page or 1)); page_size = max(1, min(int(page_size or 20), 100))
        start=(page-1)*page_size; end=start+page_size
        items = rows[start:end]
        # enrich usage counts
        for it in items:
            rid = it.get('id')
            u = RecipeService.usage(rid)
            it['menu_refs_count'] = len(u.get('menu_refs', []))
            it['devices_active_count'] = len(u.get('devices_active', []))
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def delete(recipe_id: str, force: bool = False) -> bool:
        r = redis_cli.r
        u = RecipeService.usage(recipe_id)
        if not force and (u['menu_refs'] or u['devices_active']):
            raise ValueError("REFERENCED")
        r.delete(k_dict_recipe(recipe_id))
        r.srem(k_dict_recipe_all(), recipe_id)
        r.srem(k_dict_recipe_enabled(), recipe_id)
        r.xadd(k_audit_stream(), {"action": "recipe_delete", "target_id": recipe_id, "ts": ts()})
        return True

    @staticmethod
    def usage(recipe_id: str) -> Dict[str, Any]:
        r = redis_cli.r
        # menu refs across devices
        menu_refs = []
        for key in r.scan_iter(match="cm:dev:*:menu:cat:*:items"):
            parts = key.split(":")
            device_id = parts[2]
            cat_id = parts[6] if len(parts) > 6 else parts[-2]
            for item_id in r.zrange(key, 0, -1):
                ih = r.hgetall(k_menu_item(device_id, item_id))
                if ih.get("recipe_id") == recipe_id:
                    ch = r.hgetall(k_menu_cat(device_id, cat_id))
                    name = (jget(ih.get("name_i18n_json")) or {}).get('zh') or ih.get('name') or ''
                    menu_refs.append({"device_id": device_id, "cat_id": cat_id, "item_id": item_id, "name": name})
        # devices active set
        devices_active = []
        for key in r.scan_iter(match="cm:dev:*:recipes:active"):
            device_id = key.split(":")[2]
            if r.sismember(key, recipe_id):
                devices_active.append(device_id)
        return {"menu_refs": menu_refs, "devices_active": devices_active}

    @staticmethod
    def publish(recipe_id: str) -> Dict[str, Any]:
        r = redis_cli.r
        h = r.hgetall(k_dict_recipe(recipe_id))
        if not h:
            raise KeyError(recipe_id)
        version = h.get("version", "1")
        # build a package snapshot
        pkg_key = k_recipe_pkg(recipe_id, version)
        r.hset(pkg_key, mapping={"id": recipe_id, "version": version, "schema_json": h.get("schema_json"), "built_ts": str(ts())})
        r.xadd(k_audit_stream(), {"action": "recipe_publish", "target_id": recipe_id, "ts": ts(), "summary": version})
        return {"recipe_id": recipe_id, "version": version}

    @staticmethod
    def dispatch(recipe_id: str, device_ids: List[str]) -> Dict[str, Any]:
        payload = {"recipe_id": recipe_id}
        res = CommandService.dispatch_batch(device_ids, "recipe_update", payload, note=f"recipe {recipe_id}")
        # audit
        redis_cli.r.xadd(k_audit_stream(), {"action": "recipe_dispatch", "target_id": recipe_id, "ts": ts(), "summary": res.get('batch_id')})
        return res

    @staticmethod
    def export(ids: List[str] | None = None) -> Tuple[str, str]:
        r = redis_cli.r
        arr = []
        if not ids:
            ids = list(r.smembers(k_dict_recipe_all()))
        for rid in ids:
            arr.append(r.hgetall(k_dict_recipe(rid)))
        return (json.dumps(arr, ensure_ascii=False, indent=2), "application/json")

    @staticmethod
    def import_payload(strategy: str, payload, dry_run: bool = False) -> Dict[str, Any]:
        if strategy not in ("merge","overwrite"):
            raise ValueError("INVALID_ARGUMENT:strategy")
        if not isinstance(payload, list):
            raise ValueError("INVALID_ARGUMENT:payload")
        r = redis_cli.r
        created=0; updated=0; conflicts=[]; errors=[]; missing=set()
        details=[]
        for row in payload:
            rid = (row.get('id') or '').strip()
            if not rid:
                errors.append({"error":"id_required"}); continue
            exists = r.exists(k_dict_recipe(rid))
            if exists and strategy not in ("merge","overwrite"):
                conflicts.append(rid); continue
            schema = jget(row.get('schema_json')) or row.get('schema') or {}
            ok, msg, norm = RecipeService._validate_schema(schema)
            if not ok:
                errors.append({"id": rid, "error": msg}); continue
            for ing in norm.get('ingredients', []):
                code = ing.get('material') or ing.get('material_code')
                if code and not redis_cli.r.exists(k_dict_material(code)):
                    missing.add(code)
            if dry_run:
                details.append({"id": rid, "action": ("update" if exists else "create")})
            else:
                RecipeService.upsert(rid, {"enabled": row.get('enabled', True), "schema": norm})
                if exists: updated+=1
                else: created+=1
        report = {"to_create": sum(1 for d in details if d.get('action')=='create'), "to_update": sum(1 for d in details if d.get('action')=='update'), "conflicts": conflicts, "errors": errors, "missing_materials": sorted(list(missing))}
        if dry_run:
            return {"report": report, "applied": False}
        else:
            redis_cli.r.xadd(k_audit_stream(), {"action": "recipe_import", "ts": ts(), "summary": f"c{created}/u{updated}/m{len(missing)}"})
            return {"result": {"created": created, "updated": updated, "conflicts": conflicts, "errors": errors, "missing_materials": sorted(list(missing))}, "applied": True}

    @staticmethod
    def list_enabled():
        r = redis_cli.r
        return list(r.smembers(k_dict_recipe_enabled()))
