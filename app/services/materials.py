from typing import Dict, Any, List, Tuple
from ..utils.extensions import redis_cli, jset, jget
from ..utils.keys import k_dict_material, k_dict_material_all, k_audit_stream
import re, json, csv, io
from datetime import datetime


class MaterialService:
    CODE_RE = re.compile(r"^[A-Za-z0-9_\-\./]{1,32}$")
    UNITS = {"g", "ml", "pcs", "other"}

    @staticmethod
    def _now() -> int:
        return int(datetime.utcnow().timestamp())

    @staticmethod
    def _audit(action: str, **fields):
        try:
            r = redis_cli.r
            payload = {"action": action, "ts": MaterialService._now()}
            payload.update({k: str(v) for k, v in fields.items() if v is not None})
            r.xadd(k_audit_stream(), payload)
        except Exception:
            pass

    @staticmethod
    def _validate(data: Dict[str, Any], is_update=False) -> Tuple[bool, str]:
        code = (data.get("code") or "").strip()
        if not is_update:
            if not code:
                return False, "INVALID_ARGUMENT:code_required"
            if not MaterialService.CODE_RE.match(code):
                return False, "INVALID_ARGUMENT:code_format"
        unit = (data.get("unit") or "g").strip()
        if unit not in MaterialService.UNITS:
            return False, "INVALID_ARGUMENT:unit"
        thr = data.get("default_threshold_low_pct")
        if thr is not None:
            try:
                t = float(thr)
                if t < 0 or t > 100:
                    return False, "INVALID_ARGUMENT:threshold"
            except Exception:
                return False, "INVALID_ARGUMENT:threshold"
        dens = data.get("density")
        if dens is not None and dens != "":
            try:
                d = float(dens)
                if d <= 0:
                    return False, "INVALID_ARGUMENT:density"
            except Exception:
                return False, "INVALID_ARGUMENT:density"
        return True, ""

    @staticmethod
    def upsert(code: str, data: Dict[str, Any]):
        r = redis_cli.r
        existed = bool(r.exists(k_dict_material(code)))
        ok, msg = MaterialService._validate({**data, "code": code}, is_update=existed)
        if not ok:
            raise ValueError(msg)
        key = k_dict_material(code)
        name_i18n = data.get("name_i18n") or {}
        tags = data.get("tags") or data.get("tags_csv") or ""
        if isinstance(tags, list):
            tags = ",".join([str(x).strip() for x in tags if str(x).strip()])
        meta = {
            "image_url": data.get("image_url") or "",
            "allergens_csv": data.get("allergens_csv") or "",
            "shelf_life_days": str(data.get("shelf_life_days") or ""),
            "notes": data.get("notes") or "",
        }
        r.hset(key, mapping={
            "code": code,
            "name_i18n_json": jset(name_i18n),
            "unit": data.get("unit", "g"),
            "default_threshold_low_pct": str(data.get("default_threshold_low_pct") if data.get("default_threshold_low_pct") is not None else ""),
            "density": str(data.get("density") or ""),
            "tags_csv": tags,
            "active": "1" if (data.get("active", True)) else "0",
            "meta_json": jset(meta),
            "updated_ts": str(MaterialService._now()),
        })
        r.sadd(k_dict_material_all(), code)
        MaterialService._audit("material_update" if existed else "material_create", code=code)
        return r.hgetall(key)

    @staticmethod
    def list_all():
        r = redis_cli.r
        arr = []
        for code in r.smembers(k_dict_material_all()):
            arr.append(r.hgetall(k_dict_material(code)))
        return arr

    @staticmethod
    def get(code: str) -> Dict[str, Any]:
        return redis_cli.r.hgetall(k_dict_material(code))

    @staticmethod
    def list(query: str | None = None, unit: str | None = None, tags: str | None = None, status: str | None = None, page: int = 1, page_size: int = 20):
        r = redis_cli.r
        items: List[Dict[str, Any]] = []
        q = (query or "").lower().strip()
        tags_set = set([t.strip().lower() for t in (tags or "").split(",") if t.strip()])
        for code in r.smembers(k_dict_material_all()):
            h = r.hgetall(k_dict_material(code))
            if not h:
                continue
            # filters
            if unit and (h.get("unit") != unit):
                continue
            if status in ("active", "archived"):
                want = "1" if status == "active" else "0"
                if (h.get("active") or "1") != want:
                    continue
            if q:
                name_zh = (jget(h.get("name_i18n_json")) or {}).get("zh", "")
                name_en = (jget(h.get("name_i18n_json")) or {}).get("en", "")
                if q not in (h.get("code") or "").lower() and q not in name_zh.lower() and q not in name_en.lower() and q not in (h.get("tags_csv") or "").lower():
                    continue
            if tags_set:
                ht = set([t.strip().lower() for t in (h.get("tags_csv") or "").split(",") if t.strip()])
                if not tags_set.issubset(ht):
                    continue
            items.append(h)
        def _score(o):
            try: return int(o.get("updated_ts") or 0)
            except: return 0
        items.sort(key=_score, reverse=True)
        total = len(items)
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]
        # usage counts for current page
        counts = MaterialService.usage_counts([it.get("code") for it in page_items])
        for it in page_items:
            c = it.get("code")
            uc = counts.get(c, {"recipes": 0, "bins": 0})
            it["usage_recipes"] = uc["recipes"]
            it["usage_bins"] = uc["bins"]
        return {"items": page_items, "total": total, "page": page, "page_size": page_size}

    @staticmethod
    def delete(code: str, force: bool = False) -> bool:
        # check references
        usage = MaterialService.usage(code)
        if not force and (usage["recipes"] or usage["bins"]):
            raise ValueError("REFERENCED")
        r = redis_cli.r
        r.delete(k_dict_material(code))
        r.srem(k_dict_material_all(), code)
        MaterialService._audit("material_delete", code=code)
        return True

    @staticmethod
    def usage(code: str) -> Dict[str, Any]:
        r = redis_cli.r
        # recipes scan
        recipes: List[Dict[str, Any]] = []
        for key in r.scan_iter(match="cm:dict:recipe:*"):
            # only process hash keys (skip sets like cm:dict:recipe:all)
            try:
                if r.type(key) != 'hash':
                    continue
            except Exception:
                continue
            h = r.hgetall(key)
            sj = jget(h.get("schema_json")) or {}
            # naive deep search for material_code == code
            found = False
            def walk(obj):
                nonlocal found
                if found:
                    return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("material", "material_code") and str(v) == code:
                            found = True; return
                        walk(v)
                elif isinstance(obj, list):
                    for it in obj:
                        walk(it)
            walk(sj)
            if found:
                recipes.append({"id": h.get("id"), "name": h.get("name")})
        # bins scan
        bins: List[Dict[str, Any]] = []
        for key in r.scan_iter(match="cm:dev:*:bin:*"):
            try:
                if r.type(key) != 'hash':
                    continue
            except Exception:
                continue
            bh = r.hgetall(key)
            if (bh.get("material_code") or "") == code:
                parts = key.split(":")
                did = parts[2]
                bin_index = parts[-1]
                remaining = bh.get("remaining")
                capacity = bh.get("capacity")
                unit = bh.get("unit")
                low = bool(r.sismember(f"cm:dev:{did}:bins:low", bin_index))
                bins.append({"device_id": did, "bin_index": bin_index, "remaining": remaining, "capacity": capacity, "unit": unit, "low": low})
        return {"recipes": recipes, "bins": bins}

    @staticmethod
    def usage_counts(codes: List[str]) -> Dict[str, Dict[str, int]]:
        r = redis_cli.r
        res = {c: {"recipes": 0, "bins": 0} for c in codes}
        if not codes:
            return res
        set_codes = set(codes)
        # recipes
        for key in r.scan_iter(match="cm:dict:recipe:*"):
            try:
                if r.type(key) != 'hash':
                    continue
            except Exception:
                continue
            h = r.hgetall(key)
            sj = jget(h.get("schema_json")) or {}
            # collect materials in this recipe
            found = set()
            def walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("material", "material_code") and v in set_codes:
                            found.add(v)
                        walk(v)
                elif isinstance(obj, list):
                    for it in obj:
                        walk(it)
            walk(sj)
            for c in found:
                res[c]["recipes"] += 1
        # bins
        for key in r.scan_iter(match="cm:dev:*:bin:*"):
            try:
                if r.type(key) != 'hash':
                    continue
            except Exception:
                continue
            bh = r.hgetall(key)
            c = bh.get("material_code")
            if c in set_codes:
                res[c]["bins"] += 1
        return res

    @staticmethod
    def replace(code: str, to_code: str, scope: str = "all") -> Dict[str, int]:
        r = redis_cli.r
        # validate target exists
        if not r.exists(k_dict_material(to_code)):
            raise ValueError("NOT_FOUND:to_code")
        changed_recipes = 0
        changed_bins = 0
        if scope in ("all", "recipes"):
            for key in r.scan_iter(match="cm:dict:recipe:*"):
                try:
                    if r.type(key) != 'hash':
                        continue
                except Exception:
                    continue
                h = r.hgetall(key)
                sj = jget(h.get("schema_json")) or {}
                before = json.dumps(sj, ensure_ascii=False)
                def walk(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k in ("material", "material_code") and v == code:
                                obj[k] = to_code
                            else:
                                walk(v)
                    elif isinstance(obj, list):
                        for it in obj:
                            walk(it)
                walk(sj)
                after = json.dumps(sj, ensure_ascii=False)
                if after != before:
                    r.hset(key, mapping={"schema_json": jset(sj)})
                    changed_recipes += 1
        if scope in ("all", "bins"):
            for key in r.scan_iter(match="cm:dev:*:bin:*"):
                try:
                    if r.type(key) != 'hash':
                        continue
                except Exception:
                    continue
                if r.hget(key, "material_code") == code:
                    r.hset(key, mapping={"material_code": to_code})
                    changed_bins += 1
        MaterialService._audit("material_replace", code=code, to_code=to_code, scope=scope, changed_recipes=changed_recipes, changed_bins=changed_bins)
        return {"changed_recipes": changed_recipes, "changed_bins": changed_bins}

    @staticmethod
    def export(codes: List[str] | None = None, fmt: str = "json") -> Tuple[str, str]:
        r = redis_cli.r
        data = []
        if not codes:
            codes = list(r.smembers(k_dict_material_all()))
        for c in codes:
            data.append(r.hgetall(k_dict_material(c)))
        MaterialService._audit("material_export", count=len(data))
        if fmt == "csv":
            # determine columns
            headers = set()
            for h in data:
                for k in h.keys():
                    headers.add(k)
            preferred = ["code","unit","name_i18n_json","tags_csv","active","default_threshold_low_pct","density","updated_ts","meta_json"]
            cols = [c for c in preferred if c in headers] + sorted([h for h in headers if h not in preferred])
            sio = io.StringIO()
            w = csv.writer(sio)
            w.writerow(cols)
            for h in data:
                w.writerow([h.get(c,"") for c in cols])
            return (sio.getvalue(), "text/csv")
        else:
            return (json.dumps(data, ensure_ascii=False, indent=2), "application/json")

    @staticmethod
    def import_payload(strategy: str, payload, dry_run: bool = False) -> Dict[str, Any]:
        # payload can be list[dict] or CSV text
        rows: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, str):
            # assume CSV
            reader = csv.DictReader(io.StringIO(payload))
            for row in reader:
                rows.append(row)
        else:
            raise ValueError("INVALID_ARGUMENT:payload")
        r = redis_cli.r
        created, updated, conflicts, errors = 0, 0, [], []
        details = []
        for row in rows:
            code = (row.get("code") or "").strip()
            if not code:
                errors.append({"code": code, "error": "INVALID_ARGUMENT:code_required"}); continue
            # merge normalize
            data = {
                "code": code,
                "name_i18n": row.get("name_i18n") if isinstance(row.get("name_i18n"), dict) else (json.loads(row.get("name_i18n_json", "{}")) if row.get("name_i18n_json") else {"zh": row.get("name_zh") or code, "en": row.get("name_en") or ""}),
                "unit": row.get("unit") or "g",
                "default_threshold_low_pct": row.get("default_threshold_low_pct"),
                "density": row.get("density"),
                "tags": row.get("tags") or row.get("tags_csv") or "",
                "active": (row.get("active") in (True, "1", "true", "TRUE", 1)),
                "image_url": (json.loads(row.get("meta_json","{}")).get("image_url") if row.get("meta_json") else row.get("image_url")),
                "allergens_csv": (json.loads(row.get("meta_json","{}")).get("allergens_csv") if row.get("meta_json") else row.get("allergens_csv")),
                "shelf_life_days": (json.loads(row.get("meta_json","{}")).get("shelf_life_days") if row.get("meta_json") else row.get("shelf_life_days")),
                "notes": (json.loads(row.get("meta_json","{}")).get("notes") if row.get("meta_json") else row.get("notes")),
            }
            exists = r.exists(k_dict_material(code))
            if exists and strategy == "overwrite":
                # ok to overwrite
                pass
            elif exists and strategy == "merge":
                # merge: only set provided fields
                pass
            elif exists and strategy not in ("merge","overwrite"):
                conflicts.append(code); continue
            try:
                if dry_run:
                    details.append({"code": code, "action": ("update" if exists else "create")})
                else:
                    MaterialService.upsert(code, data)
                    if exists: updated += 1
                    else: created += 1
            except ValueError as e:
                errors.append({"code": code, "error": str(e)})
        if dry_run:
            return {"report": {"to_create": sum(1 for d in details if d["action"]=="create"), "to_update": sum(1 for d in details if d["action"]=="update"), "conflicts": conflicts, "errors": errors, "count": len(rows)}, "applied": False}
        else:
            MaterialService._audit("material_import", created=created, updated=updated, conflicts=len(conflicts), errors=len(errors))
            return {"result": {"created": created, "updated": updated, "conflicts": conflicts, "errors": errors}, "applied": True}
