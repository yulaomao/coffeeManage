from flask import Blueprint, request, Response
from ..services.devices import DeviceService
from ..services.menu import MenuService
from ..services.commands import CommandService
from ..services.orders import OrderService
from ..services.materials import MaterialService
from ..services.recipes import RecipeService
from ..services.audit import AuditService
from ..services.packages import PackageService
from ..services.alarms import AlarmService
from ..utils.rbac import require_role
from ..utils.extensions import redis_cli
from ..utils.keys import k_menu_meta

api_v1_bp = Blueprint("api_v1", __name__)

def ok(data=None):
    return {"ok": True, "data": data}

def err(msg, code=400):
    return ({"ok": False, "error": msg}, code)

# Dashboard summary (minimal demo)
@api_v1_bp.get("/dashboard/summary")
@require_role(["admin", "ops", "viewer"])
def dashboard_summary():
    data = DeviceService.dashboard_summary()
    return ok(data)

@api_v1_bp.get("/dashboard/trends")
@require_role(["admin", "ops", "viewer"]) 
def dashboard_trends():
    days = int(request.args.get('days', 7))
    return ok(DeviceService.dashboard_trends(days))

@api_v1_bp.get("/dashboard/low-materials")
@require_role(["admin", "ops", "viewer"]) 
def dashboard_low_materials():
    limit = int(request.args.get('limit', 10))
    return ok(DeviceService.list_low_materials(limit_devices=limit))

# Devices
@api_v1_bp.get("/devices/<device_id>/summary")
@require_role(["admin", "ops", "viewer"])
def device_summary(device_id):
    return ok(DeviceService.get_summary(device_id))

@api_v1_bp.get("/devices")
@require_role(["admin", "ops", "viewer"]) 
def devices_list():
    status = request.args.get('status')
    query = request.args.get('query')
    page = request.args.get('page', 1)
    page_size = request.args.get('page_size', 20)
    return ok(DeviceService.list_devices(status=status, query=query, page=int(page), page_size=int(page_size)))

@api_v1_bp.post("/devices/<device_id>/sync_state")
@require_role(["admin", "ops"]) 
def sync_state(device_id):
    DeviceService.touch_device(device_id, request.remote_addr or "")
    return ok({"device_id": device_id})

# Orders
@api_v1_bp.get("/devices/<device_id>/orders")
@require_role(["admin", "ops", "viewer"]) 
def device_orders(device_id):
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    start_ts = request.args.get("from")
    end_ts = request.args.get("to")
    st = int(start_ts) if start_ts else None
    et = int(end_ts) if end_ts else None
    return ok(OrderService.list_device_orders(device_id, limit=limit, start_ts=st, end_ts=et, offset=offset))

@api_v1_bp.get("/devices/<device_id>/orders/export")
@require_role(["admin", "ops", "viewer"]) 
def device_orders_export(device_id):
    fmt = (request.args.get("format") or "csv").lower()
    start_ts = request.args.get("from")
    end_ts = request.args.get("to")
    st = int(start_ts) if start_ts else None
    et = int(end_ts) if end_ts else None
    rows = OrderService.list_device_orders(device_id, limit=10000, start_ts=st, end_ts=et)
    if fmt == "json":
        import json as _json
        payload = _json.dumps(rows, ensure_ascii=False)
        return Response(payload, mimetype="application/json", headers={"Content-Disposition": f"attachment; filename=orders-{device_id}.json"})
    # csv
    # collect headers
    headers = set()
    for r in rows:
        for k in (r or {}).keys():
            headers.add(k)
    preferred = ["order_id", "id", "status", "total_cents", "amount_cents", "server_ts", "device_ts"]
    cols = [c for c in preferred if c in headers] + sorted([h for h in headers if h not in preferred])
    # build csv
    import io, csv
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(cols)
    for r in rows:
        writer.writerow([ (r.get(c) if isinstance(r, dict) else "") for c in cols ])
    data = sio.getvalue()
    return Response(data, mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename=orders-{device_id}.csv"})

@api_v1_bp.get("/devices/<device_id>/bins")
@require_role(["admin", "ops", "viewer"]) 
def device_bins(device_id):
    return ok(DeviceService.list_bins(device_id))

# Commands
@api_v1_bp.post("/devices/<device_id>/commands")
@require_role(["admin", "ops"]) 
def device_command(device_id):
    payload = request.json or {}
    cmd_type = payload.get("type")
    body = payload.get("payload")
    note = payload.get("note")
    cmd_id = CommandService.enqueue(device_id, cmd_type, body, note)
    return ok({"command_id": cmd_id})

@api_v1_bp.get("/devices/<device_id>/commands")
@require_role(["admin", "ops", "viewer"]) 
def device_commands_list(device_id):
    limit = int(request.args.get("limit", 50))
    return ok(CommandService.list_by_device(device_id, limit))

# Menu read
@api_v1_bp.get("/devices/<device_id>/menu")
@require_role(["admin", "ops", "viewer"]) 
def get_menu(device_id):
    menu = MenuService.get_full_menu(device_id)
    return ok(menu)

@api_v1_bp.get("/devices/<device_id>/menu/available")
@require_role(["admin", "ops", "viewer"]) 
def get_available(device_id):
    return ok(MenuService.get_available_items(device_id))

# Menu categories CRUD
@api_v1_bp.post("/devices/<device_id>/menu/categories")
@require_role(["admin", "ops"]) 
def create_category(device_id):
    payload = request.json or {}
    # If-Match concurrency: require client to send current version
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        cat = MenuService.create_category(device_id, payload)
        return ok(cat)
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.put("/devices/<device_id>/menu/categories/<cat_id>")
@require_role(["admin", "ops"]) 
def update_category(device_id, cat_id):
    payload = request.json or {}
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        cat = MenuService.update_category(device_id, cat_id, payload)
        return ok(cat)
    except KeyError:
        return err("MENU_CATEGORY_NOT_FOUND", 404)

@api_v1_bp.delete("/devices/<device_id>/menu/categories/<cat_id>")
@require_role(["admin", "ops"]) 
def delete_category(device_id, cat_id):
    move_to = request.args.get("move_to")
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        MenuService.delete_category(device_id, cat_id, move_to)
        return ok()
    except ValueError as e:
        return err(str(e), 409)

# Items CRUD
@api_v1_bp.post("/devices/<device_id>/menu/items")
@require_role(["admin", "ops"]) 
def create_item(device_id):
    payload = request.json or {}
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        item = MenuService.create_item(device_id, payload)
        return ok(item)
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.put("/devices/<device_id>/menu/items/<item_id>")
@require_role(["admin", "ops"]) 
def update_item(device_id, item_id):
    payload = request.json or {}
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        item = MenuService.update_item(device_id, item_id, payload)
        return ok(item)
    except KeyError:
        return err("MENU_ITEM_NOT_FOUND", 404)

@api_v1_bp.delete("/devices/<device_id>/menu/items/<item_id>")
@require_role(["admin", "ops"]) 
def delete_item(device_id, item_id):
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    MenuService.delete_item(device_id, item_id)
    return ok()

# Visibility / schedule / price
@api_v1_bp.patch("/devices/<device_id>/menu/items/<item_id>/visibility")
@require_role(["admin", "ops"]) 
def patch_visibility(device_id, item_id):
    vis = (request.json or {}).get("visibility")
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        item = MenuService.set_visibility(device_id, item_id, vis)
        return ok(item)
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.put("/devices/<device_id>/menu/items/<item_id>/schedule")
@require_role(["admin", "ops"]) 
def put_schedule(device_id, item_id):
    sch = request.json or {}
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    try:
        item = MenuService.set_schedule(device_id, item_id, sch)
        return ok(item)
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.put("/devices/<device_id>/menu/items/<item_id>/price")
@require_role(["admin", "ops"]) 
def put_price(device_id, item_id):
    price = (request.json or {}).get("price_cents_override")
    if_match = request.headers.get("If-Match")
    if if_match is not None:
        cur_ver = redis_cli.r.hget(k_menu_meta(device_id), "version") or "1"
        if str(if_match) != str(cur_ver):
            return err("CONFLICT", 409)
    item = MenuService.set_price(device_id, item_id, price)
    return ok(item)

# Publish / export / import
@api_v1_bp.post("/devices/<device_id>/menu/publish")
@require_role(["admin", "ops"]) 
def publish_menu(device_id):
    meta = MenuService.publish(device_id)
    return ok(meta)

@api_v1_bp.get("/devices/<device_id>/menu/export")
@require_role(["admin", "ops", "viewer"]) 
def export_menu(device_id):
    return ok(MenuService.export_menu(device_id))

@api_v1_bp.post("/devices/<device_id>/menu/import")
@require_role(["admin", "ops"]) 
def import_menu(device_id):
    payload = request.json or {}
    strategy = payload.get("strategy", "overwrite")
    menu_json = payload.get("menu_json")
    try:
        meta = MenuService.import_menu(device_id, menu_json, strategy)
        return ok(meta)
    except ValueError as e:
        return err(str(e), 400)

# Materials
@api_v1_bp.get("/materials")
@require_role(["admin", "ops", "viewer"]) 
def materials_list():
    # optional filters and pagination
    query = request.args.get('query')
    unit = request.args.get('unit')
    tags = request.args.get('tags')
    status = request.args.get('status')  # active|archived
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 20))
    if any([query, unit, tags, status, request.args.get('page'), request.args.get('page_size')]):
        return ok(MaterialService.list(query=query, unit=unit, tags=tags, status=status, page=page, page_size=page_size))
    return ok(MaterialService.list_all())

@api_v1_bp.post("/materials")
@require_role(["admin", "ops"]) 
def materials_upsert():
    body = request.json or {}
    code = body.get("code")
    return ok(MaterialService.upsert(code, body))

@api_v1_bp.get("/materials/<code>")
@require_role(["admin", "ops", "viewer"]) 
def material_get(code):
    return ok(MaterialService.get(code))

@api_v1_bp.delete("/materials/<code>")
@require_role(["admin", "ops"]) 
def material_delete(code):
    force = (request.args.get('force') in ('1','true','TRUE','True'))
    try:
        return ok(MaterialService.delete(code, force=force))
    except ValueError as e:
        if str(e) == 'REFERENCED':
            return err('REFERENCED', 409)
        return err(str(e), 400)

@api_v1_bp.get("/materials/<code>/usage")
@require_role(["admin", "ops", "viewer"]) 
def material_usage(code):
    return ok(MaterialService.usage(code))

@api_v1_bp.post("/materials/<code>/replace")
@require_role(["admin", "ops"]) 
def material_replace(code):
    body = request.json or {}
    to_code = body.get('to_code')
    scope = body.get('scope','all')
    try:
        return ok(MaterialService.replace(code, to_code, scope))
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.post("/materials/import")
@require_role(["admin", "ops"]) 
def materials_import():
    body = request.json or {}
    strategy = body.get('strategy', 'merge')
    payload = body.get('payload')  # list[dict] or csv text
    dry_run = bool(body.get('dry_run', False))
    try:
        return ok(MaterialService.import_payload(strategy, payload, dry_run=dry_run))
    except ValueError as e:
        return err(str(e), 400)

@api_v1_bp.get("/materials/export")
@require_role(["admin", "ops", "viewer"]) 
def materials_export():
    fmt = (request.args.get('format') or 'json').lower()
    codes = request.args.get('codes')
    codes_list = [c.strip() for c in codes.split(',')] if codes else None
    content, mime = MaterialService.export(codes_list, fmt=fmt)
    from flask import Response
    return Response(content, mimetype=mime, headers={"Content-Disposition": f"attachment; filename=materials.{('csv' if fmt=='csv' else 'json')}"})

# Recipes
@api_v1_bp.post("/recipes")
@require_role(["admin", "ops"]) 
def recipe_upsert():
    body = request.json or {}
    rid = body.get("id")
    return ok(RecipeService.upsert(rid, body))

@api_v1_bp.get("/recipes/enabled")
@require_role(["admin", "ops", "viewer"]) 
def recipe_enabled():
    return ok(RecipeService.list_enabled())

@api_v1_bp.post("/recipes/<recipe_id>/publish")
@require_role(["admin", "ops"]) 
def recipe_publish(recipe_id):
    return ok(RecipeService.publish(recipe_id))

# Batches
@api_v1_bp.post("/commands/dispatch")
@require_role(["admin", "ops"]) 
def dispatch_batch():
    body = request.json or {}
    device_ids = body.get("device_ids") or []
    cmd_type = body.get("command_type")
    payload = body.get("payload")
    note = body.get("note")
    return ok(CommandService.dispatch_batch(device_ids, cmd_type, payload, note))

@api_v1_bp.get("/commands/batches")
@require_role(["admin", "ops", "viewer"]) 
def list_batches():
    return ok(CommandService.list_batches())

@api_v1_bp.get("/commands/batches/<batch_id>")
@require_role(["admin", "ops", "viewer"]) 
def get_batch(batch_id):
    return ok(CommandService.get_batch(batch_id))

# Metrics (very basic placeholders)
@api_v1_bp.get("/metrics")
def metrics():
    return ("api_latency_avg 0\napi_error_rate 0\n", 200, {"Content-Type": "text/plain; version=0.0.4"})

# Audit
@api_v1_bp.get("/audit")
@require_role(["admin", "ops", "viewer"]) 
def audit_list():
    return ok(AuditService.list())

# Alarms
@api_v1_bp.get('/devices/<device_id>/alarms')
@require_role(["admin", "ops", "viewer"]) 
def alarms_list(device_id):
    limit = int(request.args.get('limit', 100))
    return ok(AlarmService.list(device_id, limit))

@api_v1_bp.patch('/devices/<device_id>/alarms/<alarm_id>/status')
@require_role(["admin", "ops"]) 
def alarm_set_status(device_id, alarm_id):
    body = request.json or {}
    st = body.get('status')
    if st not in ('open','acked','closed'):
        return err('INVALID_ARGUMENT:status', 400)
    r = AlarmService.set_status(device_id, alarm_id, st)
    if not r:
        return err('ALARM_NOT_FOUND', 404)
    return ok(r)

# Packages
@api_v1_bp.get("/packages")
@require_role(["admin", "ops", "viewer"]) 
def packages_list():
    return ok(PackageService.list_all())

@api_v1_bp.post("/packages")
@require_role(["admin", "ops"]) 
def packages_upsert():
    body = request.json or {}
    pid = body.get("id")
    return ok(PackageService.upsert(pid, body))

# Reorder endpoints
@api_v1_bp.patch("/devices/<device_id>/menu/categories/reorder")
@require_role(["admin", "ops"]) 
def reorder_categories(device_id):
    arr = request.json or []
    if not isinstance(arr, list):
        return err("INVALID_ARGUMENT", 400)
    MenuService.reorder_categories(device_id, arr)
    return ok()

@api_v1_bp.patch("/devices/<device_id>/menu/categories/<cat_id>/reorder")
@require_role(["admin", "ops"]) 
def reorder_items(device_id, cat_id):
    arr = request.json or []
    if not isinstance(arr, list):
        return err("INVALID_ARGUMENT", 400)
    MenuService.reorder_items(device_id, cat_id, arr)
    return ok()
