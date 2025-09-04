from flask import Blueprint, request
from ..services.devices import DeviceService
from ..services.menu import MenuService
from ..services.commands import CommandService
from ..services.orders import OrderService
from ..services.materials import MaterialService
from ..services.recipes import RecipeService
from ..services.audit import AuditService
from ..services.packages import PackageService
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
    return ok(DeviceService.list_devices())

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
    start_ts = request.args.get("from")
    end_ts = request.args.get("to")
    st = int(start_ts) if start_ts else None
    et = int(end_ts) if end_ts else None
    return ok(OrderService.list_device_orders(device_id, limit=limit, start_ts=st, end_ts=et))

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
    return ok(MaterialService.list_all())

@api_v1_bp.post("/materials")
@require_role(["admin", "ops"]) 
def materials_upsert():
    body = request.json or {}
    code = body.get("code")
    return ok(MaterialService.upsert(code, body))

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
