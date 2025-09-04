from datetime import datetime


def k_dev(device_id: str) -> str:
    return f"cm:dev:{device_id}"


def k_menu_meta(device_id: str) -> str:
    return f"cm:dev:{device_id}:menu:meta"


def k_menu_cats(device_id: str) -> str:
    return f"cm:dev:{device_id}:menu:cats"


def k_menu_cat(device_id: str, cat_id: str) -> str:
    return f"cm:dev:{device_id}:menu:cat:{cat_id}"


def k_menu_cat_items(device_id: str, cat_id: str) -> str:
    return f"cm:dev:{device_id}:menu:cat:{cat_id}:items"


def k_menu_item(device_id: str, item_id: str) -> str:
    return f"cm:dev:{device_id}:menu:item:{item_id}"


def k_menu_available(device_id: str) -> str:
    return f"cm:dev:{device_id}:menu:available"


def k_menu_seq_cat(device_id: str) -> str:
    return f"cm:dev:{device_id}:menu:seq:cat"


def k_menu_seq_item(device_id: str) -> str:
    return f"cm:dev:{device_id}:menu:seq:item"


def k_device(device_id: str) -> str:
    return f"cm:dev:{device_id}"


def ts() -> int:
    return int(datetime.utcnow().timestamp())


def k_audit_stream() -> str:
    return "cm:stream:audit"

# Commands
def k_cmd_hash(device_id: str, cmd_id: str) -> str:
    return f"cm:dev:{device_id}:cmd:{cmd_id}"

def k_cmd_pending_q(device_id: str) -> str:
    return f"cm:dev:{device_id}:q:cmd:pending"

def k_cmd_inflight(device_id: str) -> str:
    return f"cm:dev:{device_id}:cmd:inflight"

# Orders
def k_order(device_id: str, order_id: str) -> str:
    return f"cm:dev:{device_id}:order:{order_id}"

def k_orders_by_ts(device_id: str) -> str:
    return f"cm:dev:{device_id}:orders:by_ts"

# Materials / Recipes / Packages (dict)
def k_dict_recipe(recipe_id: str) -> str:
    return f"cm:dict:recipe:{recipe_id}"

def k_dict_recipe_enabled() -> str:
    return "cm:dict:recipe:enabled"

def k_dict_recipe_all() -> str:
    return "cm:dict:recipe:all"

def k_dict_material(code: str) -> str:
    return f"cm:dict:material:{code}"

def k_dict_material_all() -> str:
    return "cm:dict:material:all"

# Alarms
def k_alarm(device_id: str, alarm_id: str) -> str:
    return f"cm:dev:{device_id}:alarm:{alarm_id}"

def k_alarms_by_ts(device_id: str) -> str:
    return f"cm:dev:{device_id}:alarms:by_ts"

def k_alarms_status(device_id: str, status: str) -> str:
    return f"cm:dev:{device_id}:alarms:status:{status}"

# Batches for dispatch
def k_batch(batch_id: str) -> str:
    return f"cm:batch:{batch_id}"

def k_batch_cmds(batch_id: str) -> str:
    return f"cm:batch:{batch_id}:cmds"

# Recipe device active set and packages
def k_dev_recipes_active(device_id: str) -> str:
    return f"cm:dev:{device_id}:recipes:active"

def k_recipe_pkg(recipe_id: str, version: str) -> str:
    return f"cm:pkg:recipe:{recipe_id}:{version}"
