import os
import json
from app import create_app

app = create_app()

# 简单 smoke 测试：创建分类与商品并导出菜单
if __name__ == "__main__":
    from app.utils.extensions import redis_cli
    r = redis_cli.r
    device_id = os.environ.get("SMOKE_DEVICE", "dev-smoke")
    # 清理菜单
    for cid in r.zrange(f"cm:dev:{device_id}:menu:cats", 0, -1):
        for iid in r.zrange(f"cm:dev:{device_id}:menu:cat:{cid}:items", 0, -1):
            r.delete(f"cm:dev:{device_id}:menu:item:{iid}")
        r.delete(f"cm:dev:{device_id}:menu:cat:{cid}:items")
        r.delete(f"cm:dev:{device_id}:menu:cat:{cid}")
    r.delete(f"cm:dev:{device_id}:menu:cats")

    from app.services.menu import MenuService
    c = MenuService.create_category(device_id, {"name_i18n": {"zh":"推荐","en":"Featured"}})
    i = MenuService.create_item(device_id, {"cat_id": c["id"], "recipe_id":"r-1", "name_i18n": {"zh":"拿铁"}, "price_cents_override": 1800})
    meta = MenuService.publish(device_id)
    menu = MenuService.get_full_menu(device_id)
    print(json.dumps({"meta": meta, "menu": menu}, ensure_ascii=False))
