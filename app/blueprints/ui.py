from flask import Blueprint, render_template

ui_bp = Blueprint("ui", __name__)

@ui_bp.get("/")
def home():
    return render_template('dashboard.html', title='仪表盘')

@ui_bp.get("/login")
def login():
    return render_template('login.html', title='登录')

@ui_bp.get("/dashboard")
def dashboard():
    return render_template('dashboard.html', title='仪表盘')

@ui_bp.get("/devices")
def devices():
    return render_template('devices.html', title='设备')

@ui_bp.get("/devices/<device_id>")
def device_detail(device_id):
    return render_template('device_detail.html', title=f'设备 {device_id}', device_id=device_id)

@ui_bp.get("/recipes")
def page_recipes():
    return render_template('recipes.html', title='配方管理')

@ui_bp.get("/materials")
def page_materials():
    return render_template('materials.html', title='物料字典')

@ui_bp.get("/packages")
def page_packages():
    return render_template('packages.html', title='包管理')

@ui_bp.get("/dispatch")
def page_dispatch():
    return render_template('dispatch.html', title='下发中心')

@ui_bp.get("/dispatch/<batch_id>")
def page_dispatch_detail(batch_id):
    return render_template('dispatch_detail.html', title=f'批次 {batch_id}', batch_id=batch_id)

@ui_bp.get("/orders")
def page_orders():
    return render_template('orders.html', title='订单')

@ui_bp.get("/alarms")
def page_alarms():
    return render_template('alarms.html', title='告警')

@ui_bp.get("/tasks")
def page_tasks():
    return render_template('tasks.html', title='任务中心')

@ui_bp.get("/audit")
def page_audit():
    return render_template('audit.html', title='审计')
