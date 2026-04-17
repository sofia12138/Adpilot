# AdPilot 服务器部署指南

## 前置条件

- Ubuntu 20.04+ 服务器
- Python 3.11+
- Node.js 18+ (构建前端)
- MySQL 8.0+（APP 库 + BIZ 库）
- Nginx
- 已创建数据库：`adpilot_app`、`adpilot_biz`
- 已有产研库 `matrix_advertise` 的只读访问权限

## 1. 拉取代码

```bash
cd /var/www
git clone <REPO_URL> adpilot
cd adpilot
```

## 2. 配置环境变量

```bash
cp deploy/env/backend.env.example .env
nano .env  # 填入实际的数据库密码、API Key 等
```

**重要**：`.env` 放在项目根目录 `/var/www/adpilot/.env`，后端 `config.py` 通过 `../.env` 向上读取。

## 3. 后端部署

### 3.1 创建 Python 虚拟环境

```bash
cd /var/www/adpilot
python3 -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt
```

### 3.2 验证后端启动

```bash
cd backend
uvicorn app:app --host 127.0.0.1 --port 8000
# 确认 Application startup complete 后 Ctrl+C 退出
```

### 3.3 配置 systemd 服务

```bash
sudo cp deploy/systemd/adpilot.service.example /etc/systemd/system/adpilot.service
sudo nano /etc/systemd/system/adpilot.service
# 修改 <USER> 为实际用户名，确认路径正确

sudo systemctl daemon-reload
sudo systemctl enable adpilot
sudo systemctl start adpilot
```

### 3.4 查看后端日志

```bash
sudo journalctl -u adpilot -f          # 实时日志
sudo journalctl -u adpilot --since today  # 今日日志
sudo systemctl status adpilot            # 服务状态
```

## 4. 前端部署

### 4.1 构建前端产物

```bash
cd /var/www/adpilot/frontend
npm install
npm run build
# 产物输出到 frontend/dist/
```

### 4.2 验证构建产物

```bash
ls frontend/dist/index.html  # 确认文件存在
```

## 5. 配置 Nginx

```bash
sudo cp deploy/nginx/adpilot.conf.example /etc/nginx/sites-available/adpilot.conf
sudo nano /etc/nginx/sites-available/adpilot.conf
# 修改 <YOUR_DOMAIN>，确认 root 路径指向 frontend/dist

sudo ln -s /etc/nginx/sites-available/adpilot.conf /etc/nginx/sites-enabled/
sudo nginx -t        # 检查配置
sudo systemctl reload nginx
```

**Nginx 代理逻辑**：
- `/` → 前端静态文件（`frontend/dist/`），SPA 回退到 `index.html`
- `/api/` → 反向代理到 `http://127.0.0.1:8000/api/`
- `/health` → 后端健康检查

## 6. 数据库说明

| 库名 | 用途 | 权限 |
|------|------|------|
| `matrix_advertise` (PRD) | 产研基础数据 | **只读** |
| `adpilot_app` (APP) | 系统元数据：用户、权限、模板、面板配置 | 读写 |
| `adpilot_biz` (BIZ) | 业务数据：报表、同步日志、回传转化 | 读写 |

后端首次启动时会自动创建 APP 和 BIZ 库所需的表结构。

## 7. 更新部署

```bash
cd /var/www/adpilot
git pull origin main

# 后端更新
source venv/bin/activate
pip install -r backend/requirements.txt
sudo systemctl restart adpilot

# 前端更新
cd frontend
npm install
npm run build
# Nginx 自动读取新的 dist/，无需重启
```

## 8. 常见问题

### 后端启动失败
```bash
sudo journalctl -u adpilot -n 50 --no-pager  # 查看最近 50 行日志
```

### .env 读取失败
确认 `.env` 位于 `/var/www/adpilot/.env`（项目根目录），后端从 `backend/` 目录通过 `../.env` 读取。

### 数据库连接失败
确认 MySQL 服务运行中，且 `.env` 中的数据库连接信息正确。APP 和 BIZ 库需在本机或可达的 MySQL 实例上预先创建。
